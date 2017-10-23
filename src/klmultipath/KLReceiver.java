package klmultipath;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.StringJoiner;
import java.lang.Runtime;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * sudo ifconfig lo:0 127.0.0.2 netmask 255.0.0.0 up
 * sudo ifconfig lo:1 127.0.0.3 netmask 255.0.0.0 up
 * sudo ifconfig lo:2 127.0.0.4 netmask 255.0.0.0 up
 * 
 * java -cp "bin:lib/commons-cli-1.2.jar" klmultipath.KLReceiver -p 127.0.0.1:127.0.0.1 -p 127.0.0.1:127.0.0.2 -p 127.0.0.1:127.0.0.3  -p 127.0.0.1:127.0.0.4  -o testrun
 * 
 * In cross-traffic mode:
 * java -cp "bin:lib/commons-cli-1.2.jar" klmultipath.KLReceiver -p 127.0.0.1:127.0.0.1 -p 127.0.0.1:127.0.0.2 -p 127.0.0.1:127.0.0.3  -p 127.0.0.1:127.0.0.4  -o testrun -x

 * 
 * @author brenton
 *
 */
public class KLReceiver {
	
	public static final int PORT = 7654;
	public static final int CROSS_TRAFFIC_PORT = 7655;

	// lets the shutdown hook tell the program to exit
	public static boolean is_running = true;
	
	PathSpec[] paths = null;
	String outfile_base = "";
	DatagramSocket[] sockets = null;
	BufferedWriter[] out = null;
	boolean cross_trafic = false;
	NanoSynchro ns = null;
	int sampling_interval = 1;
	
	/**
	 * Constructor
	 * 
	 * @param path_strings
	 * @param outfile_base
	 */
	public KLReceiver(String[] path_strings, String outfile_base, boolean cross_traffic, NanoSynchro ns) {
		paths = new PathSpec[path_strings.length];
		sockets = new DatagramSocket[path_strings.length];
		this.outfile_base = outfile_base;
		this.cross_trafic = cross_traffic;
		this.ns = ns;

		// start the time time sync thing
		if (ns != null) {
		    ns.start();
		}
		
		for (int i=0; i<path_strings.length; i++) {
			paths[i] = new PathSpec(path_strings[i]);
			System.out.println(paths[i]);
			if (! cross_trafic) {
				out = new BufferedWriter[paths.length];
			}
			try {
				sockets[i] = new DatagramSocket(null);
			} catch (SocketException e) {
				e.printStackTrace();
			}
		}
		
		// this class loops forever.  If we are running in normal mode, when
		// the program is killed we still want to flush and close the log files.
		if (! cross_traffic) {
			Runtime rt = Runtime.getRuntime();
			rt.addShutdownHook(new Thread() {
				public void run() {
					KLReceiver.is_running = false;
					try {
						sleep(1000);
						System.err.println("closing files...");
						try {
							for (int i=0; i<paths.length; i++) {
								out[i].close();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		}
	}
	
	
	/**
	 * Start listening for connections and recording received packets
	 */
	public void listen() {

		for (int i=0; i<paths.length; i++) {
			InetSocketAddress address = null;
			if (this.cross_trafic) {
				address = new InetSocketAddress(paths[i].dest, CROSS_TRAFFIC_PORT);
			} else {
				address = new InetSocketAddress(paths[i].dest, PORT);
			}
			try {
				sockets[i].bind(address);
			} catch (SocketException e) {
				e.printStackTrace();
				System.exit(1);
			}
			DatagramSocket sock = sockets[i];
			int threadnum = i;
			String outfile = this.outfile_base+"_"+threadnum+".dat";
			boolean xt = this.cross_trafic;
			int sampling_interval = this.sampling_interval;
			
			Thread listener = new Thread() {
				public void run() {
					byte[] rxData = new byte[2048];

					// if this receiver is for cross-traffic, don't bother writing a log file
					FileWriter fstream = null;
					if (! xt) {
						try {
							fstream = new FileWriter(outfile);
						} catch (IOException e1) {
							e1.printStackTrace();
							System.exit(1);
						}
						out[threadnum] = new BufferedWriter(fstream);
					}
					
					while (is_running) {
						DatagramPacket rxPacket = new DatagramPacket(rxData, rxData.length);
						try {
							sock.receive(rxPacket);
						} catch (IOException e) {
							e.printStackTrace();
							System.exit(1);
						}
						if (! xt) {
							long curtime = System.nanoTime();
							long clock_offset = ns.getClockCorrection();

							// extract the info from the packet
							byte[] rxd   = rxPacket.getData();
							int k = BytePacker.getInteger(rxd, 0);
							int index = BytePacker.getInteger(rxd, 4);
							int seqnum = BytePacker.getInteger(rxd, 8);
							long arrival = BytePacker.getLong(rxd, 12);
							long txtime = BytePacker.getLong(rxd, 20);
							int count = BytePacker.getInteger(rxd, 28);

							// write the data to the log file for this thread
							if ((index % sampling_interval) == 0) {
							    StringJoiner sj = new StringJoiner("\t","","\n");
							    sj.add(String.format("%09d",count));
							    sj.add(String.format("%09d",seqnum));
							    sj.add(String.format("%09d",index));
							    sj.add(""+arrival);
							    sj.add(""+txtime);
							    sj.add(""+curtime);
							    sj.add(""+k);
							    sj.add(""+clock_offset);

							    try {
							        out[threadnum].write(sj.toString());
							    } catch (IOException e) {
							        e.printStackTrace();
							        System.exit(1);
							    }
							    //System.out.println("received in thread "+threadnum);
							}
						}
					}
				}
			};
			listener.start();
		}
	}
	
	
	/**
	 * Set the sampling interval in terms of jobs.
	 * 
	 * @param interval
	 */
	public void setSamplingInterval(int interval) {
	    this.sampling_interval = interval;
	}
	
	
	/**
	 * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("x", "crosstraffic", false, "receiver for non-recorded crosstraffic");
		cli_options.addOption("p", "path", true, "sender/reciever IP address pair in the form X.X.X.X:Y.Y.Y.Y");
		cli_options.addOption("t", "time_sync_server", true, "IP address of the machine to try and sync nanoTime with");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("the base name of the output files").create("o"));
		
		CommandLineParser parser = new GnuParser();
		CommandLine options = null;
		try {
			options = parser.parse(cli_options, args);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("KLSender", cli_options);
			e.printStackTrace();
			System.exit(0);
		}

		String paths[] = options.hasOption("p") ? options.getOptionValues("p") : null;
		
		String sampling_interval = options.hasOption("p") ? options.getOptionValue("i") : null;
		
		NanoSynchro ns = null;
		if (options.hasOption("t")) {
		    ns = new NanoSynchro(options.getOptionValue("t"));
		}
		
		KLReceiver receiver = new KLReceiver(paths, options.getOptionValue("o"), options.hasOption("x"), ns);
		
		
		
		receiver.listen();
		
	}
	
}
