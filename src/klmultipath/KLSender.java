package klmultipath;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;

import klmultipath.randomprocesses.IntertimeProcess;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * 
 * java -cp "bin:lib/commons-cli-1.2.jar:lib/commons-math3-3.6.1.jar" klmultipath.KLSender -S x 0.01 -p 127.0.0.1:127.0.0.1 -p 127.0.0.1:127.0.0.2 -p 127.0.0.1:127.0.0.3  -p 127.0.0.1:127.0.0.4 -k 4 -l 4
 * 
 * Cross-traffic mode:
 * java -cp "bin:lib/commons-cli-1.2.jar:lib/commons-math3-3.6.1.jar" klmultipath.KLSder -S x 0.0001 -p 127.0.0.1:127.0.0.1 -p 127.0.0.1:127.0.0.2 -p 127.0.0.1:127.0.0.3  -p 127.0.0.1:127.0.0.4 -x
 * 
 * 
 * @author brenton
 *
 */
public class KLSender {
	
	 // This packetsize is chosen so that the packets are 10000 = 1e+5 bits long.
	 // There are 42 Bytes of overhead, so (1204+46)*8 = 10000.
	public static final int PACKETSIZE = 1204;
	
	// The rate scaled to ns/pkt.
	// We want a rate of 1.0 to give 1Mb/s throughput demand.
	// We have 1e+4 b/pkt
	// 1 Mb/s = 1e+6 b/s * (1e-9 s/ns)
	//        = 1e-3 b/ns * (1e-4 pkt/b)
	//        = 1e-7 pkt/ns
	// ==> 1e+7 ns/pkt
	public static final double RATE_SCALE = 1.0e7;
	
	public int k, l;
	IntertimeProcess tx_process = null;
	PathSpec[] paths = null;
	DatagramSocket[] sockets = null;
	byte[][] tx_bufs = null;
	
	NanoSynchro ns = null;

	public KLSender(int k, int l, IntertimeProcess tx_process, String[] path_strings, NanoSynchro ns) {
		this.k = k;
		this.l = l;
		this.tx_process = tx_process;
		this.ns = ns;
		
		// start the time time sync thing
		// and wait a moment for it to get enough data on he other end
		if (ns != null) {
		    System.err.println("Letting the clock differences calibrate...");
		    ns.start();
		    try {
                Thread.sleep(NanoSynchro.NUM_SAMPLES * NanoSynchro.SEND_INTERVAL * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
		    System.err.println("done.");
		}
		
		// initialize the connections
		paths = new PathSpec[path_strings.length];
		sockets = new DatagramSocket[path_strings.length];
		tx_bufs = new byte[k][];
		for (int i=0; i<k; i++) {
			tx_bufs[i] = new byte[PACKETSIZE];
			BytePacker.putInteger(tx_bufs[i], 0, k);
			BytePacker.putInteger(tx_bufs[i], 4, i);
		}
		
		for (int i=0; i<path_strings.length; i++) {
			paths[i] = new PathSpec(path_strings[i]);
			System.out.println(paths[i]);
			try {
				sockets[i] = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 
	 * @param num_messages
	 */
	public void startSending(int num_messages) {

		int path_index = 0;
		long sendstart = System.nanoTime();
		long endwait = sendstart;
		long time_elapsed = 0;
		
		// if we are generating cross traffic, run forever
		int count = 0;
		for (int i=0; i<num_messages; i++) {
			long txtime = System.nanoTime();
			for (int j=0; j<k; j++) {
				// write seq number and the arrival time and txtime into the packets
				BytePacker.putInteger(tx_bufs[j], 8, i);
				//BytePacker.putLong(tx_bufs[j], 12, time_elapsed+sendstart);
				BytePacker.putLong(tx_bufs[j], 12, endwait);
				BytePacker.putLong(tx_bufs[j], 20, txtime);
				BytePacker.putInteger(tx_bufs[j], 28, count++);

				try {
					sockets[path_index].send(new DatagramPacket(tx_bufs[j],
							tx_bufs[j].length,
							paths[path_index].dest,
							KLReceiver.PORT));
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
				path_index = (path_index + 1) % paths.length;
			}
				
			// sleep for some length of time given by the arrival process
			// scale this by 1e+5 so the rate on the command line can be in terms of Mb/s
			long dt = (long) (RATE_SCALE * this.tx_process.nextInterval());
			endwait += dt;
			time_elapsed += dt;
			long curtime = System.nanoTime();
			//if (endwait < curtime) {
			//	System.err.println("WARNING: system is backlogged: "+endwait+" < "+curtime+"    ("+(curtime-endwait)+")");
			//} else {
			//	System.err.println("empty system");
			//}
			do{
				curtime = System.nanoTime();
			} while(curtime < endwait);
		}
	}
	
	
	/**
	 * We want the cross traffic to follow an independent process over each path.
	 * That means we have to run the senders in separate threads, or get fancy with
	 * the inter-packet times.
	 */
	public void startCrossTraffic() {

		long sendstart = System.nanoTime();
		long endwait = sendstart;
		byte[][] xtbufs = new byte[paths.length][];
		for (int i=0; i<paths.length; i++) {
			xtbufs[i] = new byte[PACKETSIZE];
		}
		// we'll get fancy with the packet times.
		// The traffic sent on each path will follow tx_process, and we keep
		// an array of the next time a packet is scheduled to go out on each path.
		// nextpath is the index of the path with the smallest nextpacket[] time.
		long[] nextpacket = new long[this.paths.length];
		int nextpath = 0;
		for (int p=0; p<paths.length; p++) {
			nextpacket[p] = (long) (RATE_SCALE * this.tx_process.nextInterval());
			if (nextpacket[p] < nextpacket[nextpath]) {
				nextpath = p;
			}
		}
		
		// if we are generating cross traffic, run forever
		long curtime = System.nanoTime();
		while (true) {
			// fast-forward time to the time of the next packet
			endwait = sendstart + nextpacket[nextpath];
			//if (curtime < endwait) {
			//	System.out.println("have to wait...");
			//}
			while (curtime < endwait) {
				// don't call nanoTime() unless we have to
				curtime = System.nanoTime();
			}
			try {
				sockets[nextpath].send(new DatagramPacket(xtbufs[nextpath],
						xtbufs[nextpath].length,
						paths[nextpath].dest,
						KLReceiver.CROSS_TRAFFIC_PORT));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
				
			// sleep for some length of time given by the arrival process
			long dt = (long) (RATE_SCALE * this.tx_process.nextInterval());
			nextpacket[nextpath] += dt;
			nextpath = 0;
			for (int p=0; p<paths.length; p++) {
				if (nextpacket[p] < nextpacket[nextpath]) {
					nextpath = p;
				}
			}
			//System.out.println(Arrays.toString(nextpacket));
		}
	}
	
	
	/**
	 * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("k", "k", true, "number of packets to send per transmission");
		cli_options.addOption("l", "l", true, "number of packets that must be recieved");
		cli_options.addOption("n", "numsamples", true, "number of transmission to make");
		//cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption("p", "path", true, "sender/reciever IP address pair in the form X.X.X.X:Y.Y.Y.Y");
		cli_options.addOption("x", "crosstraffic", false, "sender for crosstraffic");
		//cli_options.addOption(OptionBuilder.withLongOpt("queuetype").hasArgs().isRequired().withDescription("queue type and arguments").create("q"));
		//cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("the base name of the output files").create("o"));
		cli_options.addOption(OptionBuilder.withLongOpt("txprocess").hasArgs().isRequired().withDescription("sending process").create("S"));

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

		int num_messages = options.hasOption("n") ? Integer.parseInt(options.getOptionValue("n")) : 1000;
		String paths[] = options.hasOption("p") ? options.getOptionValues("p") : null;
		int k = options.hasOption("k") ? Integer.parseInt(options.getOptionValue("k")) : 1;
		int l = options.hasOption("l") ? Integer.parseInt(options.getOptionValue("l")) : 1;
		if (l > k) {
			System.err.println("ERROR: l > k");
			System.exit(0);
		}
		IntertimeProcess tx_process = IntertimeProcess.parseProcessSpec(options.getOptionValues("S"));
		
		NanoSynchro ns = new NanoSynchro(null);
		
		KLSender sender = new KLSender(k, l, tx_process, paths, ns);
		
		if (options.hasOption("x")) {
			sender.startCrossTraffic();
		} else {
			sender.startSending(num_messages);
		}
		
		ns.interrupt();
		ns.requestStop();
	}

}




