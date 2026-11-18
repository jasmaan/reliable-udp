
/**
 * BurstyFtp Class
 * 
 * BurstyFtp implements a Burst-and-Wait reliable transfer protocol.
 * This implementation bridges the gap between Stop-and-Wait and Go-Back-N. 
 * It transmits data in window sized bursts to maximize throughput, then 
 * synchronizes with the receiver by waiting for a cumulative acknowledgment 
 * for the entire burst before proceeding to the next burst.
 *  
 * Completed in a University computer networking course.
 */


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;


public class BurstyFtp {
	
	private static final Logger logger = Logger.getLogger("BurstyFtp"); // global logger	

	// instances
	private int winSize;        // window burst size (segments)
	private int connTimeout;	// connection idle time -> non responsive server
	private int rtxTimeout;		// retransmission time -> timer based retransmit 
	private Timer timer;		// single Timer object

	// shared state (for main thread + RetransmitTask)
	private FtpSegment[] window;		// current burst segments (helpful for potential retransmission)
	private int base;					// sequence number of first unACK'd segment
	private int windowEnd;				// seg numger of last segment in current window (+ 1)
	private DatagramSocket udpSocket;	// shared UDP socket for send/receive
	private InetAddress serverAddress;	// reused per burst
	private int serverUdpPort;			// port received during handshake

	/**
	 * Constructor to initialize the program 
	 * 
	 * @param winSize			The window size in segments
	 * @param connTimeout		The time-out interval for non-responsive server
	 * @param rtxTimeout		The time-out interval for the retransmission timer
	 * 
	 */
	public BurstyFtp(int winSize, int connTimeout, int rtxTimeout) {
		this.winSize = winSize;
		this.connTimeout = connTimeout;
		this.rtxTimeout = rtxTimeout;
		this.timer = new Timer(true);

	}


	/**
	 * Send the specified file to the specified remote server
	 * 
	 * @param serverName	Name of the remote server
	 * @param serverPort	Port number of the remote server
	 * @param fileName		Name of the file to be trasferred to the rmeote server
	 * 
	 * @return 				true if the file transfer completed successfully, false otherwise
	 */
	public boolean send(String serverName, int serverPort, String fileName) {
		Socket tcpSocket = null;
		DataInputStream tcpIn = null;
		DataOutputStream tcpOut = null;
		FileInputStream fileIn = null;

		try {
			tcpSocket = new Socket(serverName, serverPort);		// TCP socket for handshake
			udpSocket = new DatagramSocket(); 					// pattern from course tutorial
			serverAddress = InetAddress.getByName(serverName);	// DNS resolution pattern (also from course tutorial)

			tcpIn = new DataInputStream(tcpSocket.getInputStream());	// binary streams for the handshake
			tcpOut = new DataOutputStream(tcpSocket.getOutputStream());
			logger.info("TCP connected to " + serverName + ":" + serverPort);
			logger.info("UDP socket open on local port: " + udpSocket.getLocalPort());

			// Handshake message sequence
			File file = new File(fileName);
			long fileLength = file.length();
			tcpOut.writeUTF(fileName);					// send file name as UTF string 
			tcpOut.writeLong(fileLength);				// send length in bytes of file
			tcpOut.writeInt(udpSocket.getLocalPort());	// send local UDP port number
			tcpOut.flush();								// flush at end of send section
			serverUdpPort = tcpIn.readInt();			// receive server UDP port 
			int initSeqNum = tcpIn.readInt();			// receive initial seq number

			logger.info("Server UDP port: " + serverUdpPort);
			logger.info("Initial seq num: " + initSeqNum);
			
			// File transfer over UDP, based on course provided pseudocode and Go-Back-N
			udpSocket.setSoTimeout(connTimeout);		// detects non responsive server
			fileIn = new FileInputStream(file);
			byte[] buffer = new byte[FtpSegment.MAX_PAYLOAD_SIZE];	// max payload for segments
			int seqNum = initSeqNum;
			int bytesRead;
			boolean transferDone = false;

			while (!transferDone) {
				// Only the final burst can have fewer segments
				List<FtpSegment> burstList = new ArrayList<FtpSegment>();
				for (int i = 0; i < winSize; i ++) {
					bytesRead = fileIn.read(buffer);
					if (bytesRead == -1) {
						transferDone = true; 	// end of file reached so burst may be partial
						break;
					}
					FtpSegment seg = new FtpSegment(seqNum, buffer, bytesRead);		// creates segment with only bytes actually read
					burstList.add(seg);
					seqNum++;

				}
				if (burstList.isEmpty()) {		// nothing to send
					break;
				}

				// burst stored as array for retransmission access
				// synchronized block prevents race condition with retransmitTask (pattern from course tutorial)
				synchronized (this) {
					window = burstList.toArray(new FtpSegment[0]);
					base = window[0].getSeqNum();						   // first seq num in burst
					windowEnd = window[window.length - 1].getSeqNum() + 1; // one past last seq num
				}

				// Send all segments in burst before waiting for ACKs
				// send/receive pattern from course tutorial
				sendWindow();

				// Starts retransmission timer for burst
				RetransmitTask task = new RetransmitTask();
				synchronized(this) {
					timer.schedule(task, rtxTimeout);
				}

				// Wait for cumulative ACKs until entire burst
				while (true) {
					synchronized(this) {
						if (base >= windowEnd) {
							task.cancel();			// entire burst ACKnowledged, timer canceled and move 
							break;
						}
					}
					
					try {
						// DatagramPacket receive pattern from course tutorial
						byte[] ackBuffer = new byte[FtpSegment.MAX_SEGMENT_SIZE];
						DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
						udpSocket.receive(ackPacket);			// blcoks until ACK or connTimeout expire
						FtpSegment ack = new FtpSegment(ackPacket);		// deencapsulates received packet
						int ackSeqNum = ack.getSeqNum();
						System.out.println("ack \t" + ackSeqNum);		// console output

						synchronized (this) {
							if (ackSeqNum > base) {
								base = ackSeqNum;						// advance base
							}
						}
					} catch (SocketTimeoutException e) {
						// connTimeout exceeded, cancel transfer
						logger.warning("Connection timeout - server non-responsive. Aborting.");
						task.cancel();
						return false;
					}
				}

			}

		} catch (IOException e) {
			System.out.println("I/O error during transfer: " + e.getMessage());
			return false;
		} finally {
			cleanUp(tcpSocket, tcpIn, tcpOut, fileIn);		// closes all streams and sockets

		}
		return true;


	}

	//
	// --- HELPER METHODS --- 
	//

	// Sends all unACK segments in current window back to back
	// send pattern from course tutorial
	private void sendWindow() throws IOException {
		synchronized (this) {
			for (FtpSegment seg : window) {
				if (seg.getSeqNum() >= base) {
					DatagramPacket pkt = FtpSegment.makePacket(seg, serverAddress, serverUdpPort);
					udpSocket.send(pkt);
					System.out.println("send \t" + seg.getSeqNum()); 	// console output
				}
			}
		}
	}

	// Closes all streams and sockets at end of transfer
	// Based on cleanUp() from my java-web-server project
	private void cleanUp(Socket tcpSocket, DataInputStream tcpIn, DataOutputStream tcpOut, FileInputStream fileIn) {
		timer.cancel();

		try {
			if (fileIn != null) {
				fileIn.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if (tcpIn != null) {
				tcpIn.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if (tcpOut != null) {
				tcpOut.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if (tcpSocket != null && !tcpSocket.isClosed()) {
				tcpSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}


		if (udpSocket != null && !udpSocket.isClosed()) {
			udpSocket.close();
		}

		logger.info("All sockets and stream closed.");
	}



	/**
	 * Timer task that fires when retransmission timeout expires
	 * Uses the Timer/TimerTask pattern
	 * Pattern based on course tutorial
	 * Inner class has to lock an outer object to share the same monitor as main thread (which prevents race conditions on window variables)
	 */ 
	private class RetransmitTask extends TimerTask {

		@Override
		public void run() {
			System.out.println("timeout");		// console output

			try {
				synchronized (BurstyFtp.this) {
					if (base >= windowEnd) {
						return; // burst already done, doesn't retransmit/reschedule
					}
					for (FtpSegment seg : window) {			// retransmit all segments not yet ACK'd 
						if (seg.getSeqNum() >= base) {
							DatagramPacket pkt = FtpSegment.makePacket(seg, serverAddress, serverUdpPort);
							udpSocket.send(pkt);
							System.out.println("retx \t" + seg.getSeqNum());		// console output
						}
					}

					RetransmitTask newTask = new RetransmitTask(); 		// new TimerTask instance to reschedule, previous cannot be rescheduled
					timer.schedule(newTask, rtxTimeout);
				}
			} catch (IOException e) {
				System.out.println("I/O error during retransmission: " + e.getMessage());
			}
		}
	}


} // end of class