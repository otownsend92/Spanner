package spanner;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class DataCenter extends Thread {

	private String myIp;

	private ServerSocket serverSocket;
	private Map<String, Integer> pendingTxns = 
			Collections.synchronizedMap(new HashMap<String, Integer>());
	
	
	Shard shardX; 
	Shard shardY;
	Shard shardZ;

	private ArrayList<Shard> allShards = new ArrayList<Shard>();
	
	private final int PORT = 3001;
	
	
	// DataCenter constructor
	public DataCenter(int numShardData, String ip) {
		try{
			serverSocket = new ServerSocket(PORT);
			
			myIp = ip;
			
			shardX = new Shard("X");
			shardY = new Shard("Y");
			shardZ = new Shard("Z");
			allShards.add(shardX);
			allShards.add(shardY);
			allShards.add(shardZ);
			
			System.out.println("Shards configured");
		}
		catch (IOException e){
			System.out.println(e.toString());
		}
	}
	

	/**
	 * Listener thread 
	 */
	public void run() {
		System.out.println("Data center listening on port " + PORT + "...");
		
		while(true) {
			
			// Accept incoming client connections
			Socket clientSocket = null;
			try {
				clientSocket = serverSocket.accept();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(clientSocket != null) 
				new Thread(new DCHandlerThread(this, clientSocket)).start();
				
			else {
				System.out.println("DC failed to connect to client.");
			}
		}
	}

	
	// HandlerThread class to handle new client connection requests
	public class DCHandlerThread extends Thread {
		
		private Socket socket;
		private DataCenter parentThread;
		private String clientReadMsgs;
		
		public DCHandlerThread(DataCenter t, Socket s){
//			System.out.println("New DCHandlerThread");
			this.socket = s;
			this.parentThread = t;
			this.clientReadMsgs = "";
		}
		
		// Open up socket that was passed in from DataCenter
		// and read contents and parse
		public void run(){
			try{
				String input = null;
				Scanner socketIn = new Scanner(socket.getInputStream());
				if (socketIn.hasNext()){
					input = socketIn.nextLine();
				}
				if (input == null){
					socketIn.close();
					socket.close();
					return;
				}
				processInput(input);
				socketIn.close();
				socket.close();
			}
			catch(IOException e){
				System.out.println(e.toString());
			}
		}

		/**
		 * Send a message
		 * @param host who you're sending to
		 * @param msg msg you're trying to send
		 */
		private void sendMessage(Integer host, String msg){
			try{
				Socket s = new Socket(Main.serverHosts.get(host), PORT);
				PrintWriter socketOut = new PrintWriter(s.getOutputStream(), true);
				socketOut.println(msg);
				socketOut.close();
				s.close();
			}catch(IOException e){

			}
		}

		/**
		 * Sends a message to all datacenters
		 * @param msg
		 */
		private void sendMessageAllDC(String msg){
			for (int i = 0; i < Main.serverHosts.size(); i++){
				sendMessage(i, msg);
			}
		}
		
		
		
		/*
		 * Parse incoming string from client socket
		 */
		private void processInput(String input) {
			System.out.println("Received input: " + input);
			String[] recvMsg = input.split("!");
			
			if(recvMsg.length < 3) {
				System.out.println("Malformed message. Returning.");
				return;
			}

			if (recvMsg[0].equals("prepare2PCClient")) {
				String ipAddr = recvMsg[1];
				String txn = recvMsg[2];
				addPendingTxn(txn);

				// Grab dem locks homies
				boolean xGood = shardX.processTransaction(ipAddr, txn);
				boolean yGood = shardY.processTransaction(ipAddr, txn);
				boolean zGood = shardZ.processTransaction(ipAddr, txn);
				if (xGood && yGood && zGood){
					// All the shards are cool, we have the locks.
					// Log the transaction if contains an operation on an item in that particular shard
					// If we logged it, replicate it to the other shards
					for (Shard s: allShards){
						// TODO: I think we're supposed to log the 2PC prepare not
						// the transaction...?
						if(s.logTransaction(LogEntry.EntryType.PREPARE, txn)){
							// replicate that log entry to other data center shards
							sendMessageAllDC("acceptPaxos!"+ipAddr+"!"+s.shardId+"!"+txn);
						}
					}

				}else {
					// JH: We need some kind of a lock failure strategy
					// I don't think the paper explicitly mentions one
					// We could simply sleep in a loop until we get those locks back
					// OT: I think the paper says that they use the simple deadlock prevention
					// scheme of just having a timeout and if the process can't acquire locks before
					// the time runs out, it just fails.
				}
			}
			
			/*
			 * Map<String, Integer> ackAcceptPaxos
			 * Map<String, Integer> ack2PC
			 */
			
			else if (recvMsg[0].equals("acceptPaxos")) {
				// Log 2PC prepare
				// 
				// 
				String ipAddr = recvMsg[1];
			}
			else if(recvMsg[0].equals("rejectPaxos")) {
				
			}
			
			else if (recvMsg[0].equals("ackAcceptPaxos")) {
				// Count acks for majority
				// send ack2PC if you are NOT the 2PC coordinator
				// If you are 2PC coordinator, wait for acks
				// 
			}
			else if(recvMsg[0].equals("ackRejectPaxos")) {
				
			}
			
			else if (recvMsg[0].equals("ack2PC")) {
				// Only the 2PC coordinator will be receiving this message
				// 
				// 2PC coord logs the COMMIT locally
				// Now send coordinatorAccept2PC
			}
			else if(recvMsg[0].equals("ackReject2PC")) {
				
			}
			
			else if (recvMsg[0].equals("coordinatorAccept2PC")) {
				// 
				// 
				// 
			}
			else if(recvMsg[0].equals("coordinatorReject2PC")) {
				
			}
			
			else if (recvMsg[0].equals("ackCoordinatorAccept2PC")) {
				// Only 2PC coord will receive this message
				// When 2 acks are received, release locks
				//
				// Then send commit2PC to other Paxos leaders and client
			}
			else if(recvMsg[0].equals("ackCoordinatorReject2PC")){
				
			}
			
			else if(recvMsg[0].equals("commit2PC")) {
				// When Paxos leader receives this, replicate log entry
				// of 2PC commit using Paxos again
				// 
				// send repCom to other 2 DCs
			}

			else if(recvMsg[0].equals("repCom")) {
				// send ackRepCom back to sender
				// 
				// 
			}
			
			else if(recvMsg[0].equals("ackRepCom")) {
				// Once these shards receive majority of ackRepComs,
				// release local locks
				// 
			}
		}

		/*
		 * Add this new incoming txn to pendingTxns
		 */
		private synchronized void addPendingTxn(String txn) {
			pendingTxns.put(txn, 0);
			System.out.println("Added " + txn + " to pendingTxns");
		}

		/*
		 * This txn is finished. Remove it from pendingTxns
		 */
		private synchronized void removePendingTxn(String txn) {
			pendingTxns.remove(txn);
			System.out.println("Removed " + txn + " from pendingTxns \nDone.\n");
		}
	}
}
