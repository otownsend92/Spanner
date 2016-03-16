package spanner;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class DataCenter extends Thread {

	private String myIp;
	private int myHostId;

	private ServerSocket serverSocket;
	private Map<String, Integer> pendingTxns = 
			Collections.synchronizedMap(new HashMap<String, Integer>());
	
	private List<String> PaxosLog;
	
	Shard shardX; 
	Shard shardY;
	Shard shardZ;
	
	//maps to store number of accepts/rejects
  	Map<String, Integer> ackAcceptPaxos =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	Map<String, Integer> ackRejectPaxos =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	
  	Map<String, Integer> ackAccept2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	Map<String, Integer> ackReject2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	
  	Map<String, Integer> ackCoordinatorAccept2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	Map<String, Integer> ackCoordinatorReject2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	
  	Map<String, Integer> ackRepCom =
  			Collections.synchronizedMap(new HashMap<String, Integer>());

	private ArrayList<Shard> allShards = new ArrayList<Shard>();
	
	private final int PORT = 3000;
	
	
	// DataCenter constructor
	public DataCenter(int numShardData, String ip, int hostId) {
		try{
			serverSocket = new ServerSocket(PORT);
			
			myIp = ip;
			myHostId = hostId;
			
			shardX = new Shard("X", numShardData);
			shardY = new Shard("Y", numShardData);
			shardZ = new Shard("Z", numShardData);
			allShards.add(shardX);
			allShards.add(shardY);
			allShards.add(shardZ);
			
			PaxosLog = new ArrayList<String>();
			
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
		private void sendMessage(String host, String msg){
			System.out.println("Sending: " + msg +" to " + host);
			try{
				Socket s = new Socket(host, PORT);
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
				sendMessage(Main.serverHosts.get(i), msg);
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
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				
				System.out.println("clientIp: "+ clientIp);
				System.out.println("txn: "+ txn);
				System.out.println("shardId: "+ shardId);
				
				addPendingTxn(txn);
				System.out.println("added transaction");
				
				int hostId1 = (myHostId + 1) % 3;
				int hostId2 = (myHostId + 2) % 3;

				// Grab dem locks homies
				if(shardId.equals("X")) {
					System.out.println("ShardId equals X");
					boolean good = shardX.processTransaction(clientIp, txn);
					if(good) {
						shardX.logTransaction(LogEntry.EntryType.PREPARE, txn);
						System.out.println("shard x is sending acceptPaxos");
						sendMessage(Main.serverHosts.get(hostId1), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						sendMessage(Main.serverHosts.get(hostId2), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						
					} else {
						sendMessage(clientIp, "prepare2PC failed for txn: " + txn);
					}
				} else if(shardId.equals("Y")) {
					System.out.println("ShardId equals Y");
					boolean good = shardY.processTransaction(clientIp, txn);
					if(good) {
						System.out.println("shard y is sending acceptPaxos");

						shardY.logTransaction(LogEntry.EntryType.PREPARE, txn);
						sendMessage(Main.serverHosts.get(hostId1), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						sendMessage(Main.serverHosts.get(hostId2), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						
					} else {
						sendMessage(clientIp, "prepare2PC failed for txn: " + txn);
					}

				} else if(shardId.equals("Z")) {
					System.out.println("ShardId equals Z");
					boolean good = shardZ.processTransaction(clientIp, txn);
					if(good) {
						System.out.println("shard z is sending acceptPaxos");

						shardZ.logTransaction(LogEntry.EntryType.PREPARE, txn);
						sendMessage(Main.serverHosts.get(hostId1), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						sendMessage(Main.serverHosts.get(hostId2), "acceptPaxos"+"!"+clientIp+"!"+txn+"!"+shardId+"!"+myHostId);
						
					} else {
						sendMessage(clientIp, "prepare2PC failed for txn: " + txn);
					}
					// TODO: maybe try to send message multiple times before failing
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
				String clientIp 	= recvMsg[1];
				String txn 		= recvMsg[2];
				String shardId 	= recvMsg[3];
				int senderId = Integer.parseInt(recvMsg[4]);
				
				
				String senderIP = Main.serverHosts.get(senderId);
								
				if(shardId.equals("X")) {
					boolean good = shardX.processTransaction(clientIp, txn);
					shardX.logTransaction(LogEntry.EntryType.PREPARE, txn);
					sendAckPaxos(good, clientIp, shardId, txn, senderIP);
					
				} else if (shardId.equals("Y")) {
					boolean good = shardY.processTransaction(clientIp, txn);
					shardY.logTransaction(LogEntry.EntryType.PREPARE, txn);
					sendAckPaxos(good, clientIp, shardId, txn, senderIP);
					
				} else if (shardId.equals("Z")) {
					boolean good = shardZ.processTransaction(clientIp, txn);
					shardZ.logTransaction(LogEntry.EntryType.PREPARE, txn);
					sendAckPaxos(good, clientIp, shardId, txn, senderIP);
					
				} else {
					System.out.println("Invalid message received: " + recvMsg[0] + recvMsg[1] + recvMsg[2] + recvMsg[3] + recvMsg[4]);
					return;
				}
								
			}
			
			else if (recvMsg[0].equals("ackAcceptPaxos")) {
				// Count acks for majority
				// send ack2PC if you are NOT the 2PC coordinator
				// If you are 2PC coordinator, wait for acks
				
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				
				synchronized(ackAcceptPaxos) {
					if(!ackAcceptPaxos.containsKey(clientIp+"!"+txn))
						ackAcceptPaxos.put(clientIp+"!"+txn, 0);
					
					int currentQVal = ackAcceptPaxos.get(clientIp+"!"+txn);
					ackAcceptPaxos.put(clientIp+"!"+txn, currentQVal+1);
					
					checkAckAcceptPaxosQuorum(true, clientIp, txn);
				}
			}
			else if(recvMsg[0].equals("ackRejectPaxos")) {
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				
				synchronized(ackRejectPaxos) {
					if(!ackRejectPaxos.containsKey(clientIp+"!"+txn))
						ackRejectPaxos.put(clientIp+"!"+txn, 0);
					
					int currentQVal = ackRejectPaxos.get(clientIp+"!"+txn);
					ackRejectPaxos.put(clientIp+"!"+txn, currentQVal+1);
					
					checkAckAcceptPaxosQuorum(false, clientIp, txn);
					
					//TODO: rollback log? or nah?
				}
			}
			
			else if (recvMsg[0].equals("ackAccept2PC")) {
				// Only the 2PC coordinator will be receiving this message
				// 
				// 2PC coord logs the COMMIT locally
				// Now send coordinatorAccept2PC
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				
				synchronized(ackAccept2PC) {
					if(!ackAccept2PC.containsKey(clientIp + "!" + txn))
						ackAccept2PC.put(clientIp+"!"+txn, 0);
					
					int currentQVal = ackAccept2PC.get(clientIp+"!"+txn);
					ackAccept2PC.put(clientIp+"!"+txn, currentQVal+1);
							
					//log
					if(Main.coord2PCShard.equals("X")) {
						shardX.logTransaction(LogEntry.EntryType.COMMIT, txn);
					} else if(Main.coord2PCShard.equals("Y")) {
						shardY.logTransaction(LogEntry.EntryType.COMMIT, txn);
					} else if(Main.coord2PCShard.equals("Z")) {
						shardZ.logTransaction(LogEntry.EntryType.COMMIT, txn);
					}  
					
					//we check the quorum and send coordinatorAccept2PC
					checkAck2PCQuorum(true, clientIp, txn, Main.coord2PCShard);

				}
			}
			else if(recvMsg[0].equals("ackReject2PC")) {
				//TODO
			}
			
			else if (recvMsg[0].equals("coordinatorAccept2PC")) {
				// Cohort receives coordinatorAccept2PC, check if we can accept it
				// 
				// 
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				
				boolean loggedTransaction = false;
				//log
				if(shardId.equals("X")) {
					loggedTransaction = shardX.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Y")) {
					loggedTransaction = shardY.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Z")) {
					loggedTransaction = shardZ.logTransaction(LogEntry.EntryType.COMMIT, txn);
				}
				
				if(loggedTransaction) {
					//send ackCoordinatorAccept2PC
					sendMessage(Main.coord2PCIp, "ackCoordinatorAccept2PC" + "!" + clientIp + "!" + txn);
				} else {
					//send ackCoordinatorReject2PC
					sendMessage(Main.coord2PCIp, "ackCoordinatorReject2PC" + "!" + clientIp + "!" + txn);
				}
			}
			
			
			else if (recvMsg[0].equals("ackCoordinatorAccept2PC")) {
				// Only 2PC coord will receive this message
				// When 2 acks are received, release locks
				//
				// Then send commit2PC to other Paxos leaders and client
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String key = clientIp + "!" + txn;
				
				if(ackCoordinatorAccept2PC.containsKey(key)) {
					int val = ackCoordinatorAccept2PC.get(key).intValue();
					val++;
					ackCoordinatorAccept2PC.put(key, new Integer(val));
					
					if(val >= 2) { //got majority of responses
						//2PC coordinator releases his locks
						//2PC coordinator performs transaction
						//TODO is this right?
						for(int i = 0; i < allShards.size(); i++) {
							allShards.get(i).performTransaction(clientIp, true, txn, true);
						}
						
						//2PC coordinator tells everyone else to commit
						int hostId1 = (myHostId + 1) % 3;
						int hostId2 = (myHostId + 2) % 3;
						
						if(Main.coord2PCShard.equals("X")) {
							String commit2PC1 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "Y";
							sendMessage(Main.serverHosts.get(hostId1), commit2PC1);
							String commit2PC2 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "Z";
							sendMessage(Main.serverHosts.get(hostId2), commit2PC2);
						} else if(Main.coord2PCShard.equals("Y")) {
							String commit2PC1 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "X";
							sendMessage(Main.serverHosts.get(hostId1), commit2PC1);
							String commit2PC2 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "Z";
							sendMessage(Main.serverHosts.get(hostId2), commit2PC2);
						} else if (Main.coord2PCShard.equals("Z")) {
							String commit2PC1 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "X";
							sendMessage(Main.serverHosts.get(hostId1), commit2PC1);
							String commit2PC2 = "commit2PC" + "!" + clientIp + "!" + txn + "!" + "Y";
							sendMessage(Main.serverHosts.get(hostId2), commit2PC2);
						}
						
						// 2PC coordinator tells client that its committed
						sendMessage(clientIp, "committed " + txn);
					}
					
				} else {
					//increment quorum count
					ackCoordinatorAccept2PC.put(key, new Integer(1));
				}
			}
			else if(recvMsg[0].equals("ackCoordinatorReject2PC")){
				// Only 2PC coord will receive this message
				// When 2 acks are received, release locks
				// If 2 acks not received, paxos fails - TODO: quit txn
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String key = clientIp + "!" + txn;
				
				if(ackCoordinatorAccept2PC.containsKey(key)) {
					int val = ackCoordinatorAccept2PC.get(key).intValue();
					val--;
					ackCoordinatorAccept2PC.put(key, new Integer(val));
					
				} else {
					// quorum count is 0
					ackCoordinatorAccept2PC.put(key, new Integer(0));
				}
				
			}
			
			else if(recvMsg[0].equals("commit2PC")) {
				// When Paxos leader receives this, replicate log entry
				// of 2PC commit using Paxos again
				// 
				// send repCom to other 2 DCs
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				
				//log
				boolean loggedTransaction = false;
				if(shardId.equals("X")) {
					loggedTransaction = shardX.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Y")) {
					loggedTransaction = shardY.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Z")) {
					loggedTransaction = shardZ.logTransaction(LogEntry.EntryType.COMMIT, txn);
				}
				
			
				for(int i = 0; i < allShards.size(); i++) {
					allShards.get(i).performTransaction(clientIp, true, txn, false);
				}	
					
				
				int hostId1 = (myHostId + 1) % 3;
				int hostId2 = (myHostId + 2) % 3;
				String repCom = "repCom" + "!" + clientIp + "!" + txn + "!" + shardId + "!" + myHostId;
				sendMessage(Main.serverHosts.get(hostId1), repCom);
				sendMessage(Main.serverHosts.get(hostId2), repCom);
			}

			else if(recvMsg[0].equals("repCom")) {
				// send ackRepCom back to sender
				// 
				// 
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String shardId = recvMsg[3];
				String paxosLeaderIp = recvMsg[4];
				
				//log
				if(shardId.equals("X")) {
					shardX.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Y")) {
					shardY.logTransaction(LogEntry.EntryType.COMMIT, txn);
				} else if(shardId.equals("Z")) {
					shardZ.logTransaction(LogEntry.EntryType.COMMIT, txn);
				}
				
				sendMessage(paxosLeaderIp, "ackRepCom" + "!" + clientIp + "!" + txn + "!" + shardId);

				
			}
			
			else if(recvMsg[0].equals("ackRepCom")) {
				// Once these shards receive majority of ackRepComs,
				// release local locks
				// 
				String clientIp = recvMsg[1];
				String txn = recvMsg[2];
				String key = clientIp + "!" + txn;
				
				if(ackRepCom.containsKey(key)) {
					int val = ackRepCom.get(key).intValue();
					val++;
					ackRepCom.put(key, new Integer(val));
					
					if(val >= allShards.size()/2+1) { //got majority of responses
						//Paxos leader releases his locks
						for(int i = 0; i < allShards.size(); i++) {
							allShards.get(i).releaseLocks(clientIp);
						}
					}
					
				} else {
					//increment quorum count
					ackCoordinatorAccept2PC.put(key, new Integer(1));
				}
			}
		}
		
		private void sendAckPaxos(boolean accept, String clientIp, String shardId, String txn, String senderIp) {
			String acceptString = "";
			if(accept) {
				acceptString = "Accept";
			} else {
				acceptString = "Reject";
			}
			
			String acceptPaxosReply = "ack" + acceptString + "Paxos" + "!" 
					+ clientIp + "!" + txn + "!" + shardId;
			
			sendMessage(senderIp, acceptPaxosReply);
		}

		private synchronized void checkAckAcceptPaxosQuorum(boolean accept, String clientIpAddr, String txn) {
			int quorumVal = -9;
			
			if(accept) {
				synchronized(ackAcceptPaxos) {
					quorumVal = ackAcceptPaxos.get(clientIpAddr+"!"+txn);
				}
				
				if(quorumVal >= 2) {
					// Notify 2PC coord that we accept
					String ack2PC = "ackAccept2PC!"+clientIpAddr+"!"+txn;
					sendMessage(Main.coord2PCIp, ack2PC);
				}
			} else {
				synchronized(ackRejectPaxos) {
					quorumVal = ackRejectPaxos.get(clientIpAddr+"!"+txn);
				}
				
				if(quorumVal >= 2) {
					// Notify 2PC coord and client that we reject
					String ack2PC = "ackReject2PC!"+clientIpAddr+"!"+txn;
					sendMessage(Main.coord2PCIp, ack2PC);
				}
			}
			
		}
		
		private synchronized void checkAck2PCQuorum(boolean accept, String clientIp, String txn, String shardId) {
int quorumVal = -9;
			
			if(accept) {
				synchronized(ackAccept2PC) {
					quorumVal = ackAccept2PC.get(clientIp+"!"+txn);
				}
				
				if(quorumVal >= 2) {
					// Notify all cohorts that the coordinator has accepted
					String coordinatorAck2PC = "coordinatorAccept2PC!"+clientIp+"!"+txn + "!" + shardId;
					sendMessage(Main.serverHosts.get((myHostId + 1) % 3), coordinatorAck2PC);
					sendMessage(Main.serverHosts.get((myHostId + 2) % 3), coordinatorAck2PC);
				}
			} else {
				synchronized(ackReject2PC) {
					quorumVal = ackReject2PC.get(clientIp +"!"+txn);
				}
				
				if(quorumVal >= 2) {
					// TODO: notify client that transaction has failed
				}
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
