package spanner;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class DataCenter extends Thread {

	private String myIp;
    
	private ServerSocket serverSocket;
	private Map<String, Integer> pendingTxns = 
			Collections.synchronizedMap(new HashMap<String, Integer>());
	
	Shard shardX; 
	Shard shardY;
	Shard shardZ;
	
	private final int PORT = 3001;
	
	
	// DataCenter constructor
	public DataCenter(int numShardData, String ip) {
		try{
			serverSocket = new ServerSocket(PORT);
			
			myIp = ip;
			
			shardX = new Shard();
			shardY = new Shard();
			shardZ = new Shard();
			
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
		
		
		
		/*
		 * Parse incoming string from client socket
		 */
		private void processInput(String input) {
			System.out.println("Received input: " + input);
			String[] recvMsg = input.split("!");
			
			if(recvMsg.length != 3) {
				return;
			}
			else {
				String ipAddr = recvMsg[1];
				String txn = recvMsg[2];
			}
			
			if (recvMsg[0].equals("prepareClient")) {
				// Upon receiving client 2PC 'prepareClient', acquire
				// exclusive locks and log 2PC 'prepareClient' locally.
				// Then, send 
			}
			
			else if (recvMsg[0].equals("prepareDC")) {
				
			}
			
			else if (recvMsg[0].equals("accept")) {
				
			}
			
			else if (recvMsg[0].equals("yes")) {
				
			}
			
			else if(recvMsg[0].equals("no")) {
				
			}
		}
	}
}
