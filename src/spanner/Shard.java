package spanner;

import sun.font.TrueTypeFont;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Iterator;


public class Shard {
    private final int UNLOCKED = 0;
    private final int READ = 1;
    private final int WRITE = 2;
    
    //maps to store number of accepts/rejects
  	private Map<String, Integer> ackAcceptPaxos =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	private Map<String, Integer> ack2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	private Map<String, Integer> ackCoordinatorAccept2PC =
  			Collections.synchronizedMap(new HashMap<String, Integer>());
  	private Map<String, Integer> ackRepCom =
  			Collections.synchronizedMap(new HashMap<String, Integer>());

	ArrayList<LogEntry> transactionLog;

	//these two need to be the same length
    //indexed by variable name (a,b,c...)
	Map<String, Lock> lockTable;
	Map<String, Integer> data;
	
	String readValues;
	String shardId;

	public Shard(String shardId) {
		lockTable = new HashMap<String, Lock>();
		data = new HashMap<String, Integer>();
	}

	/**
	 * Initializes this shard by populating the lockTable and the data Maps
	 */
	public Shard(String varName, int numData) {
		lockTable = new HashMap<String, Lock>();
		data = new HashMap<String, Integer>();

		for(int i = 0; i < numData; i++) {
			String newVar = varName + Integer.toString(i);
			data.put(newVar, new Integer(0));
			lockTable.put(newVar, new Lock());
		}
	}

	/**
	 * Phase 1 of two phase commit - can I perform this transaction? Try getting all the locks
	 * @return: true if it can gather all locks.
	 */
	public boolean processTransaction(String clientIp, String rawTransaction) {
		List<Transaction> trans = tokenizeTransaction(rawTransaction);
		boolean firstInsert = true;
		StringBuilder sb = new StringBuilder();

		for(Transaction tran:trans) {
			System.out.println("Shard: " + tran.getType() + ", " + tran.getVariable() + ", " + tran.getWriteValue());

			String key = tran.getVariable();
			if(tran.isRead()) {
				//if reads, save all the reads
				Integer value = data.get(key);
				if(value == null)
					continue;
				if(firstInsert == true) {
					firstInsert = false;
				} else {
					sb.append(", ");
				}
				sb.append(key + " = " + value.toString());
			}
		}

		//at this point, gather all the read values
		synchronized(this) {
			readValues = sb.toString();
		}


		return gatherLocks(clientIp, trans);
	}

	private boolean gatherLocks(String clientIp, List<Transaction> trans) {
		for(Transaction tran:trans) {
			if(!lockTable.containsKey(tran.getVariable())) //don't look in lockTable for variables we don't store
				continue;

			synchronized(this) {
				Lock lock = lockTable.get(tran.getVariable());
				int lockStatus = lock.getLockStatus();
				List<String> lockIp = lock.getClientIp();

				if(tran.isRead()) { //processing read transaction
					if(lockStatus == WRITE && !lockIp.contains(clientIp)) { //write lock has been acquired (by someone else),  we can't get our read lock
						return false;
					}

					if(lockStatus == UNLOCKED){
						//acquire read lock
						lock.addClientIp(clientIp);
						lock.setLockStatus(READ);
					}
				} else { //processing write transaction
					if(lockStatus == WRITE && !lockIp.contains(clientIp)) { //write lock has been acquired by someone else,  we can't get our write lock
						return false;
					} else if(lockStatus == WRITE && lockIp.contains(clientIp)) { //we got the write lock already
						continue;
					}

					lock.removeAllClients(); //remove all clients that have had a read lock

					//acquire write lock
					lock.addClientIp(clientIp);
					lock.setLockStatus(WRITE);
				}

			}
		}
		return true;
	}


	/*
 	* Given a string of input, returns a list of transactions
 	*/
	private List<Transaction> tokenizeTransaction(String rawTransaction) {
		List<Transaction> trans = new ArrayList<Transaction>();
		StringTokenizer st = new StringTokenizer(rawTransaction, ",");

		while(st.hasMoreElements()) {
			String type = (String)st.nextElement();
			if(type.equals("r")) {
				String variable = (String)st.nextElement();
				if(variable == null) {
					System.out.println("read is wrong");
					return null;
				}
				Transaction tran = new Transaction(type, variable, 0);
				trans.add(tran);

			} else if(type.equals("w")) {
				String variable = (String)st.nextElement();
				if(variable == null) {
					System.out.println("write is wrong");
					return null;
				}
				String write = (String)st.nextElement();
				if(write == null) {
					System.out.println("write is wrong");
					return null;
				}
				int writeValue = Integer.parseInt(write);

				Transaction tran = new Transaction(type, variable, writeValue);
				trans.add(tran);
			} else {
				System.out.println("bro wtf");
				return null;
			}
		}

		return trans;
	}

	/**
	 * Creates a new log entry, returns true if the rawTransaction contains an operation
	 * relevant to this shard i.e. if this is shard x, and we have a r,x1 then it will log
	 * and return true
	 */
	public boolean logTransaction(LogEntry.EntryType e, String rawTransaction){

		boolean logTrans = false;
		for(Transaction tran:tokenizeTransaction(rawTransaction)) {
			if (lockTable.containsKey(tran.getVariable())) //don't log a transaction
				logTrans = true;
		}

		if(logTrans){
			transactionLog.add(new LogEntry(e, rawTransaction));
			return true;
		}else{
			return false;
		}
	}


}
