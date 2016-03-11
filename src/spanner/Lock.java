package spanner;

import java.util.ArrayList;
import java.util.List;


public class Lock {
	private int lockStatus;	
	private List<String> clientIp; //the client holding the lock

	public Lock() {
		lockStatus = 0;
        clientIp = new ArrayList<String>();
	}

	public synchronized void setLockStatus(int i) {
		if(i >= 0 && i <= 2)
			lockStatus = i;
	}

	public synchronized int getLockStatus() { return lockStatus; }

	public synchronized void addClientIp(String c) { clientIp.add(c); }

    public synchronized boolean removeClientIp(String c) {
        return clientIp.remove(c);
    }

    public synchronized void removeAllClients() {
        clientIp = new ArrayList<String>();
    }

	public synchronized List<String> getClientIp() { return clientIp; }
}