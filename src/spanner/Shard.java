package spanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Iterator;


public class Shard {
    private final int UNLOCKED = 0;
    private final int READ = 1;
    private final int WRITE = 2;

	ArrayList<String> transactionLog;

	//these two need to be the same length
    //indexed by variable name (a,b,c...)
	Map<String, Lock> lockTable;
	Map<String, Integer> data;
	
	String readValues;

	public Shard() { 
		lockTable = new HashMap<String, Lock>();
		data = new HashMap<String, Integer>();
	}
}
