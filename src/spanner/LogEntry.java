package spanner;

import java.util.Enumeration;

/**
 * Created by jhughes on 3/11/16.
 */
public class LogEntry {

    private EntryType entryType;
    private String rawTx;

    public enum EntryType{
        PREPARE, COMMIT
    }

    public LogEntry(EntryType entryType, String tx) {
        this.entryType = entryType;
        this.rawTx = tx;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public void setEntryType(EntryType entryType) {
        this.entryType = entryType;
    }

    public String getRawTx() {
        return rawTx;
    }

    public void setRawTx(String rawTx) {
        this.rawTx = rawTx;
    }
}
