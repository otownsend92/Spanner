package spanner;

/**
 * Created by jhughes on 3/11/16.
 */

public class Transaction {
    private String type;
    private String variable;
    private int writeValue;

    public Transaction(String type, String variable, int writeValue) {
        this.type = type;
        this.variable = variable;
        this.writeValue = writeValue;
    }

    public boolean isRead() { return type.equals("r"); }

    public String getType() { return type; }

    public String getVariable() { return variable; }

    public int getWriteValue() { return writeValue; }

}
