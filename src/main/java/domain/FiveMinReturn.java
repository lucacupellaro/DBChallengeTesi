package domain;

import java.io.Serializable;

public class FiveMinReturn implements Serializable {
    private String symbol;
    private long timestamp;
    private double logReturn;

    public FiveMinReturn() {}

    public FiveMinReturn(String symbol, long timestamp, double logReturn) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.logReturn = logReturn;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getLogReturn() { return logReturn; }

    @Override
    public String toString() {
        return "FiveMinReturn{" + symbol + ", t=" + timestamp + ", r=" + logReturn + "}";
    }
}
