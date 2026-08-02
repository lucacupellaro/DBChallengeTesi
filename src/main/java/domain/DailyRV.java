package domain;

import java.io.Serializable;

public class DailyRV implements Serializable {
    private String symbol;
    private long timestamp;
    private double rvDaily;

    public DailyRV() {}

    public DailyRV(String symbol, long timestamp, double rvDaily) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getRvDaily() { return rvDaily; }

    @Override
    public String toString() {
        return "DailyRV{" + symbol + ", t=" + timestamp + ", rv=" + rvDaily + "}";
    }
}
