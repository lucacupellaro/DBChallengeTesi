package domain;

import java.io.Serializable;

public class DailyRV implements Serializable {
    private String symbol;
    private long timestamp;
    private double rvDaily;

    // wall-clock di quando la finestra ha emesso questo risultato (per latency tracking)
    private long windowEmitWallClock;

    public DailyRV() {}

    public DailyRV(String symbol, long timestamp, double rvDaily) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
    }

    public DailyRV(String symbol, long timestamp, double rvDaily, long windowEmitWallClock) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
        this.windowEmitWallClock = windowEmitWallClock;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getRvDaily() { return rvDaily; }
    public long getWindowEmitWallClock() { return windowEmitWallClock; }
    public void setWindowEmitWallClock(long windowEmitWallClock) { this.windowEmitWallClock = windowEmitWallClock; }

    @Override
    public String toString() {
        return "DailyRV{" + symbol + ", t=" + timestamp + ", rv=" + rvDaily + "}";
    }
}
