package domain;

import java.io.Serializable;

/**
 * Carries wall-clock timestamps from window emission to DB ack.
 *
 * t0 = window emits the result (System.currentTimeMillis() in ProcessWindowFunction)
 * t1 = buffer-and-predict receives the record
 * t2 = sink receives the record
 * t3 = DB confirms the write
 *
 * Three segments:
 *   t0→t1 : internal queue between operators
 *   t1→t2 : computation (buffer, ML prediction)
 *   t2→t3 : DB write
 */
public class LatencyRecord implements Serializable {

    private String symbol;
    private long t0WindowEmit;
    private long t1PredictReceive;
    private long t2SinkReceive;
    private long t3DbAck;

    public LatencyRecord() {}

    public LatencyRecord(String symbol, long t0WindowEmit) {
        this.symbol = symbol;
        this.t0WindowEmit = t0WindowEmit;
    }

    // --- Segment durations (ms) ---

    public long internalQueueMs() { return t1PredictReceive - t0WindowEmit; }
    public long computeMs() { return t2SinkReceive - t1PredictReceive; }
    public long dbWriteMs() { return t3DbAck - t2SinkReceive; }
    public long endToEndMs() { return t3DbAck - t0WindowEmit; }

    // --- Getters / Setters ---

    public String getSymbol() { return symbol; }
    public long getT0WindowEmit() { return t0WindowEmit; }
    public long getT1PredictReceive() { return t1PredictReceive; }
    public long getT2SinkReceive() { return t2SinkReceive; }
    public long getT3DbAck() { return t3DbAck; }

    public void setSymbol(String symbol) { this.symbol = symbol; }
    public void setT0WindowEmit(long t0) { this.t0WindowEmit = t0; }
    public void setT1PredictReceive(long t1) { this.t1PredictReceive = t1; }
    public void setT2SinkReceive(long t2) { this.t2SinkReceive = t2; }
    public void setT3DbAck(long t3) { this.t3DbAck = t3; }

    public boolean hasClockSkew() {
        return internalQueueMs() < 0 || computeMs() < 0 || dbWriteMs() < 0;
    }

    @Override
    public String toString() {
        return String.format("Latency{%s, queue=%dms, compute=%dms, db=%dms, e2e=%dms%s}",
                symbol, internalQueueMs(), computeMs(), dbWriteMs(), endToEndMs(),
                hasClockSkew() ? " CLOCK_SKEW!" : "");
    }
}
