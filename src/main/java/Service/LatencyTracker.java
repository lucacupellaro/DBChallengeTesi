package Service;

import domain.LatencyRecord;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accumulates per-segment latencies using HdrHistogram.
 *
 * Three segments:
 *   internalQueue (t0→t1) : coda tra operatori Flink
 *   compute       (t1→t2) : buffer + chiamata ML
 *   dbWrite       (t2→t3) : scrittura PostgreSQL
 */
public class LatencyTracker implements Serializable {

    private static final long HIGHEST_TRACKABLE_VALUE_US = 60_000_000L;
    private static final int SIGNIFICANT_DIGITS = 3;

    private final ConcurrentHashMap<String, SegmentHistograms> perSymbol = new ConcurrentHashMap<>();
    private final SegmentHistograms global = new SegmentHistograms();

    private long clockSkewCount = 0;
    private long totalRecorded = 0;

    public void record(LatencyRecord lr) {
        if (lr.hasClockSkew()) {
            clockSkewCount++;
            return;
        }

        long queueUs = lr.internalQueueMs() * 1000;
        long computeUs = lr.computeMs() * 1000;
        long dbUs = lr.dbWriteMs() * 1000;
        long e2eUs = lr.endToEndMs() * 1000;

        global.record(queueUs, computeUs, dbUs, e2eUs);
        perSymbol.computeIfAbsent(lr.getSymbol(), k -> new SegmentHistograms())
                .record(queueUs, computeUs, dbUs, e2eUs);

        totalRecorded++;
    }

    public void printReport(PrintStream out) {
        out.println("=== LATENCY REPORT (global) ===");
        out.printf("Total recorded: %d | Clock-skew discarded: %d%n", totalRecorded, clockSkewCount);
        global.printTo(out);

        out.println();
        for (Map.Entry<String, SegmentHistograms> entry : perSymbol.entrySet()) {
            out.printf("--- Symbol: %s ---%n", entry.getKey());
            entry.getValue().printTo(out);
        }
        out.println("=== END LATENCY REPORT ===");
    }

    /**
     * Returns which segment dominates the end-to-end latency at p99.
     * If returns "dbWrite" → R_sust is determined by the DB.
     */
    public String dominantSegmentAtP99() {
        Map<String, Map<String, Long>> snap = global.snapshot();
        String dominant = null;
        long maxP99 = 0;
        for (Map.Entry<String, Map<String, Long>> entry : snap.entrySet()) {
            if ("endToEnd".equals(entry.getKey())) continue;
            long p99 = entry.getValue().get("p99");
            if (p99 > maxP99) {
                maxP99 = p99;
                dominant = entry.getKey();
            }
        }
        return dominant;
    }

    public Map<String, Map<String, Long>> getGlobalSnapshot() {
        return global.snapshot();
    }

    public long getTotalRecorded() { return totalRecorded; }
    public long getClockSkewCount() { return clockSkewCount; }

    public void reset() {
        global.reset();
        perSymbol.clear();
        totalRecorded = 0;
        clockSkewCount = 0;
    }

    // ---- Inner class: one histogram per segment ----

    static class SegmentHistograms implements Serializable {
        final Histogram internalQueue = createHistogram();
        final Histogram compute = createHistogram();
        final Histogram dbWrite = createHistogram();
        final Histogram endToEnd = createHistogram();

        private static Histogram createHistogram() {
            return new Histogram(HIGHEST_TRACKABLE_VALUE_US, SIGNIFICANT_DIGITS);
        }

        void record(long queueUs, long computeUs, long dbUs, long e2eUs) {
            internalQueue.recordValue(clamp(queueUs));
            compute.recordValue(clamp(computeUs));
            dbWrite.recordValue(clamp(dbUs));
            endToEnd.recordValue(clamp(e2eUs));
        }

        private static long clamp(long value) {
            return Math.min(Math.max(value, 0), HIGHEST_TRACKABLE_VALUE_US);
        }

        void printTo(PrintStream out) {
            printSegment(out, "internalQueue(t0→t1)", internalQueue);
            printSegment(out, "compute      (t1→t2)", compute);
            printSegment(out, "dbWrite      (t2→t3)", dbWrite);
            printSegment(out, "endToEnd     (t0→t3)", endToEnd);
        }

        private void printSegment(PrintStream out, String label, Histogram h) {
            if (h.getTotalCount() == 0) {
                out.printf("  %-25s  (no data)%n", label);
                return;
            }
            out.printf("  %-25s  p50=%6dµs  p90=%6dµs  p99=%6dµs  p99.9=%6dµs  max=%6dµs  (n=%d)%n",
                    label,
                    h.getValueAtPercentile(50),
                    h.getValueAtPercentile(90),
                    h.getValueAtPercentile(99),
                    h.getValueAtPercentile(99.9),
                    h.getMaxValue(),
                    h.getTotalCount());
        }

        Map<String, Map<String, Long>> snapshot() {
            Map<String, Map<String, Long>> result = new LinkedHashMap<>();
            result.put("internalQueue", percentiles(internalQueue));
            result.put("compute", percentiles(compute));
            result.put("dbWrite", percentiles(dbWrite));
            result.put("endToEnd", percentiles(endToEnd));
            return result;
        }

        private static Map<String, Long> percentiles(Histogram h) {
            Map<String, Long> m = new LinkedHashMap<>();
            m.put("p50", h.getValueAtPercentile(50));
            m.put("p90", h.getValueAtPercentile(90));
            m.put("p99", h.getValueAtPercentile(99));
            m.put("p999", h.getValueAtPercentile(99.9));
            m.put("max", h.getMaxValue());
            m.put("count", h.getTotalCount());
            return m;
        }

        void reset() {
            internalQueue.reset();
            compute.reset();
            dbWrite.reset();
            endToEnd.reset();
        }
    }
}
