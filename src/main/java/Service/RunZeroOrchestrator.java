package Service;

import java.io.PrintStream;

/**
 * Orchestrates the "run zero" measurement report.
 *
 * The LatencyTracker is shared with InstrumentedJdbcSink (it accumulates data
 * automatically during the pipeline run). This class just adds the bottleneck
 * and JVM reports on top.
 *
 * Usage:
 *   LatencyTracker tracker = new LatencyTracker();  // shared with InstrumentedJdbcSink
 *   RunZeroOrchestrator rz = new RunZeroOrchestrator(tracker);
 *
 *   // take JVM snapshot before measurement
 *   JvmMetricsCollector.MetricsSnapshot start = rz.getJvmCollector().takeSnapshot(0);
 *   // ... run pipeline for 10 min ...
 *   JvmMetricsCollector.MetricsSnapshot end = rz.getJvmCollector().takeSnapshot(eventCount);
 *
 *   rz.report(jobId, start, end, System.out);
 */
public class RunZeroOrchestrator {

    private final LatencyTracker latencyTracker;
    private final BottleneckDetector bottleneckDetector;
    private final JvmMetricsCollector jvmCollector;

    public RunZeroOrchestrator(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
        this.bottleneckDetector = new BottleneckDetector();
        this.jvmCollector = new JvmMetricsCollector();
    }

    public RunZeroOrchestrator(LatencyTracker latencyTracker, String flinkRestUrl) {
        this.latencyTracker = latencyTracker;
        this.bottleneckDetector = new BottleneckDetector(flinkRestUrl);
        this.jvmCollector = new JvmMetricsCollector(flinkRestUrl);
    }

    public LatencyTracker getLatencyTracker() { return latencyTracker; }
    public BottleneckDetector getBottleneckDetector() { return bottleneckDetector; }
    public JvmMetricsCollector getJvmCollector() { return jvmCollector; }

    /**
     * Prints the full run-zero report.
     */
    public void report(String jobId,
                       JvmMetricsCollector.MetricsSnapshot start,
                       JvmMetricsCollector.MetricsSnapshot end,
                       PrintStream out) {
        out.println("╔══════════════════════════════════════════╗");
        out.println("║          RUN ZERO REPORT                ║");
        out.println("╚══════════════════════════════════════════╝");
        out.println();

        // 0. Cgroup throttle check
        JvmMetricsCollector.CgroupThrottleInfo cgroup = jvmCollector.checkCgroupThrottling();
        if (cgroup != null) {
            cgroup.printReport(out);
            out.println();
        }

        // 1. Latency segments
        latencyTracker.printReport(out);
        out.println();

        String dominant = latencyTracker.dominantSegmentAtP99();
        if ("dbWrite".equals(dominant)) {
            out.println(">>> DB WRITE domina la latenza al p99.");
            out.println(">>> R_sust è determinato dal DB. Ottimizzazioni Flink invisibili.");
        } else if (dominant != null) {
            out.printf(">>> Segmento dominante al p99: %s%n", dominant);
        }
        out.println();

        // 2. Bottleneck
        try {
            BottleneckDetector.BottleneckReport bottleneck = bottleneckDetector.detect(jobId);
            bottleneck.printReport(out);
        } catch (Exception e) {
            out.printf("  [Bottleneck detection failed: %s]%n", e.getMessage());
        }
        out.println();

        // 3. JVM metrics
        JvmMetricsCollector.MetricsReport jvmReport = jvmCollector.computeReport(start, end);
        jvmReport.printReport(out);
        out.println();

        out.println("═══════════════════════════════════════════");
    }
}
