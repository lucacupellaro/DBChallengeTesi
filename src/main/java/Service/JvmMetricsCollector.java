package Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Collects the minimal CPU/memory metrics needed for the "run zero":
 *
 * 1. CPU-seconds (cumulative) → then µs/event
 * 2. GC overhead % and average pause
 * 3. Cgroup throttling check (if running in a container)
 *
 * These are all from cumulative counters (exact, no sampling noise).
 *
 * IMPORTANT: If running in a container, check cgroup nr_throttled FIRST.
 * If throttled, you're not measuring Flink — you're measuring the cgroup scheduler.
 * This is a precondition of validity, not an optimization to do later.
 */
public class JvmMetricsCollector implements Serializable {

    private final String flinkRestUrl;

    // Snapshot values for delta computation
    private long snapshotTimeMs;
    private long snapshotCpuTimeNs;
    private long snapshotGcCount;
    private long snapshotGcTimeMs;
    private long snapshotEventCount;

    public JvmMetricsCollector(String flinkRestUrl) {
        this.flinkRestUrl = flinkRestUrl.endsWith("/")
                ? flinkRestUrl.substring(0, flinkRestUrl.length() - 1)
                : flinkRestUrl;
    }

    public JvmMetricsCollector() {
        this("http://localhost:8081");
    }

    /**
     * Takes a snapshot of cumulative metrics. Call this at the START and END
     * of the measurement window, then use computeReport() to get the deltas.
     */
    public MetricsSnapshot takeSnapshot(long eventCount) {
        long cpuTimeNs = 0;
        long gcCount = 0;
        long gcTimeMs = 0;

        // JVM CPU time from thread MXBean (process-level)
        com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        cpuTimeNs = osBean.getProcessCpuTime();

        // GC stats from all collectors
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += gc.getCollectionCount();
            gcTimeMs += gc.getCollectionTime();
        }

        return new MetricsSnapshot(System.currentTimeMillis(), cpuTimeNs, gcCount, gcTimeMs, eventCount);
    }

    /**
     * Takes a snapshot by querying Flink REST API (for remote TaskManagers).
     */
    public MetricsSnapshot takeSnapshotFromFlink(String jobId, long eventCount) throws Exception {
        // Query Flink's JVM metrics via REST
        String tmListJson = httpGet(flinkRestUrl + "/taskmanagers");
        JsonObject tmList = JsonParser.parseString(tmListJson).getAsJsonObject();
        JsonArray taskmanagers = tmList.getAsJsonArray("taskmanagers");

        long totalCpuNs = 0;
        long totalGcCount = 0;
        long totalGcTimeMs = 0;

        for (JsonElement tm : taskmanagers) {
            String tmId = tm.getAsJsonObject().get("id").getAsString();

            String metricsUrl = String.format(
                    "%s/taskmanagers/%s/metrics?get=Status.JVM.CPU.Time,Status.JVM.GarbageCollector.All.Count,Status.JVM.GarbageCollector.All.Time",
                    flinkRestUrl, tmId);
            String metricsJson = httpGet(metricsUrl);
            JsonArray metrics = JsonParser.parseString(metricsJson).getAsJsonArray();

            for (JsonElement m : metrics) {
                JsonObject metric = m.getAsJsonObject();
                String id = metric.get("id").getAsString();
                if ("Status.JVM.CPU.Time".equals(id)) {
                    totalCpuNs += metric.get("value").getAsLong();
                } else if (id.contains("GarbageCollector") && id.endsWith(".Count")) {
                    totalGcCount += metric.get("value").getAsLong();
                } else if (id.contains("GarbageCollector") && id.endsWith(".Time")) {
                    totalGcTimeMs += metric.get("value").getAsLong();
                }
            }
        }

        return new MetricsSnapshot(System.currentTimeMillis(), totalCpuNs, totalGcCount, totalGcTimeMs, eventCount);
    }

    /**
     * Computes the deltas between two snapshots and returns a report.
     */
    public MetricsReport computeReport(MetricsSnapshot start, MetricsSnapshot end) {
        long wallTimeMs = end.wallTimeMs - start.wallTimeMs;
        long deltaCpuNs = end.cpuTimeNs - start.cpuTimeNs;
        long deltaGcCount = end.gcCount - start.gcCount;
        long deltaGcTimeMs = end.gcTimeMs - start.gcTimeMs;
        long deltaEvents = end.eventCount - start.eventCount;

        double cpuSeconds = deltaCpuNs / 1_000_000_000.0;
        double wallSeconds = wallTimeMs / 1000.0;
        double gcOverheadPct = wallTimeMs > 0 ? (deltaGcTimeMs * 100.0 / wallTimeMs) : 0;
        double avgGcPauseMs = deltaGcCount > 0 ? (double) deltaGcTimeMs / deltaGcCount : 0;
        double usPerEvent = deltaEvents > 0 ? (deltaCpuNs / 1000.0) / deltaEvents : 0;

        return new MetricsReport(cpuSeconds, wallSeconds, gcOverheadPct, avgGcPauseMs,
                usPerEvent, deltaEvents, deltaGcCount);
    }

    /**
     * Check cgroup v2 CPU throttling. Returns null if not in a cgroup.
     * Non-zero nr_throttled means you're measuring the cgroup scheduler, not Flink.
     */
    public CgroupThrottleInfo checkCgroupThrottling() {
        // cgroup v2
        Path cgroupV2 = Paths.get("/sys/fs/cgroup/cpu.stat");
        if (Files.exists(cgroupV2)) {
            return parseCgroupV2(cgroupV2);
        }

        // cgroup v1
        Path cgroupV1 = Paths.get("/sys/fs/cgroup/cpu/cpu.stat");
        if (Files.exists(cgroupV1)) {
            return parseCgroupV1(cgroupV1);
        }

        return null;
    }

    private CgroupThrottleInfo parseCgroupV2(Path path) {
        try {
            long nrPeriods = 0, nrThrottled = 0, throttledUs = 0;
            for (String line : Files.readAllLines(path)) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;
                switch (parts[0]) {
                    case "nr_periods":   nrPeriods = Long.parseLong(parts[1]); break;
                    case "nr_throttled": nrThrottled = Long.parseLong(parts[1]); break;
                    case "throttled_usec": throttledUs = Long.parseLong(parts[1]); break;
                }
            }
            return new CgroupThrottleInfo(nrPeriods, nrThrottled, throttledUs);
        } catch (Exception e) {
            return null;
        }
    }

    private CgroupThrottleInfo parseCgroupV1(Path path) {
        try {
            long nrPeriods = 0, nrThrottled = 0, throttledNs = 0;
            for (String line : Files.readAllLines(path)) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;
                switch (parts[0]) {
                    case "nr_periods":       nrPeriods = Long.parseLong(parts[1]); break;
                    case "nr_throttled":     nrThrottled = Long.parseLong(parts[1]); break;
                    case "throttled_time":   throttledNs = Long.parseLong(parts[1]); break;
                }
            }
            return new CgroupThrottleInfo(nrPeriods, nrThrottled, throttledNs / 1000);
        } catch (Exception e) {
            return null;
        }
    }

    private String httpGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    // ---- Data classes ----

    public static class MetricsSnapshot implements Serializable {
        final long wallTimeMs;
        final long cpuTimeNs;
        final long gcCount;
        final long gcTimeMs;
        final long eventCount;

        public MetricsSnapshot(long wallTimeMs, long cpuTimeNs, long gcCount,
                               long gcTimeMs, long eventCount) {
            this.wallTimeMs = wallTimeMs;
            this.cpuTimeNs = cpuTimeNs;
            this.gcCount = gcCount;
            this.gcTimeMs = gcTimeMs;
            this.eventCount = eventCount;
        }
    }

    public static class MetricsReport implements Serializable {
        private final double cpuSeconds;
        private final double wallSeconds;
        private final double gcOverheadPct;
        private final double avgGcPauseMs;
        private final double usPerEvent;
        private final long totalEvents;
        private final long gcCollections;

        public MetricsReport(double cpuSeconds, double wallSeconds,
                             double gcOverheadPct, double avgGcPauseMs,
                             double usPerEvent, long totalEvents, long gcCollections) {
            this.cpuSeconds = cpuSeconds;
            this.wallSeconds = wallSeconds;
            this.gcOverheadPct = gcOverheadPct;
            this.avgGcPauseMs = avgGcPauseMs;
            this.usPerEvent = usPerEvent;
            this.totalEvents = totalEvents;
            this.gcCollections = gcCollections;
        }

        public double getCpuSeconds() { return cpuSeconds; }
        public double getWallSeconds() { return wallSeconds; }
        public double getGcOverheadPct() { return gcOverheadPct; }
        public double getAvgGcPauseMs() { return avgGcPauseMs; }
        public double getUsPerEvent() { return usPerEvent; }
        public long getTotalEvents() { return totalEvents; }
        public long getGcCollections() { return gcCollections; }

        public void printReport(PrintStream out) {
            out.println("=== JVM METRICS REPORT ===");
            out.printf("  Wall time:         %.1f s%n", wallSeconds);
            out.printf("  CPU time:          %.3f s%n", cpuSeconds);
            out.printf("  CPU utilization:   %.1f%%%n", wallSeconds > 0 ? (cpuSeconds / wallSeconds) * 100 : 0);
            out.printf("  µs CPU / event:    %.1f µs%n", usPerEvent);
            out.printf("  Events processed:  %d%n", totalEvents);
            out.printf("  GC collections:    %d%n", gcCollections);
            out.printf("  GC overhead:       %.2f%%%n", gcOverheadPct);
            out.printf("  Avg GC pause:      %.1f ms%n", avgGcPauseMs);

            if (gcOverheadPct > 5) {
                out.println("  >>> WARNING: GC overhead > 5% — investigate heap sizing or allocation rate");
            }
            out.println("=== END JVM METRICS REPORT ===");
        }
    }

    public static class CgroupThrottleInfo implements Serializable {
        private final long nrPeriods;
        private final long nrThrottled;
        private final long throttledUs;

        public CgroupThrottleInfo(long nrPeriods, long nrThrottled, long throttledUs) {
            this.nrPeriods = nrPeriods;
            this.nrThrottled = nrThrottled;
            this.throttledUs = throttledUs;
        }

        public long getNrPeriods() { return nrPeriods; }
        public long getNrThrottled() { return nrThrottled; }
        public long getThrottledUs() { return throttledUs; }
        public boolean isThrottled() { return nrThrottled > 0; }

        public void printReport(PrintStream out) {
            out.println("=== CGROUP THROTTLE CHECK ===");
            out.printf("  nr_periods:   %d%n", nrPeriods);
            out.printf("  nr_throttled: %d%n", nrThrottled);
            out.printf("  throttled:    %.3f s%n", throttledUs / 1_000_000.0);

            if (isThrottled()) {
                double pct = nrPeriods > 0 ? (nrThrottled * 100.0 / nrPeriods) : 0;
                out.printf("  >>> THROTTLED %.1f%% of periods — measurements are INVALID.%n", pct);
                out.println("  >>> Increase CPU limits or reduce parallelism before benchmarking.");
            } else {
                out.println("  >>> OK: no throttling detected");
            }
            out.println("=== END CGROUP CHECK ===");
        }
    }
}
