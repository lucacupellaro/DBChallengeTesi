package Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Identifies the pipeline bottleneck by querying Flink REST API metrics.
 *
 * Rule: the first operator that is busy-but-NOT-backpressured (going downstream
 * from the source) is the bottleneck. Everything upstream of it is backpressured
 * by it; everything downstream is starved.
 *
 * IMPORTANT: use disableChaining() during profiling so that parser+window+sink
 * report separate busyTime values. Re-enable chaining for actual performance runs.
 *
 * Uses per-subtask metrics and looks at MAX across subtasks — with ~5000 symbols
 * skew is likely, and the average hides exactly the saturated subtask that
 * determines everything.
 */
public class BottleneckDetector implements Serializable {

    private final String flinkRestUrl;

    public BottleneckDetector(String flinkRestUrl) {
        this.flinkRestUrl = flinkRestUrl.endsWith("/")
                ? flinkRestUrl.substring(0, flinkRestUrl.length() - 1)
                : flinkRestUrl;
    }

    public BottleneckDetector() {
        this("http://localhost:8081");
    }

    /**
     * Detects the bottleneck operator for the given job.
     * Returns a report with per-operator busyTime and backPressure (max across subtasks).
     */
    public BottleneckReport detect(String jobId) throws Exception {
        // 1. Get job vertices (operators)
        String jobJson = httpGet(flinkRestUrl + "/jobs/" + jobId);
        JsonObject job = JsonParser.parseString(jobJson).getAsJsonObject();
        JsonArray vertices = job.getAsJsonArray("vertices");

        List<OperatorMetrics> operators = new ArrayList<>();

        for (JsonElement v : vertices) {
            JsonObject vertex = v.getAsJsonObject();
            String vertexId = vertex.get("id").getAsString();
            String name = vertex.get("name").getAsString();
            int parallelism = vertex.get("parallelism").getAsInt();

            // 2. Get per-subtask metrics
            double maxBusyMs = 0;
            double maxBackPressuredMs = 0;

            for (int subtask = 0; subtask < parallelism; subtask++) {
                String metricsUrl = String.format(
                        "%s/jobs/%s/vertices/%s/subtasks/%d/metrics?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond",
                        flinkRestUrl, jobId, vertexId, subtask);

                String metricsJson = httpGet(metricsUrl);
                JsonArray metrics = JsonParser.parseString(metricsJson).getAsJsonArray();

                for (JsonElement m : metrics) {
                    JsonObject metric = m.getAsJsonObject();
                    String id = metric.get("id").getAsString();
                    double value = metric.get("value").getAsDouble();

                    if ("busyTimeMsPerSecond".equals(id)) {
                        maxBusyMs = Math.max(maxBusyMs, value);
                    } else if ("backPressuredTimeMsPerSecond".equals(id)) {
                        maxBackPressuredMs = Math.max(maxBackPressuredMs, value);
                    }
                }
            }

            operators.add(new OperatorMetrics(name, vertexId, parallelism,
                    maxBusyMs, maxBackPressuredMs));
        }

        // 3. Find bottleneck: first operator that is busy but not backpressured
        OperatorMetrics bottleneck = null;
        for (OperatorMetrics op : operators) {
            if (op.isBusy() && !op.isBackPressured()) {
                bottleneck = op;
                break;
            }
        }

        // If all operators are backpressured, the sink is likely the bottleneck
        if (bottleneck == null && !operators.isEmpty()) {
            bottleneck = operators.get(operators.size() - 1);
        }

        return new BottleneckReport(operators, bottleneck);
    }

    /**
     * Lists all running job IDs.
     */
    public List<String> getRunningJobIds() throws Exception {
        String json = httpGet(flinkRestUrl + "/jobs/overview");
        JsonObject overview = JsonParser.parseString(json).getAsJsonObject();
        JsonArray jobs = overview.getAsJsonArray("jobs");

        List<String> ids = new ArrayList<>();
        for (JsonElement j : jobs) {
            JsonObject jObj = j.getAsJsonObject();
            if ("RUNNING".equals(jObj.get("state").getAsString())) {
                ids.add(jObj.get("jid").getAsString());
            }
        }
        return ids;
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

    public static class OperatorMetrics implements Serializable {
        private final String name;
        private final String vertexId;
        private final int parallelism;
        private final double maxBusyMsPerSec;
        private final double maxBackPressuredMsPerSec;

        public OperatorMetrics(String name, String vertexId, int parallelism,
                               double maxBusyMsPerSec, double maxBackPressuredMsPerSec) {
            this.name = name;
            this.vertexId = vertexId;
            this.parallelism = parallelism;
            this.maxBusyMsPerSec = maxBusyMsPerSec;
            this.maxBackPressuredMsPerSec = maxBackPressuredMsPerSec;
        }

        /** Operator is considered busy if max subtask busyTime > 700 ms/s */
        public boolean isBusy() { return maxBusyMsPerSec > 700; }

        /** Operator is backpressured if max subtask backPressuredTime > 100 ms/s */
        public boolean isBackPressured() { return maxBackPressuredMsPerSec > 100; }

        /** Utilization ρ = busyTime / 1000 */
        public double utilization() { return maxBusyMsPerSec / 1000.0; }

        public String getName() { return name; }
        public String getVertexId() { return vertexId; }
        public int getParallelism() { return parallelism; }
        public double getMaxBusyMsPerSec() { return maxBusyMsPerSec; }
        public double getMaxBackPressuredMsPerSec() { return maxBackPressuredMsPerSec; }

        @Override
        public String toString() {
            return String.format("%-40s busy=%.0f ms/s  backPressured=%.0f ms/s  ρ=%.2f  [%s%s]  (par=%d)",
                    name, maxBusyMsPerSec, maxBackPressuredMsPerSec, utilization(),
                    isBusy() ? "BUSY" : "idle",
                    isBackPressured() ? "+BP" : "",
                    parallelism);
        }
    }

    public static class BottleneckReport implements Serializable {
        private final List<OperatorMetrics> operators;
        private final OperatorMetrics bottleneck;

        public BottleneckReport(List<OperatorMetrics> operators, OperatorMetrics bottleneck) {
            this.operators = operators;
            this.bottleneck = bottleneck;
        }

        public List<OperatorMetrics> getOperators() { return operators; }
        public OperatorMetrics getBottleneck() { return bottleneck; }

        public void printReport(java.io.PrintStream out) {
            out.println("=== BOTTLENECK REPORT ===");
            for (OperatorMetrics op : operators) {
                out.println("  " + op);
            }
            if (bottleneck != null) {
                out.printf("%n  >>> BOTTLENECK: %s (ρ=%.2f)%n", bottleneck.getName(), bottleneck.utilization());
            } else {
                out.println("  >>> No bottleneck detected (pipeline idle or under-loaded)");
            }
            out.println("=== END BOTTLENECK REPORT ===");
        }
    }
}
