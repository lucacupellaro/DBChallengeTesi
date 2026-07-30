package Service;

import domain.DailyRV;
import domain.FiveMinReturn;
import domain.Prediction;
import domain.Tick;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class FlinkJob {

    private static final String TOPIC = "ticks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "flink-tick-consumer";
    private static final String ML_SERVER_URL = "http://localhost:8080/predict";
    private static final int BUFFER_SIZE = 22; // giorni di storia per RV monthly

    // JDBC
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5433/volatility";
    private static final String JDBC_USER = "flink";
    private static final String JDBC_PASSWORD = "flink";

    public void start() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Checkpointing: snapshot dello stato ogni 60s ---
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // --- Kafka source ---
        KafkaSource<Tick> kafkaSource = KafkaSource.<Tick>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                Tick.class, SCHEMA_REGISTRY_URL))
                .build();

        WatermarkStrategy<Tick> watermarkStrategy = WatermarkStrategy
                .<Tick>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((tick, recordTimestamp) -> tick.getTimestamp());

        DataStream<Tick> tickStream = env.fromSource(
                kafkaSource, watermarkStrategy, "kafka-ticks-source");

        // --- Filtro tick invalidi ---
        DataStream<Tick> cleanStream = tickStream.filter(tick ->
                tick.getTimestamp() > 0
                && tick.getBid() > 0
                && tick.getAsk() > 0
                && tick.getAsk() >= tick.getBid()
                && tick.getVolume() >= 0
        ).name("filter-invalid-ticks");

        // DEBUG
        cleanStream.map(t -> "TICK: " + t.getSymbol() + " ts=" + t.getTimestamp() + " bid=" + t.getBid()).print();

        // --- Finestra 5 minuti: tick -> 5-min log-return ---
        DataStream<FiveMinReturn> fiveMinReturns = cleanStream
                .keyBy(tick -> tick.getSymbol().toString())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ProcessWindowFunction<Tick, FiveMinReturn, String, TimeWindow>() {
                    @Override
                    public void process(String symbol, Context ctx,
                                        Iterable<Tick> ticks, Collector<FiveMinReturn> out) {
                        List<Tick> sorted = new ArrayList<>();
                        ticks.forEach(sorted::add);
                        sorted.sort(Comparator.comparingLong(Tick::getTimestamp));

                        if (sorted.size() < 2) return;

                        double firstBid = sorted.get(0).getBid();
                        double lastBid = sorted.get(sorted.size() - 1).getBid();
                        double logReturn = Math.log(lastBid / firstBid);

                        out.collect(new FiveMinReturn(
                                symbol, ctx.window().getEnd(), logReturn));
                    }
                }).name("5min-log-return");

        // --- Finestra giornaliera: 5-min returns -> RV daily ---
        // t0: timbriamo il wall-clock di emissione della finestra
        DataStream<DailyRV> dailyRV = fiveMinReturns
                .keyBy(FiveMinReturn::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new ProcessWindowFunction<FiveMinReturn, DailyRV, String, TimeWindow>() {
                    @Override
                    public void process(String symbol, Context ctx,
                                        Iterable<FiveMinReturn> returns, Collector<DailyRV> out) {
                        double sumSquared = 0.0;
                        int count = 0;
                        for (FiveMinReturn r : returns) {
                            sumSquared += r.getLogReturn() * r.getLogReturn();
                            count++;
                        }
                        if (count == 0) return;

                        out.collect(new DailyRV(
                                symbol, ctx.window().getEnd(), sumSquared,
                                System.currentTimeMillis()));  // t0: window emit wall-clock
                    }
                }).name("daily-rv");

        // --- Buffer 22 giorni + calcolo RV weekly/monthly + invio ML ---
        DataStream<Prediction> predictions = dailyRV
                .keyBy(DailyRV::getSymbol)
                .process(new KeyedProcessFunction<String, DailyRV, Prediction>() {

                    private transient ListState<DailyRV> buffer;
                    private transient ValueState<Boolean> warmupSent;

                    @Override
                    public void open(Configuration params) {
                        buffer = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("rv-buffer",
                                        TypeInformation.of(DailyRV.class)));
                        warmupSent = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("warmup-sent", Boolean.class));
                    }

                    @Override
                    public void processElement(DailyRV rv, Context ctx,
                                               Collector<Prediction> out) throws Exception {
                        // t1: buffer-and-predict riceve il record
                        long t1PredictReceive = System.currentTimeMillis();

                        buffer.add(rv);

                        List<DailyRV> history = new ArrayList<>();
                        buffer.get().forEach(history::add);
                        history.sort(Comparator.comparingLong(DailyRV::getTimestamp));

                        // mantieni solo gli ultimi 22 giorni
                        if (history.size() > BUFFER_SIZE) {
                            history = history.subList(
                                    history.size() - BUFFER_SIZE, history.size());
                            buffer.update(history);
                        }

                        if (history.size() < BUFFER_SIZE) return; // servono 22 giorni per RV monthly

                        double rvDaily = history.get(history.size() - 1).getRvDaily();

                        // RV weekly: media ultimi 5 giorni
                        double rvWeekly = history.subList(
                                history.size() - 5, history.size())
                                .stream()
                                .mapToDouble(DailyRV::getRvDaily)
                                .average().orElse(0);

                        // RV monthly: media ultimi 22 giorni (o quanti disponibili)
                        double rvMonthly = history.stream()
                                .mapToDouble(DailyRV::getRvDaily)
                                .average().orElse(0);

                        // Salta se valori non validi
                        if (Double.isNaN(rvDaily) || Double.isInfinite(rvDaily)) return;

                        // Invio al server ML
                        boolean isFirstCall = warmupSent.value() == null || !warmupSent.value();
                        String json;
                        if (isFirstCall) {
                            // Warmup: manda tutta la storia di 22 giorni
                            StringBuilder historyJson = new StringBuilder("[");
                            for (int i = 0; i < history.size(); i++) {
                                DailyRV h = history.get(i);
                                // Calcola rv_weekly e rv_monthly per ogni giorno della storia
                                double hRvD = h.getRvDaily();
                                double hRvW = history.subList(Math.max(0, i + 1 - 5), i + 1)
                                        .stream().mapToDouble(DailyRV::getRvDaily).average().orElse(0);
                                double hRvM = history.subList(0, i + 1)
                                        .stream().mapToDouble(DailyRV::getRvDaily).average().orElse(0);
                                if (i > 0) historyJson.append(",");
                                historyJson.append(String.format(java.util.Locale.US,
                                        "[%.10f,%.10f,%.10f]", hRvD, hRvW, hRvM));
                            }
                            historyJson.append("]");
                            json = String.format(java.util.Locale.US,
                                    "{\"symbol\":\"%s\",\"rv_daily\":%.10f,\"rv_weekly\":%.10f,\"rv_monthly\":%.10f,\"history\":%s}",
                                    ctx.getCurrentKey(), rvDaily, rvWeekly, rvMonthly, historyJson);
                            warmupSent.update(true);
                        } else {
                            json = String.format(java.util.Locale.US,
                                    "{\"symbol\":\"%s\",\"rv_daily\":%.10f,\"rv_weekly\":%.10f,\"rv_monthly\":%.10f}",
                                    ctx.getCurrentKey(), rvDaily, rvWeekly, rvMonthly);
                        }

                        URL url = new URL(ML_SERVER_URL);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/json");
                        conn.setDoOutput(true);

                        try (OutputStream os = conn.getOutputStream()) {
                            os.write(json.getBytes(StandardCharsets.UTF_8));
                        }

                        int code = conn.getResponseCode();
                        if (code != 200) {
                            BufferedReader err = new BufferedReader(
                                    new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
                            StringBuilder errMsg = new StringBuilder();
                            String l;
                            while ((l = err.readLine()) != null) errMsg.append(l);
                            err.close();
                            conn.disconnect();
                            return;
                        }

                        StringBuilder sb = new StringBuilder();
                        try (BufferedReader br = new BufferedReader(
                                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                            String line;
                            while ((line = br.readLine()) != null) sb.append(line);
                        }
                        conn.disconnect();

                        // Parse JSON response
                        JsonObject resp = JsonParser.parseString(sb.toString()).getAsJsonObject();
                        Double mambaPred = resp.get("mamba").isJsonNull() ? null : resp.get("mamba").getAsDouble();
                        Double lstmPred = resp.get("lstm").isJsonNull() ? null : resp.get("lstm").getAsDouble();
                        double harPred = resp.get("har").getAsDouble();

                        Prediction pred = new Prediction(
                                ctx.getCurrentKey(), rv.getTimestamp(),
                                rvDaily, rvWeekly, rvMonthly,
                                mambaPred, lstmPred, harPred);

                        // Propaga i timestamp di latenza
                        pred.setWindowEmitWallClock(rv.getWindowEmitWallClock());
                        pred.setPredictCompleteWallClock(t1PredictReceive);

                        out.collect(pred);
                    }
                }).name("buffer-and-predict");

        predictions.print();

        // --- JDBC Sink instrumentato: salva predictions + misura latenza ---
        LatencyTracker latencyTracker = new LatencyTracker();
        predictions.addSink(new InstrumentedJdbcSink(
                JDBC_URL, JDBC_USER, JDBC_PASSWORD, latencyTracker
        )).name("jdbc-sink");

        // --- Thread di monitoring in background ---
        // 1 min warmup, poi snapshot periodici ogni 30s.
        // Alla fine usa il penultimo snapshot (esclude l'ultimo minuto).
        JvmMetricsCollector jvm = new JvmMetricsCollector();
        // Array di 1 elemento per condividere gli snapshot col thread (effectively final)
        final JvmMetricsCollector.MetricsSnapshot[] snapshots = new JvmMetricsCollector.MetricsSnapshot[2];
        // [0] = start snapshot, [1] = penultimo snapshot (fine misura escluso ultimo minuto)

        Thread monitorThread = new Thread(() -> {
            try {
                BottleneckDetector detector = new BottleneckDetector();

                // Cgroup check immediato (precondizione di validità)
                JvmMetricsCollector.CgroupThrottleInfo cgroup = jvm.checkCgroupThrottling();
                if (cgroup != null) {
                    cgroup.printReport(System.out);
                }

                // Warmup: aspetta 1 minuto
                Thread.sleep(60_000);

                // Snapshot iniziale (dopo warmup)
                snapshots[0] = jvm.takeSnapshot(0);

                // Snapshot periodici ogni 30s
                JvmMetricsCollector.MetricsSnapshot prev = snapshots[0];

                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(30_000);

                    snapshots[1] = prev;
                    prev = jvm.takeSnapshot(latencyTracker.getTotalRecorded());

                    // Bottleneck check periodico
                    try {
                        List<String> jobIds = detector.getRunningJobIds();
                        if (!jobIds.isEmpty()) {
                            detector.detect(jobIds.get(0)).printReport(System.out);
                        }
                    } catch (Exception e) {
                        // Job non ancora raggiungibile via REST, riprova al prossimo ciclo
                    }
                }
            } catch (InterruptedException e) {
                // Job finito, il thread viene interrotto
            }
        }, "metrics-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();

        try {
            env.execute("TickVolatilityJob");
        } finally {
            monitorThread.interrupt();
            monitorThread.join(5000);

            // Report JVM finale (start → penultimo snapshot, esclude ultimo minuto)
            if (snapshots[0] != null && snapshots[1] != null) {
                jvm.computeReport(snapshots[0], snapshots[1]).printReport(System.out);
            }
        }
    }
}
