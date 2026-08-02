package Service;

import domain.DailyRV;
import domain.FiveMinReturn;
import domain.LogRetAcc;
import domain.LogRetAgg;
import domain.MlRequest;
import domain.Prediction;
import domain.RvAcc;
import domain.RvAgg;
import domain.Tick;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FlinkJob {

    private static final String TOPIC = "ticks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "flink-tick-consumer";
    private static final String ML_SERVER_URL = "http://localhost:8080/predict";
    private static final int BUFFER_SIZE = 22; // giorni di storia per RV monthly

    // --- Split train/test sull'asse temporale ---
    private static final String TEST_START_DATE = System.getProperty("testStart", "");
    private static final long TEST_START_MILLIS =
            TEST_START_DATE.isEmpty()
                    ? Long.MIN_VALUE
                    : LocalDate.parse(TEST_START_DATE)
                               .atStartOfDay(ZoneOffset.UTC)
                               .toInstant()
                               .toEpochMilli();

    // JDBC
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5433/volatility";
    private static final String JDBC_USER = "flink";
    private static final String JDBC_PASSWORD = "flink";

    // --- Configurazione sperimentale ---
    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream is = FlinkJob.class.getClassLoader()
                .getResourceAsStream("pipeline.properties")) {
            if (is != null) props.load(is);
        } catch (Exception e) {
            System.out.println("[config] pipeline.properties non trovato, uso default");
        }
        return props;
    }

    private static int intConf(Properties p, String key, int def) {
        return Integer.parseInt(p.getProperty(key, String.valueOf(def)));
    }

    private static long longConf(Properties p, String key, long def) {
        return Long.parseLong(p.getProperty(key, String.valueOf(def)));
    }

    private static boolean boolConf(Properties p, String key, boolean def) {
        return Boolean.parseBoolean(p.getProperty(key, String.valueOf(def)));
    }

    private final int parallelismOverride;
    private final int kafkaPartitionsOverride;

    public FlinkJob() {
        this(-1, -1);
    }

    public FlinkJob(int parallelismOverride) {
        this(parallelismOverride, -1);
    }

    public FlinkJob(int parallelismOverride, int kafkaPartitionsOverride) {
        this.parallelismOverride = parallelismOverride;
        this.kafkaPartitionsOverride = kafkaPartitionsOverride;
    }

    public void start() throws Exception {

        Properties conf = loadConfig();
        int parallelism = parallelismOverride > 0
                ? parallelismOverride
                : intConf(conf, "flink.parallelism", 12);
        long checkpointMs     = longConf(conf, "checkpoint.interval.ms", 60000);
        boolean mlEnabled     = boolConf(conf, "ml.enabled", true);
        int mlAsyncCapacity   = intConf(conf, "ml.async.capacity", 20);
        int mlAsyncTimeoutS   = intConf(conf, "ml.async.timeout.s", 30);
        int sinkParallelism   = intConf(conf, "sink.jdbc.parallelism", 4);

        System.out.printf("[config] parallelism=%d  checkpoint=%dms  ml.enabled=%s  " +
                "ml.async.capacity=%d  sink.parallelism=%d%n",
                parallelism, checkpointMs, mlEnabled, mlAsyncCapacity, sinkParallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(checkpointMs);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointMs / 2);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointMs * 2);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(parallelism);

        if (TEST_START_DATE.isEmpty()) {
            System.out.println("[split] nessun gate di warmup: inferenze su tutti i giorni ingeriti");
        } else {
            System.out.println("[split] inferenze emesse dal " + TEST_START_DATE
                    + " in poi; i giorni precedenti riempiono il buffer senza inferire");
        }

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

        // --- Filtro tick invalidi + contatore per benchmark ---
        DataStream<Tick> cleanStream = tickStream.filter(new RichFilterFunction<Tick>() {
            private LongCounter tickCounter = new LongCounter();

            @Override
            public void open(Configuration parameters) {
                getRuntimeContext().addAccumulator("tick-counter", tickCounter);
            }

            @Override
            public boolean filter(Tick tick) {
                tickCounter.add(1L);
                return tick.getTimestamp() > 0
                        && tick.getBid() > 0
                        && tick.getAsk() > 0
                        && tick.getAsk() >= tick.getBid()
                        && tick.getVolume() >= 0;
            }
        }).name("filter-invalid-ticks");

        // --- Finestra 5 minuti: tick -> 5-min log-return ---
        // Stato costante (40 byte) invece del buffer di tutti i tick della finestra:
        // servono solo gli estremi, per l'identita' telescopica dei log-return.
        DataStream<FiveMinReturn> fiveMinReturns = cleanStream
                .keyBy(tick -> tick.getSymbol().toString())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new LogRetAgg(),
                        new ProcessWindowFunction<LogRetAcc, FiveMinReturn, String, TimeWindow>() {
                            @Override
                            public void process(String symbol, Context ctx,
                                                Iterable<LogRetAcc> accs,
                                                Collector<FiveMinReturn> out) {
                                // l'Iterable contiene un solo elemento: il risultato dell'accumulatore
                                LogRetAcc acc = accs.iterator().next();
                                if (acc.count < 2) return;

                                out.collect(new FiveMinReturn(
                                        symbol, ctx.window().getEnd(),
                                        Math.log(acc.bidAtMax / acc.bidAtMin)));
                            }
                        }).name("5min-log-return");

        // --- Finestra giornaliera: 5-min returns -> RV daily ---
        // Somma corrente (12 byte) invece del buffer dei ~96 return del giorno:
        // ogni return contribuisce a sumSquared e viene dimenticato.
        DataStream<DailyRV> dailyRV = fiveMinReturns
                .keyBy(FiveMinReturn::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new RvAgg(),
                        new ProcessWindowFunction<RvAcc, DailyRV, String, TimeWindow>() {
                            @Override
                            public void process(String symbol, Context ctx,
                                                Iterable<RvAcc> accs,
                                                Collector<DailyRV> out) {
                                RvAcc acc = accs.iterator().next();
                                if (acc.count == 0) return;

                                out.collect(new DailyRV(
                                        symbol, ctx.window().getEnd(), acc.sumSquared));
                            }
                        }).name("daily-rv");

        // --- Buffer 22 giorni + calcolo RV weekly/monthly (niente I/O) ---
        DataStream<MlRequest> mlRequests = dailyRV
                .keyBy(DailyRV::getSymbol)
                .process(new KeyedProcessFunction<String, DailyRV, MlRequest>() {

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
                                               Collector<MlRequest> out) throws Exception {
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

                        if (history.size() < BUFFER_SIZE) return;

                        // --- Gate di warmup ---
                        // Nei giorni precedenti al test il buffer si riempie ma NON si
                        // chiama il modello: nessuna richiesta HTTP, nessuna predizione.
                        // rv.getTimestamp() e' la FINE della finestra giornaliera, cioe'
                        // la mezzanotte del giorno successivo: serve '<=' per escludere
                        // il giorno che termina esattamente all'inizio del test.
                        if (rv.getTimestamp() <= TEST_START_MILLIS) return;

                        double rvDaily = history.get(history.size() - 1).getRvDaily();

                        double rvWeekly = history.subList(
                                history.size() - 5, history.size())
                                .stream()
                                .mapToDouble(DailyRV::getRvDaily)
                                .average().orElse(0);

                        double rvMonthly = history.stream()
                                .mapToDouble(DailyRV::getRvDaily)
                                .average().orElse(0);

                        if (Double.isNaN(rvDaily) || Double.isInfinite(rvDaily)) return;

                        // Costruisci il JSON da mandare al ML server
                        boolean isFirstCall = warmupSent.value() == null || !warmupSent.value();
                        String json;
                        if (isFirstCall) {
                            StringBuilder historyJson = new StringBuilder("[");
                            for (int i = 0; i < history.size(); i++) {
                                DailyRV h = history.get(i);
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

                        out.collect(new MlRequest(
                                ctx.getCurrentKey(), rv.getTimestamp(),
                                rvDaily, rvWeekly, rvMonthly, json));
                    }
                }).name("buffer-and-compute");

        // --- Predizioni: ML server asincrono oppure bypass ---
        DataStream<Prediction> predictions;

        if (mlEnabled) {
            predictions = AsyncDataStream.unorderedWait(
                    mlRequests,
                    new AsyncFunction<MlRequest, Prediction>() {
                        @Override
                        public void asyncInvoke(MlRequest req, ResultFuture<Prediction> resultFuture) {
                            CompletableFuture.supplyAsync(() -> {
                                try {
                                    URL url = new URL(ML_SERVER_URL);
                                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                                    conn.setRequestMethod("POST");
                                    conn.setRequestProperty("Content-Type", "application/json");
                                    conn.setDoOutput(true);

                                    try (OutputStream os = conn.getOutputStream()) {
                                        os.write(req.getJsonPayload().getBytes(StandardCharsets.UTF_8));
                                    }

                                    int code = conn.getResponseCode();
                                    if (code != 200) {
                                        conn.disconnect();
                                        return null;
                                    }

                                    StringBuilder sb = new StringBuilder();
                                    try (BufferedReader br = new BufferedReader(
                                            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                                        String line;
                                        while ((line = br.readLine()) != null) sb.append(line);
                                    }
                                    conn.disconnect();

                                    JsonObject resp = JsonParser.parseString(sb.toString()).getAsJsonObject();
                                    Double mambaPred = resp.get("mamba").isJsonNull() ? null : resp.get("mamba").getAsDouble();
                                    Double lstmPred = resp.get("lstm").isJsonNull() ? null : resp.get("lstm").getAsDouble();
                                    double harPred = resp.get("har").getAsDouble();

                                    return new Prediction(
                                            req.getSymbol(), req.getTimestamp(),
                                            req.getRvDaily(), req.getRvWeekly(), req.getRvMonthly(),
                                            mambaPred, lstmPred, harPred);
                                } catch (Exception e) {
                                    return null;
                                }
                            }).thenAccept(prediction -> {
                                if (prediction != null) {
                                    resultFuture.complete(Collections.singleton(prediction));
                                } else {
                                    resultFuture.complete(Collections.emptyList());
                                }
                            });
                        }
                    },
                    mlAsyncTimeoutS, TimeUnit.SECONDS,
                    mlAsyncCapacity
            ).name("async-ml-predict");
        } else {
            // ML disabilitato: predizione solo con i dati locali (no I/O)
            predictions = mlRequests.map(req -> new Prediction(
                    req.getSymbol(), req.getTimestamp(),
                    req.getRvDaily(), req.getRvWeekly(), req.getRvMonthly(),
                    null, null, 0.0
            )).name("ml-bypass");
        }

        // --- JDBC Sink ---
        predictions.addSink(new RichSinkFunction<Prediction>() {
            private transient Connection connection;
            private transient PreparedStatement statement;

            private static final String INSERT_SQL =
                    "INSERT INTO predictions (symbol, ts, rv_daily, rv_weekly, rv_monthly, mamba_pred, lstm_pred, har_pred) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
                connection.setAutoCommit(true);
                statement = connection.prepareStatement(INSERT_SQL);
            }

            @Override
            public void invoke(Prediction p, Context context) throws Exception {
                statement.setString(1, p.getSymbol());
                statement.setLong(2, p.getTimestamp());
                statement.setDouble(3, p.getRvDaily());
                statement.setDouble(4, p.getRvWeekly());
                statement.setDouble(5, p.getRvMonthly());
                if (p.getMambaPred() != null) {
                    statement.setDouble(6, p.getMambaPred());
                } else {
                    statement.setNull(6, java.sql.Types.DOUBLE);
                }
                if (p.getLstmPred() != null) {
                    statement.setDouble(7, p.getLstmPred());
                } else {
                    statement.setNull(7, java.sql.Types.DOUBLE);
                }
                statement.setDouble(8, p.getHarPred());
                statement.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            }
        }).name("jdbc-sink").setParallelism(sinkParallelism);

        JobExecutionResult result = env.execute("TickVolatilityJob");

        // --- Benchmark: scrivi risultato su CSV ---
        long totalTicks = result.getAccumulatorResult("tick-counter");
        long elapsedMs = result.getNetRuntime(TimeUnit.MILLISECONDS);
        int kafkaPartitions = kafkaPartitionsOverride > 0
                ? kafkaPartitionsOverride
                : intConf(conf, "kafka.partitions", 16);

        Path csvPath = Paths.get("src/main/java/results/benchmark.csv");
        new BenchmarkWriter(csvPath).write(
                parallelism, kafkaPartitions, checkpointMs,
                mlEnabled, totalTicks, elapsedMs);

        double throughput = elapsedMs > 0 ? totalTicks / (elapsedMs / 1000.0) : 0;
        System.out.printf("[benchmark] %,d tick in %,d ms -> %.2f tick/s  (parallelism=%d)%n",
                totalTicks, elapsedMs, throughput, parallelism);
    }

}
