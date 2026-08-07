package Service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Job separato che legge dal topic "ml-failed" (DLQ) e rimanda le richieste
 * al ML server quando è tornato operativo.
 * Processa una richiesta alla volta con pausa tra una e l'altra,
 * così il traffico normale dal Job principale ha sempre priorità.
 */
public class DlqReprocessor {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DLQ_TOPIC = "ml-failed";
    private static final String ML_SERVER_URL = "http://localhost:8080/predict";
    private static final String GROUP_ID = "dlq-reprocessor";
    private static final long PAUSE_BETWEEN_REQUESTS_MS = 500;

    // JDBC
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5433/volatility";
    private static final String JDBC_USER = "flink";
    private static final String JDBC_PASSWORD = "flink";

    private static final String INSERT_SQL =
            "INSERT INTO predictions (symbol, ts, rv_daily, rv_weekly, rv_monthly, mamba_pred, lstm_pred, har_pred) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private volatile boolean running = true;

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {

            consumer.subscribe(Collections.singletonList(DLQ_TOPIC));
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement(INSERT_SQL);

            System.out.println("[dlq-reprocessor] In attesa di messaggi dal topic " + DLQ_TOPIC);

            // Prima verifica che il ML server sia raggiungibile
            waitForMlServer();

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord<String, String> record : records) {
                    String symbol = record.key();
                    String jsonPayload = record.value();

                    System.out.printf("[dlq-reprocessor] Riprocesso richiesta per %s%n", symbol);

                    boolean success = sendToMlServer(jsonPayload, symbol, stmt);

                    if (success) {
                        System.out.printf("[dlq-reprocessor] OK per %s%n", symbol);
                    } else {
                        // ML server di nuovo giù: aspetta che torni
                        System.err.printf("[dlq-reprocessor] ML server non risponde, attendo che torni su...%n");
                        waitForMlServer();
                        // Riprova questa richiesta
                        sendToMlServer(jsonPayload, symbol, stmt);
                    }

                    Thread.sleep(PAUSE_BETWEEN_REQUESTS_MS);
                }

                // Commit offset solo dopo aver processato tutto il batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
            System.err.printf("[dlq-reprocessor] Errore fatale: %s%n", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Manda la richiesta al ML server e salva la predizione in Postgres.
     * Ritorna true se ok, false se il server non risponde.
     */
    private boolean sendToMlServer(String jsonPayload, String symbol, PreparedStatement stmt) {
        try {
            URL url = new URL(ML_SERVER_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            if (code != 200) {
                conn.disconnect();
                return false;
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) sb.append(line);
            }
            conn.disconnect();

            // Parsing risposta ML
            JsonObject resp = JsonParser.parseString(sb.toString()).getAsJsonObject();
            Double mambaPred = resp.get("mamba").isJsonNull() ? null : resp.get("mamba").getAsDouble();
            Double lstmPred = resp.get("lstm").isJsonNull() ? null : resp.get("lstm").getAsDouble();
            double harPred = resp.get("har").getAsDouble();

            // Parsing dati originali dal payload per salvare in Postgres
            JsonObject req = JsonParser.parseString(jsonPayload).getAsJsonObject();
            double rvDaily = req.get("rv_daily").getAsDouble();
            double rvWeekly = req.get("rv_weekly").getAsDouble();
            double rvMonthly = req.get("rv_monthly").getAsDouble();

            // Salva in Postgres
            stmt.setString(1, symbol);
            stmt.setLong(2, System.currentTimeMillis()); // timestamp riprocessamento
            stmt.setDouble(3, rvDaily);
            stmt.setDouble(4, rvWeekly);
            stmt.setDouble(5, rvMonthly);
            if (mambaPred != null) {
                stmt.setDouble(6, mambaPred);
            } else {
                stmt.setNull(6, java.sql.Types.DOUBLE);
            }
            if (lstmPred != null) {
                stmt.setDouble(7, lstmPred);
            } else {
                stmt.setNull(7, java.sql.Types.DOUBLE);
            }
            stmt.setDouble(8, harPred);
            stmt.executeUpdate();

            return true;

        } catch (Exception e) {
            System.err.printf("[dlq-reprocessor] Errore invio per %s: %s%n", symbol, e.getMessage());
            return false;
        }
    }

    /**
     * Attende che il ML server sia raggiungibile facendo un health check
     * ogni 10 secondi.
     */
    private void waitForMlServer() {
        while (running) {
            try {
                URL url = new URL(ML_SERVER_URL.replace("/predict", "/health"));
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(3000);
                int code = conn.getResponseCode();
                conn.disconnect();
                if (code == 200) {
                    System.out.println("[dlq-reprocessor] ML server raggiungibile, riprendo il riprocessamento");
                    return;
                }
            } catch (Exception e) {
                // Server ancora giù
            }
            try {
                System.out.println("[dlq-reprocessor] ML server non raggiungibile, riprovo tra 10s...");
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void stop() {
        running = false;
    }

    public static void main(String[] args) {
        new DlqReprocessor().start();
    }
}
