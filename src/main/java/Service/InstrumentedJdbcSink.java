package Service;

import domain.LatencyRecord;
import domain.Prediction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Custom JDBC sink that timestamps t2 (sink receive) and t3 (DB ack)
 * and records the latency in the LatencyTracker.
 *
 * Replaces JdbcSink.sink() to allow instrumentation.
 */
public class InstrumentedJdbcSink extends RichSinkFunction<Prediction> {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final LatencyTracker tracker;

    private transient Connection connection;
    private transient PreparedStatement statement;

    private static final String INSERT_SQL =
            "INSERT INTO predictions (symbol, ts, rv_daily, rv_weekly, rv_monthly, mamba_pred, lstm_pred, har_pred) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    public InstrumentedJdbcSink(String jdbcUrl, String user, String password, LatencyTracker tracker) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.tracker = tracker;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(true);
        statement = connection.prepareStatement(INSERT_SQL);
    }

    @Override
    public void invoke(Prediction p, Context context) throws Exception {
        // t2: sink riceve il record
        long t2SinkReceive = System.currentTimeMillis();

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

        // t3: DB ha confermato
        long t3DbAck = System.currentTimeMillis();

        // Registra la latenza
        if (p.getWindowEmitWallClock() > 0) {
            LatencyRecord lr = new LatencyRecord(p.getSymbol(), p.getWindowEmitWallClock());
            lr.setT1PredictReceive(p.getPredictCompleteWallClock());
            lr.setT2SinkReceive(t2SinkReceive);
            lr.setT3DbAck(t3DbAck);
            tracker.record(lr);
        }
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();

        // Stampa il report finale
        tracker.printReport(System.out);
        String dominant = tracker.dominantSegmentAtP99();
        if ("dbWrite".equals(dominant)) {
            System.out.println(">>> DB WRITE domina la latenza al p99.");
            System.out.println(">>> R_sust è determinato dal DB. Ottimizzazioni Flink invisibili.");
        } else if (dominant != null) {
            System.out.printf(">>> Segmento dominante al p99: %s%n", dominant);
        }
    }
}
