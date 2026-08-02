package domain;

import java.io.Serializable;

/**
 * Oggetto intermedio tra buffer-and-compute e async-ml-predict.
 * Contiene i dati RV calcolati e il JSON pronto da mandare al ML server.
 */
public class MlRequest implements Serializable {
    private String symbol;
    private long timestamp;
    private double rvDaily;
    private double rvWeekly;
    private double rvMonthly;
    private String jsonPayload;

    public MlRequest() {}

    public MlRequest(String symbol, long timestamp,
                     double rvDaily, double rvWeekly, double rvMonthly,
                     String jsonPayload) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
        this.rvWeekly = rvWeekly;
        this.rvMonthly = rvMonthly;
        this.jsonPayload = jsonPayload;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getRvDaily() { return rvDaily; }
    public double getRvWeekly() { return rvWeekly; }
    public double getRvMonthly() { return rvMonthly; }
    public String getJsonPayload() { return jsonPayload; }

    @Override
    public String toString() {
        return "MlRequest{" + symbol + ", t=" + timestamp + "}";
    }
}
