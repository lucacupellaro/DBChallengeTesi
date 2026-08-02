package domain;

import java.io.Serializable;

public class Prediction implements Serializable {
    private String symbol;
    private long timestamp;
    private double rvDaily;
    private double rvWeekly;
    private double rvMonthly;
    private Double mambaPred;
    private Double lstmPred;
    private double harPred;

    public Prediction() {}

    public Prediction(String symbol, long timestamp,
                      double rvDaily, double rvWeekly, double rvMonthly,
                      Double mambaPred, Double lstmPred, double harPred) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
        this.rvWeekly = rvWeekly;
        this.rvMonthly = rvMonthly;
        this.mambaPred = mambaPred;
        this.lstmPred = lstmPred;
        this.harPred = harPred;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getRvDaily() { return rvDaily; }
    public double getRvWeekly() { return rvWeekly; }
    public double getRvMonthly() { return rvMonthly; }
    public Double getMambaPred() { return mambaPred; }
    public Double getLstmPred() { return lstmPred; }
    public double getHarPred() { return harPred; }

    @Override
    public String toString() {
        return "Prediction{" + symbol + ", t=" + timestamp +
               ", mamba=" + mambaPred + ", lstm=" + lstmPred + ", har=" + harPred + "}";
    }
}
