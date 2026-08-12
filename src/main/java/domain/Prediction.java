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
    private double wHar;
    private Double wLstm;
    private Double wMamba;
    private double sAdjHar;
    private Double sAdjLstm;
    private Double sAdjMamba;

    public Prediction() {}

    public Prediction(String symbol, long timestamp,
                      double rvDaily, double rvWeekly, double rvMonthly,
                      Double mambaPred, Double lstmPred, double harPred,
                      double wHar, Double wLstm, Double wMamba,
                      double sAdjHar, Double sAdjLstm, Double sAdjMamba) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.rvDaily = rvDaily;
        this.rvWeekly = rvWeekly;
        this.rvMonthly = rvMonthly;
        this.mambaPred = mambaPred;
        this.lstmPred = lstmPred;
        this.harPred = harPred;
        this.wHar = wHar;
        this.wLstm = wLstm;
        this.wMamba = wMamba;
        this.sAdjHar = sAdjHar;
        this.sAdjLstm = sAdjLstm;
        this.sAdjMamba = sAdjMamba;
    }

    public String getSymbol() { return symbol; }
    public long getTimestamp() { return timestamp; }
    public double getRvDaily() { return rvDaily; }
    public double getRvWeekly() { return rvWeekly; }
    public double getRvMonthly() { return rvMonthly; }
    public Double getMambaPred() { return mambaPred; }
    public Double getLstmPred() { return lstmPred; }
    public double getHarPred() { return harPred; }
    public double getWHar() { return wHar; }
    public Double getWLstm() { return wLstm; }
    public Double getWMamba() { return wMamba; }
    public double getSAdjHar() { return sAdjHar; }
    public Double getSAdjLstm() { return sAdjLstm; }
    public Double getSAdjMamba() { return sAdjMamba; }

    @Override
    public String toString() {
        return "Prediction{" + symbol + ", t=" + timestamp +
               ", mamba=" + mambaPred + ", lstm=" + lstmPred + ", har=" + harPred +
               ", wHar=" + wHar + ", wLstm=" + wLstm + ", wMamba=" + wMamba + "}";
    }
}
