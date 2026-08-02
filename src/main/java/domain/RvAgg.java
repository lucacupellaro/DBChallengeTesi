package domain;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * AggregateFunction che, nella finestra tumbling giornaliera, riduce i
 * FiveMinReturn in un singolo RvAcc con la realized variance del giorno
 * (somma dei quadrati dei log-return). Stato costante (12 byte).
 */
public class RvAgg implements AggregateFunction<FiveMinReturn, RvAcc, RvAcc> {

    @Override
    public RvAcc createAccumulator() {
        return new RvAcc();
    }

    @Override
    public RvAcc add(FiveMinReturn r, RvAcc acc) {
        double x = r.getLogReturn();
        acc.sumSquared += x * x;
        acc.count++;
        return acc;
    }

    @Override
    public RvAcc getResult(RvAcc acc) {
        return acc;
    }

    @Override
    public RvAcc merge(RvAcc a, RvAcc b) {
        a.sumSquared += b.sumSquared;
        a.count      += b.count;
        return a;
    }
}
