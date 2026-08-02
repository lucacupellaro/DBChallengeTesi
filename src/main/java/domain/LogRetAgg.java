package domain;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * AggregateFunction che, nella finestra tumbling a 5 minuti, riduce uno stream
 * di Tick a un singolo LogRetAcc contenente gli estremi temporali necessari
 * per il calcolo del log-return. Stato costante (40 byte) indipendentemente
 * dal numero di tick nella finestra.
 */
public class LogRetAgg implements AggregateFunction<Tick, LogRetAcc, LogRetAcc> {

    @Override
    public LogRetAcc createAccumulator() {
        return new LogRetAcc();
    }

    @Override
    public LogRetAcc add(Tick t, LogRetAcc acc) {
        long ts = t.getTimestamp();
        if (ts <  acc.tsMin) { acc.tsMin = ts; acc.bidAtMin = t.getBid(); }
        if (ts >= acc.tsMax) { acc.tsMax = ts; acc.bidAtMax = t.getBid(); }
        acc.count++;
        return acc;
    }

    @Override
    public LogRetAcc getResult(LogRetAcc acc) {
        return acc;
    }

    @Override
    public LogRetAcc merge(LogRetAcc a, LogRetAcc b) {
        if (b.tsMin <  a.tsMin) { a.tsMin = b.tsMin; a.bidAtMin = b.bidAtMin; }
        if (b.tsMax >= a.tsMax) { a.tsMax = b.tsMax; a.bidAtMax = b.bidAtMax; }
        a.count += b.count;
        return a;
    }
}
