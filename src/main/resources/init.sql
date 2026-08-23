CREATE TABLE IF NOT EXISTS predictions (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    ts          BIGINT       NOT NULL,
    rv_daily    DOUBLE PRECISION NOT NULL,
    rv_weekly   DOUBLE PRECISION NOT NULL,
    rv_monthly  DOUBLE PRECISION NOT NULL,
    mamba_pred  DOUBLE PRECISION,
    lstm_pred   DOUBLE PRECISION,
    har_pred    DOUBLE PRECISION NOT NULL,
    w_har       DOUBLE PRECISION NOT NULL,
    w_lstm      DOUBLE PRECISION,
    w_mamba     DOUBLE PRECISION,
    s_adj_har   DOUBLE PRECISION NOT NULL,
    s_adj_lstm  DOUBLE PRECISION,
    s_adj_mamba DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Migrazione: aggiunge le colonne di peso se la tabella esiste gia'
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS w_har       DOUBLE PRECISION;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS w_lstm      DOUBLE PRECISION;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS w_mamba     DOUBLE PRECISION;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS s_adj_har   DOUBLE PRECISION;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS s_adj_lstm  DOUBLE PRECISION;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS s_adj_mamba DOUBLE PRECISION;

-- Chiave logica di una predizione. La topologia emette una sola predizione per
-- (symbol, giorno) — finestra giornaliera con keyBy(symbol) — quindi la coppia
-- identifica il record; id e created_at sono metadati tecnici.
-- Serve al sink idempotente (ON CONFLICT ... DO NOTHING): al riavvio da checkpoint
-- Flink rilegge da Kafka e riscrive le predizioni gia' emesse, e senza questo
-- vincolo finirebbero duplicate. Vedi FlinkJob INSERT_SQL.
-- Su un database gia' popolato va prima rimossa l'eventuale duplicazione:
--   DELETE FROM predictions p USING predictions q
--    WHERE p.symbol = q.symbol AND p.ts = q.ts AND p.id > q.id;
ALTER TABLE predictions DROP CONSTRAINT IF EXISTS predictions_symbol_ts_key;
ALTER TABLE predictions ADD  CONSTRAINT predictions_symbol_ts_key UNIQUE (symbol, ts);
