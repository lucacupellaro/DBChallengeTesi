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
