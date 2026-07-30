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
    created_at  TIMESTAMP DEFAULT NOW()
);
