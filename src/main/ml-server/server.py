from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import numpy as np
import math
import logging
import uvicorn
import torch

from model_loader import load_all
from vol_models import MIN_HISTORY_HAR, MIN_HISTORY_DEEP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml-server")

app = FastAPI()

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# ---------------------------------------------------------------------------
# Caricamento modelli con pesi addestrati
# ---------------------------------------------------------------------------
models = load_all(device=DEVICE)
har_model = models["har"]
lstm_model = models["lstm"]
mamba_model = models["mamba"]

# ---------------------------------------------------------------------------
# Buffer: storia dei log-RV per asset (servono per costruire le feature)
# ---------------------------------------------------------------------------
symbol_history: dict[str, list[float]] = {}

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    symbol: str
    log_rv: Optional[float] = None                     # log-RV del giorno corrente (legacy)
    rv_daily: Optional[float] = None                   # alias usato dal FlinkJob
    rv_weekly: Optional[float] = None                  # ignorato (ricalcolato internamente)
    rv_monthly: Optional[float] = None                 # ignorato (ricalcolato internamente)
    history: Optional[list] = None                     # warmup: log-RV o triple [d,w,m]


@app.get("/health")
def health():
    return {"status": "ok"}


def _to_log(x, symbol):
    """rv_daily arriva in LIVELLO (volatilita' giornaliera, es. 0.0145); i modelli
    lavorano in log. Il campo log_rv invece e' gia' in log e passa inalterato."""
    x = float(x)
    if x <= 0 or math.isnan(x) or math.isinf(x):
        raise HTTPException(status_code=422,
                            detail=f"rv_daily non positivo o non finito per {symbol}: {x}")
    return math.log(x)


@app.post("/predict")
def predict(req: PredictRequest):
    # Due contratti: log_rv e' gia' in log, rv_daily e' in livello e va trasformato.
    # Trattare rv_daily come se fosse un log fa arrivare al modello feature ~0, e la
    # predizione collassa su exp(const) ~ 0,73 per ogni asset e ogni giorno.
    if req.log_rv is not None:
        log_rv = float(req.log_rv)
        if math.isnan(log_rv) or math.isinf(log_rv):
            logger.warning("log_rv non valido per %s: %.6f", req.symbol, log_rv)
            raise HTTPException(status_code=422, detail="log_rv contiene NaN o Inf")
    elif req.rv_daily is not None:
        log_rv = _to_log(req.rv_daily, req.symbol)
    else:
        raise HTTPException(status_code=422, detail="Serve log_rv o rv_daily")

    try:
        # Warmup: se arriva la storia completa, sostituisci il buffer
        # Il FlinkJob può mandare history come lista di triple [d,w,m]: prendi solo d
        if req.history is not None:
            # la storia arriva nelle stesse unita' del campo usato per il giorno corrente
            parsed = []
            for item in req.history:
                v = float(item[0]) if isinstance(item, (list, tuple)) else float(item)
                parsed.append(v if req.log_rv is not None else _to_log(v, req.symbol))
            symbol_history[req.symbol] = parsed
        else:
            if req.symbol not in symbol_history:
                symbol_history[req.symbol] = []
            symbol_history[req.symbol].append(log_rv)

        hist = symbol_history[req.symbol]

        # HAR: servono almeno MIN_HISTORY_HAR giorni
        har_pred = None
        if len(hist) >= MIN_HISTORY_HAR:
            har_pred = har_model.predict(hist)

        # LSTM: servono almeno MIN_HISTORY_DEEP giorni
        lstm_pred = None
        if lstm_model is not None and len(hist) >= MIN_HISTORY_DEEP:
            lstm_pred = lstm_model.predict(hist)

        # Mamba: servono almeno MIN_HISTORY_DEEP giorni + CUDA
        mamba_pred = None
        if mamba_model is not None and len(hist) >= MIN_HISTORY_DEEP:
            mamba_pred = mamba_model.predict(hist)

        return {
            "symbol": req.symbol,
            "har": har_pred,
            "lstm": lstm_pred,
            "mamba": mamba_pred,
            "device": DEVICE,
            "buffer_len": len(hist),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Errore durante l'inferenza per %s: %s", req.symbol, str(e))
        raise HTTPException(status_code=500, detail=f"Errore inferenza: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
