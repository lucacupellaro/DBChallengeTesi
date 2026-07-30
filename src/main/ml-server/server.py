from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import torch
import torch.nn as nn
import numpy as np
import uvicorn

try:
    from mamba_ssm import Mamba
    HAS_MAMBA = True
except ImportError:
    HAS_MAMBA = False

app = FastAPI()

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# ---------------------------------------------------------------------------
# Modelli
# ---------------------------------------------------------------------------

class MambaVol(nn.Module):
    def __init__(s, F=3, dm=32):
        super().__init__()
        s.proj = nn.Linear(F, dm)
        s.m = Mamba(d_model=dm, d_state=16, d_conv=4, expand=2)
        s.n = nn.LayerNorm(dm)
        s.h = nn.Linear(dm, 1)

    def forward(s, x):
        z = s.m(s.proj(x))
        return s.h(s.n(z)[:, -1]).squeeze(-1)


class LSTMVol(nn.Module):
    def __init__(s, F=3, h=32):
        super().__init__()
        s.lstm = nn.LSTM(F, h, batch_first=True)
        s.head = nn.Linear(h, 1)

    def forward(s, x):
        o, _ = s.lstm(x)
        return s.head(o[:, -1]).squeeze(-1)


class HARVol:
    """HAR: log-RV(t+1) = b0 + b1*daily + b2*weekly + b3*monthly."""
    def __init__(self):
        self.b0 = 0.1
        self.b1 = 0.4
        self.b2 = 0.3
        self.b3 = 0.2

    def predict(self, rv_daily: float, rv_weekly: float, rv_monthly: float) -> float:
        return self.b0 + self.b1 * rv_daily + self.b2 * rv_weekly + self.b3 * rv_monthly


# ---------------------------------------------------------------------------
# Caricamento modelli all'avvio
# ---------------------------------------------------------------------------

mamba_model = MambaVol(F=3, dm=32).to(DEVICE).eval() if HAS_MAMBA else None
lstm_model = LSTMVol(F=3, h=32).to(DEVICE).eval()
har_model = HARVol()

# TODO: caricare i pesi addestrati
# mamba_model.load_state_dict(torch.load("weights/mamba.pth", map_location=DEVICE))
# lstm_model.load_state_dict(torch.load("weights/lstm.pth", map_location=DEVICE))
# har_model.b0, har_model.b1, har_model.b2, har_model.b3 = <valori OLS>

# ---------------------------------------------------------------------------
# Buffer per serie storica (per Mamba e LSTM servono sequenze)
# ---------------------------------------------------------------------------

symbol_history: dict[str, list[list[float]]] = {}
SEQUENCE_LEN = 22  # giorni di storia da dare ai modelli sequenziali

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    symbol: str
    rv_daily: float
    rv_weekly: float
    rv_monthly: float
    history: Optional[list[list[float]]] = None  # warmup: 22 giorni di [rv_d, rv_w, rv_m]


@app.post("/predict")
def predict(req: PredictRequest):
    # HAR: predizione diretta dalle 3 feature correnti
    har_pred = har_model.predict(req.rv_daily, req.rv_weekly, req.rv_monthly)

    # Warmup: se arriva la storia completa, sostituisci il buffer
    if req.history is not None:
        symbol_history[req.symbol] = req.history[-SEQUENCE_LEN:]
    else:
        # Aggiorna con i nuovi valori giornalieri
        features = [req.rv_daily, req.rv_weekly, req.rv_monthly]
        if req.symbol not in symbol_history:
            symbol_history[req.symbol] = []
        symbol_history[req.symbol].append(features)
        if len(symbol_history[req.symbol]) > SEQUENCE_LEN:
            symbol_history[req.symbol] = symbol_history[req.symbol][-SEQUENCE_LEN:]

    # Mamba e LSTM: servono almeno SEQUENCE_LEN osservazioni
    mamba_pred = None
    lstm_pred = None
    history = symbol_history[req.symbol]
    if len(history) >= SEQUENCE_LEN:
        tensor = torch.tensor([history], dtype=torch.float32, device=DEVICE)  # [1, seq, 3]
        with torch.no_grad():
            if mamba_model is not None:
                mamba_pred = mamba_model(tensor).item()
            lstm_pred = lstm_model(tensor).item()

    return {
        "symbol": req.symbol,
        "mamba": mamba_pred,
        "lstm": lstm_pred,
        "har": har_pred,
        "device": str(DEVICE),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
