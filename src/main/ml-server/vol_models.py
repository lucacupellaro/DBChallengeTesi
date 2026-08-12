"""Modelli di volatilita' esportati da AnalyzeTicker.ipynb (cella R3c).

CONTRATTO DI INFERENZA
----------------------
input  : storia dei log-RV giornalieri di UN asset, in ordine cronologico (il piu' recente
         per ultimo). Servono almeno MIN_HISTORY_HAR giorni per HAR e
         MIN_HISTORY_DEEP giorni per LSTM/Mamba.
         ATTENZIONE: MambaVol richiede CUDA (kernel senza fallback CPU); LSTMVol e HAR
         girano anche su CPU.
         lrv_t = log( RV del giorno t ), dove
         RV = sqrt( somma dei log-return a 5 minuti al quadrato dentro la giornata ),
         mid = (bid + ask) / 2,
         campionamento 5 min = ULTIMO mid della finestra (previous-tick sampling),
         i log-return NON attraversano il confine di giornata (nessun gap overnight).
output : RV prevista per il giorno SUCCESSIVO, in livello (predict) o in log (predict_log).

Tutti i modelli sono POOLED: gli stessi parametri valgono per ogni asset e l'asset NON e'
un input. Lo stato da mantenere per asset e' solo la sua storia di log-RV.

Per i modelli deep, mu / sd sono stati calcolati sul TRAIN del fold esportato e vanno usati
CONGELATI: ricalcolarli sullo stream introduce look-ahead.

HAR non richiede torch: bastano numpy e i 4 coefficienti in HAR.json.
"""
import json
from pathlib import Path

import numpy as np

try:
    import torch
    import torch.nn as nn
    _Base = nn.Module
except Exception:                  # senza torch resta utilizzabile HARPredictor
    torch = nn = None
    class _Base:                   # segnaposto: istanziare i modelli deep dara' errore, non l'import
        pass

try:
    from mamba_ssm import Mamba
except Exception:                  # senza mamba_ssm (es. server CPU-only) resta usabile LSTMVol
    Mamba = None


# ------------------------------- architetture deep -------------------------------------
class MambaVol(_Base):
    def __init__(s, F=3, dm=32):
        super().__init__()
        s.proj = nn.Linear(F, dm)
        s.m = Mamba(d_model=dm, d_state=16, d_conv=4, expand=2)
        s.n = nn.LayerNorm(dm)
        s.h = nn.Linear(dm, 1)

    def forward(s, x):
        z = s.m(s.proj(x))
        return s.h(s.n(z)[:, -1]).squeeze(-1)


class LSTMVol(_Base):
    def __init__(s, F=3, h=32):
        super().__init__()
        s.lstm = nn.LSTM(F, h, batch_first=True)
        s.head = nn.Linear(h, 1)

    def forward(s, x):
        o, _ = s.lstm(x)
        return s.head(o[:, -1]).squeeze(-1)


# --------------------------------- feature --------------------------------------------
_W = 5                              # finestra "settimana"
_M = 22                             # finestra "mese"
_L = 22                          # timestep della sequenza per i modelli deep

MIN_HISTORY_HAR  = _M               # HAR: serve solo l'ultima riga di feature
MIN_HISTORY_DEEP = _M + _L - 1      # deep: L timestep, ognuno con la media a 22 giorni


def _trail_mean(x, w):
    """media mobile trailing, equivalente a pandas .rolling(w).mean() (NaN all'inizio)."""
    c = np.cumsum(np.concatenate(([0.0], x)))
    out = np.full(len(x), np.nan)
    out[w - 1:] = (c[w:] - c[:-w]) / w
    return out


def har_features(lrv_hist):
    """storia di log-RV -> [lrv_oggi, media_5gg, media_22gg] (le 3 componenti HAR)."""
    x = np.asarray(lrv_hist, dtype=np.float64).ravel()
    if len(x) < MIN_HISTORY_HAR:
        raise ValueError(f"HAR: servono almeno {MIN_HISTORY_HAR} giorni di log-RV, ricevuti {len(x)}")
    f = np.array([x[-1], x[-_W:].mean(), x[-_M:].mean()])
    if not np.isfinite(f).all():
        raise ValueError("feature HAR con NaN: buchi nei dati")
    return f


def build_sequence(lrv_hist):
    """storia di log-RV -> matrice (L, 3): [lrv, media_5gg, media_22gg] per timestep."""
    x = np.asarray(lrv_hist, dtype=np.float64).ravel()
    if len(x) < MIN_HISTORY_DEEP:
        raise ValueError(f"deep: servono almeno {MIN_HISTORY_DEEP} giorni di log-RV, ricevuti {len(x)}")
    seq = np.stack([x, _trail_mean(x, _W), _trail_mean(x, _M)], axis=-1)[-_L:]
    if not np.isfinite(seq).all():
        raise ValueError("sequenza con NaN: storia insufficiente o buchi nei dati")
    return seq.astype(np.float32)


# -------------------------------- predittori ------------------------------------------
class HARPredictor:
    """HAR-RV: 4 coefficienti stimati per OLS. Solo numpy, torch non serve.

        p = HARPredictor("export_vol/HAR.json")
        p.predict(lrv_ultimi_22_giorni)       # -> RV prevista, in livello
    """

    def __init__(self, path):
        c = json.loads(Path(path).read_text(encoding="utf-8"))
        self.const = float(c["const"])
        self.beta = np.array([c["beta_d"], c["beta_w"], c["beta_m"]], dtype=np.float64)
        self.meta = c.get("meta", {})

    def predict_log(self, lrv_hist):
        """log-RV previsto per il giorno successivo."""
        return float(self.const + har_features(lrv_hist) @ self.beta)

    def predict(self, lrv_hist):
        """RV prevista per il giorno successivo, in LIVELLO."""
        return float(np.exp(self.predict_log(lrv_hist)))


class VolPredictor:
    """Modello deep: carica un artefatto .pt e prevede la RV del giorno successivo.

        p = VolPredictor("export_vol/LSTM.pt")
        p.predict(lrv_ultimi_43_giorni)       # -> RV prevista, in livello
    """

    def __init__(self, path, device="cpu"):
        if torch is None:
            raise RuntimeError("torch non disponibile: per i modelli deep serve PyTorch. "
                               "Senza torch usa HARPredictor (HAR.json).")
        ck = torch.load(path, map_location=device)
        name = ck["class_name"]
        if name == "MambaVol":
            if Mamba is None:
                raise RuntimeError("mamba_ssm non installato: MambaVol non e' utilizzabile. "
                                   "Usa LSTM.pt o HAR.json.")
            if not str(device).startswith("cuda"):
                # i kernel di mamba_ssm / causal_conv1d NON hanno implementazione CPU:
                # senza questo controllo si ottiene "Expected x.is_cuda() to be true" al forward.
                raise RuntimeError("MambaVol richiede CUDA: i kernel di mamba_ssm non girano su CPU. "
                                   "Usa device='cuda', oppure LSTM.pt / HAR.json.")
        self.net = globals()[name](**ck.get("init_args", {})).to(device).eval()
        self.net.load_state_dict(ck["state_dict"])      # strict=True: fallisce se l'architettura differisce
        self.mu = ck["mu"].cpu().numpy().astype(np.float32)   # .cpu(): con map_location='cuda'
        self.sd = ck["sd"].cpu().numpy().astype(np.float32)   #         sarebbero tensori GPU
        self.device = device
        self.meta = ck["meta"]

    def predict_log(self, lrv_hist):
        """log-RV previsto per il giorno successivo."""
        seq = (build_sequence(lrv_hist) - self.mu) / self.sd
        with torch.no_grad():
            t = torch.tensor(seq[None], dtype=torch.float32, device=self.device)
            return float(self.net(t).reshape(-1)[0].item())

    def predict(self, lrv_hist):
        """RV prevista per il giorno successivo, in LIVELLO."""
        return float(np.exp(self.predict_log(lrv_hist)))
