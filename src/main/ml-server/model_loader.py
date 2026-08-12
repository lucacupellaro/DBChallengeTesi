"""Caricamento dei modelli di volatilita' con i pesi addestrati.

Questo modulo istanzia i predictor (HARPredictor, VolPredictor) a partire
dai file in weights/ e li espone come singletoni pronti per l'inferenza.
Viene importato da server.py all'avvio.
"""
import logging
from pathlib import Path

from vol_models import HARPredictor, VolPredictor

logger = logging.getLogger("ml-server")

WEIGHTS_DIR = Path(__file__).parent / "weights"


def load_har() -> HARPredictor:
    path = WEIGHTS_DIR / "HAR.json"
    logger.info("Caricamento HAR da %s", path)
    return HARPredictor(str(path))


def load_deep(name: str, device: str = "cpu") -> VolPredictor | None:
    """Carica un modello deep (LSTM o Mamba). Ritorna None se non disponibile."""
    path = WEIGHTS_DIR / f"{name}.pt"
    if not path.exists():
        logger.warning("Pesi %s non trovati, modello disabilitato", path)
        return None
    try:
        pred = VolPredictor(str(path), device=device)
        logger.info("Caricato %s su %s", name, device)
        return pred
    except RuntimeError as e:
        logger.warning("Impossibile caricare %s: %s", name, e)
        return None


def load_all(device: str = "cpu"):
    """Carica tutti i modelli e li restituisce come dizionario.

    Returns:
        dict con chiavi 'har', 'lstm', 'mamba' (quest'ultimo puo' essere None).
    """
    har = load_har()
    lstm = load_deep("LSTM", device=device)
    mamba = load_deep("Mamba", device=device)
    return {"har": har, "lstm": lstm, "mamba": mamba}
