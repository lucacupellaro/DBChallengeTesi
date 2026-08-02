package domain;

import java.io.Serializable;

/**
 * Accumulatore per la finestra giornaliera: mantiene la somma corrente dei
 * quadrati dei log-return a 5 minuti (realized variance). Ogni return viene
 * assorbito nel totale e dimenticato, senza conservare i ~96 valori del giorno.
 */
public class RvAcc implements Serializable {
    public double sumSquared = 0.0;
    public long   count      = 0;

    public RvAcc() {}
}
