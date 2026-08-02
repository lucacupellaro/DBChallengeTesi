package domain;

import java.io.Serializable;

/**
 * Accumulatore per la finestra a 5 minuti: conserva solo il primo e l'ultimo
 * tick (per timestamp) così da calcolare il log-return della finestra tramite
 * l'identità telescopica log(P_last / P_first), senza tenere in memoria tutti
 * i tick intermedi. Insensibile all'ordine di arrivo degli eventi.
 */
public class LogRetAcc implements Serializable {
    public long   tsMin    = Long.MAX_VALUE;
    public double bidAtMin = 0.0;
    public long   tsMax    = Long.MIN_VALUE;
    public double bidAtMax = 0.0;
    public long   count    = 0;

    public LogRetAcc() {}
}
