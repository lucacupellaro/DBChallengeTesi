package Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Assegnazione load-aware dei simboli alle partizioni Kafka.
 *
 * Il partitioner di default di Kafka usa murmur2(symbol) % numPartitions: distribuisce
 * uniformemente il NUMERO DI SIMBOLI, non il NUMERO DI TICK. Con 61 asset e un rapporto
 * 1850x fra il simbolo piu' attivo e il meno attivo, la partizione piu' carica finisce
 * per portare ~2,4x il carico medio, e lo stadio di ingestione impiega altrettanto in piu'
 * (il tempo lo detta l'ultima partizione che finisce).
 *
 * Qui l'assegnazione viene invece calcolata con LPT (Longest Processing Time first):
 * i simboli sono ordinati per carico previsto decrescente e assegnati via via alla
 * partizione meno carica. I "massi" vengono piazzati per primi, la "ghiaia" riempie i buchi.
 *
 * Il carico previsto per il giorno d e' la MEDIA DEI TICK DEGLI ULTIMI {@code window} GIORNI
 * (default 2). Solo dati passati: nessun look-ahead, coerente con il walk-forward dei modelli.
 * Una finestra corta batte le medie lunghe perche' i conteggi dei tick fanno clustering come
 * la volatilita': una media lunga insegue con ritardo i cambi di regime, e LPT — che impacca
 * le partizioni per essere uguali sui volumi PREVISTI — amplifica ogni errore di stima.
 *
 * Finche' non c'e' storia sufficiente {@link #planFor} restituisce null e il chiamante
 * ricade sul partitioner di default.
 */
public class PartitionPlanner {

    private final int numPartitions;
    private final int window;

    /** Conteggi tick per simbolo degli ultimi {@code window} giorni gia' ingeriti. */
    private final Deque<Map<String, Long>> history = new ArrayDeque<>();

    /** Conteggi del giorno in corso, accumulati mentre lo si pubblica. */
    private Map<String, Long> current = new HashMap<>();

    public PartitionPlanner(int numPartitions, int window) {
        if (numPartitions < 1) throw new IllegalArgumentException("numPartitions < 1");
        if (window < 1)        throw new IllegalArgumentException("window < 1");
        this.numPartitions = numPartitions;
        this.window = window;
    }

    /**
     * Mappa simbolo -> partizione da usare per il prossimo giorno, oppure null se la storia
     * non basta ancora (il chiamante ricade sull'hash di default).
     */
    public Map<String, Integer> planFor() {
        if (history.isEmpty()) return null;

        // carico previsto = media dei tick per simbolo sugli ultimi `window` giorni
        Map<String, Double> forecast = new HashMap<>();
        for (Map<String, Long> day : history) {
            day.forEach((sym, n) -> forecast.merge(sym, n.doubleValue(), Double::sum));
        }
        int n = history.size();
        forecast.replaceAll((sym, tot) -> tot / n);

        return lpt(forecast);
    }

    /** LPT: ordina per carico decrescente e assegna alla partizione meno carica. */
    private Map<String, Integer> lpt(Map<String, Double> forecast) {
        double[] load = new double[numPartitions];
        Map<String, Integer> plan = new HashMap<>();

        List<Map.Entry<String, Double>> sorted = forecast.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed()
                        .thenComparing(Map.Entry::getKey))   // tie-break deterministico
                .collect(Collectors.toList());

        for (Map.Entry<String, Double> e : sorted) {
            int p = 0;
            for (int i = 1; i < numPartitions; i++) if (load[i] < load[p]) p = i;
            plan.put(e.getKey(), p);
            load[p] += e.getValue();
        }
        return plan;
    }

    /** Registra un tick pubblicato del giorno in corso. */
    public void record(String symbol) {
        current.merge(symbol, 1L, Long::sum);
    }

    /** Chiude il giorno in corso e lo fa entrare nella finestra di storia. */
    public void endOfDay() {
        if (current.isEmpty()) return;
        history.addLast(current);
        while (history.size() > window) history.removeFirst();
        current = new HashMap<>();
    }

    /**
     * Sbilanciamento max/mean dei conteggi {@code counts} sotto il piano {@code plan}.
     * Serve solo a stampare la diagnostica di ingestione.
     */
    public static double imbalance(Map<String, Long> counts, Map<String, Integer> plan,
                                   int numPartitions) {
        long[] load = new long[numPartitions];
        long total = 0;
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            int p = plan == null ? 0 : plan.getOrDefault(e.getKey(), 0);
            load[p] += e.getValue();
            total += e.getValue();
        }
        if (total == 0) return 1.0;
        long max = 0;
        for (long l : load) max = Math.max(max, l);
        return max / ((double) total / numPartitions);
    }

    /** Sbilanciamento effettivo osservato, dati i conteggi per partizione. */
    public static double imbalance(long[] perPartition) {
        long total = 0, max = 0;
        for (long l : perPartition) { total += l; max = Math.max(max, l); }
        if (total == 0) return 1.0;
        return max / ((double) total / perPartition.length);
    }

    public int numPartitions() { return numPartitions; }

    /** Conteggi del giorno in corso (per la diagnostica di fine giornata). */
    public Map<String, Long> currentCounts() { return current; }
}
