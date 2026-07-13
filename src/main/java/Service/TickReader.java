package Service;

import domain.Tick;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Reader "lazy" di UN file CSV giornaliero: legge un tick alla volta
 * (streaming, memoria costante). Non carica mai tutto il file in RAM.
 *
 * Schema atteso per riga: DateTime, Bid, Ask, Volume, Spread, Symbol
 *
 * Uso tipico:
 *   try (TickReader r = new TickReader(csvPath)) {
 *       Tick t;
 *       while ((t = r.next()) != null) { ... }
 *   }
 */
public class TickReader implements Closeable {


    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");

    private final BufferedReader r;
    private boolean headerSkipped = false;

    public TickReader(Path csv) throws IOException {
        this.r = Files.newBufferedReader(csv);
    }

    /** Legge la riga successiva -> Tick. Ritorna null a fine file. */
    public Tick next() throws IOException {
        // salta l'header alla prima chiamata (se presente)
        if (!headerSkipped) {
            r.readLine();
            headerSkipped = true;
        }

        String line;
        // salta righe vuote o non parsabili (validazione), senza fermarsi
        while ((line = r.readLine()) != null) {
            Tick t = parse(line);
            if (t != null) return t;
        }
        return null; // fine file
    }

    //converte la riga del csv in un oggetto avro
    private static Tick parse(String line) {
        if (line.trim().isEmpty()) return null;
        String[] f = line.split(",");
        if (f.length < 6) return null; // riga malformata

        try {
            long   ts     = parseTimestamp(f[0]);
            double bid    = Double.parseDouble(f[1]);
            double ask    = Double.parseDouble(f[2]);
            double volume = Double.parseDouble(f[3]);
            double spread = Double.parseDouble(f[4]);
            String symbol = f[5].trim();

            // validazione: scarto tick sporchi
            if (bid <= 0 || ask < bid) return null;

            return Tick.newBuilder()  //crea un builder avro
                       .setSymbol(symbol)
                       .setTimestamp(ts)
                       .setBid(bid)
                       .setAsk(ask)
                       .setVolume(volume)
                       .setSpread(spread)
                       .build(); //restituisce un oggetto Tick (Avro)
        } catch (RuntimeException ex) {
            return null; // numeri/date non parsabili -> scarto la riga
        }
    }

    /** DateTime -> epoch millis. ADATTA fuso/formato al tuo CSV reale. */
    private static long parseTimestamp(String s) {
        s = s.trim();
        // se il campo è già un epoch numerico, usalo direttamente
        if (s.chars().allMatch(Character::isDigit)) {
            return Long.parseLong(s);
        }
        return LocalDateTime.parse(s, FORMATTER)
                            .toInstant(ZoneOffset.UTC)   // verifica il fuso!
                            .toEpochMilli();
    }

    @Override
    public void close() throws IOException {
        r.close();
    }
}
