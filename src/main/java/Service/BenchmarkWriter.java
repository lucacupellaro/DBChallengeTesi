package Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Scrive una riga di risultato benchmark in un CSV.
 * Se il file non esiste lo crea con l'header.
 */
public class BenchmarkWriter {

    private static final String HEADER =
            "run_timestamp,parallelism,kafka_partitions,checkpoint_ms," +
            "ml_enabled,total_ticks,elapsed_ms,throughput_ticks_per_sec";

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final Path csvPath;

    public BenchmarkWriter(Path csvPath) {
        this.csvPath = csvPath;
    }

    public void write(int parallelism, int kafkaPartitions, long checkpointMs,
                      boolean mlEnabled, long totalTicks, long elapsedMs) throws IOException {

        double throughput = elapsedMs > 0
                ? totalTicks / (elapsedMs / 1000.0)
                : 0.0;

        boolean needsHeader = !Files.exists(csvPath) || Files.size(csvPath) == 0;

        if (!Files.exists(csvPath.getParent())) {
            Files.createDirectories(csvPath.getParent());
        }

        StringBuilder sb = new StringBuilder();
        if (needsHeader) {
            sb.append(HEADER).append('\n');
        }
        sb.append(String.format("%s,%d,%d,%d,%s,%d,%d,%.2f%n",
                LocalDateTime.now().format(FMT),
                parallelism, kafkaPartitions, checkpointMs,
                mlEnabled, totalTicks, elapsedMs, throughput));

        Files.write(csvPath, sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
