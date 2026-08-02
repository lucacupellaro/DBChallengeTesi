package Service;

import Controller.Executor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Orchestratore benchmark: per ogni combinazione (kafka partitions, flink parallelism)
 * ricrea il topic, ri-ingestiona i tick e lancia il job Flink,
 * scrivendo i risultati su CSV.
 */
public class BenchmarkRunner {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ticks";

    private final Path dataDir;
    private final int[] partitions;
    private final int[] parallelisms;

    public BenchmarkRunner(Path dataDir, int[] partitions, int[] parallelisms) {
        this.dataDir = dataDir;
        this.partitions = partitions;
        this.parallelisms = parallelisms;
    }

    public void run() throws Exception {
        int totalRuns = partitions.length * parallelisms.length;
        int currentRun = 0;

        System.out.printf("[benchmark-full] %d combinazioni: %d partizioni x %d parallelismi%n",
                totalRuns, partitions.length, parallelisms.length);

        for (int numPartitions : partitions) {
            // 1. Ricrea il topic con il nuovo numero di partizioni
            recreateTopic(numPartitions);

            // 2. Ri-ingestiona i tick
            System.out.printf("%n>>> INGESTION con %d partizioni Kafka...%n", numPartitions);
            new Executor().execute(dataDir);

            // 3. Sweep parallelismo
            for (int p : parallelisms) {
                currentRun++;
                System.out.printf(
                        "%n========== RUN %d/%d  partitions=%d  parallelism=%d ==========%n",
                        currentRun, totalRuns, numPartitions, p);
                try {
                    new FlinkJob(p, numPartitions).start();
                } catch (Exception e) {
                    Throwable cause = e;
                    while (cause.getCause() != null) cause = cause.getCause();
                    System.err.printf("[benchmark-full] ERRORE partitions=%d parallelism=%d: %s%n",
                            numPartitions, p, cause.getMessage());
                }
            }
        }

        System.out.println("\n[benchmark-full] completato -> src/main/java/results/benchmark.csv");
    }

    private void recreateTopic(int numPartitions) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            // Cancella il topic se esiste
            if (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(TOPIC)) {
                System.out.printf("[kafka] cancello topic '%s'...%n", TOPIC);
                admin.deleteTopics(Collections.singletonList(TOPIC)).all().get(30, TimeUnit.SECONDS);
                // Attendi che la cancellazione si propaghi completamente
                Thread.sleep(5000);
                // Verifica che sia effettivamente sparito
                while (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(TOPIC)) {
                    System.out.println("[kafka] attendo cancellazione topic...");
                    Thread.sleep(2000);
                }
            }

            // Crea il topic con il numero di partizioni desiderato
            System.out.printf("[kafka] creo topic '%s' con %d partizioni...%n", TOPIC, numPartitions);
            NewTopic newTopic = new NewTopic(TOPIC, numPartitions, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);
            Thread.sleep(3000);

            System.out.printf("[kafka] topic '%s' pronto (%d partizioni)%n", TOPIC, numPartitions);
        }
    }
}
