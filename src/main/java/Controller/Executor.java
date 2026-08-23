package Controller;

import Service.PartitionPlanner;
import Service.TickReader;
import domain.Tick;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Producer: scorre i CSV giornalieri IN ORDINE DI DATA
 * e pubblica ogni tick su Kafka (topic "ticks"), serializzato con Avro.
 */
public class Executor {

    private static final String TOPIC = "ticks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public void execute(Path dataDir) throws IOException {

        // 1) configurazione producer Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        try (KafkaProducer<String, Tick> producer = new KafkaProducer<>(props)) {

            // 2) elenco dei file giornalieri, ordinati per data
            List<Path> giorni;
            try (Stream<Path> s = Files.list(dataDir)) {
                giorni = s.filter(p -> p.toString().toLowerCase().endsWith(".csv"))
                          .sorted(Comparator.comparing(Path::getFileName))
                          .collect(Collectors.toList());
            }
            int trovati = giorni.size();

            // 2a) finestra di INGESTIONE.
            //     I nomi file sono YYYY-MM-DD.csv, quindi il confronto lessicografico
            //     coincide con quello cronologico.
            //     -DfromDate=2026-03-11  -> parte dal giorno 49 (warmup 22gg + test)
            String fromDate = System.getProperty("fromDate", "");
            if (!fromDate.isEmpty()) {
                giorni = giorni.stream()
                               .filter(p -> p.getFileName().toString().compareTo(fromDate) >= 0)
                               .collect(Collectors.toList());
            }
            //     -DmaxDays=10 -> tiene solo gli ultimi N giorni (0 = nessun limite)
            int maxDays = Integer.getInteger("maxDays", 0);
            if (maxDays > 0 && giorni.size() > maxDays) {
                giorni = giorni.subList(giorni.size() - maxDays, giorni.size());
            }

            if (giorni.isEmpty()) {
                System.out.println("Nessun file da processare (trovati " + trovati
                        + ", esclusi tutti dai filtri fromDate/maxDays).");
                return;
            }
            System.out.printf("File da processare: %d/%d  (dal %s al %s)%n",
                              giorni.size(), trovati,
                              giorni.get(0).getFileName(),
                              giorni.get(giorni.size() - 1).getFileName());

            // 2b) partizionamento load-aware (-Dpartitioner=lpt|hash, default lpt).
            //     "hash" = partitioner di default di Kafka, murmur2(symbol) % P: cieco al
            //     carico, sbilancia ~2,4x. "lpt" = mappa ricalcolata a ogni fine giornata
            //     sulla media dei tick degli ultimi N giorni (solo passato), vedi
            //     PartitionPlanner e docs/skew-partitioning.md.
            String mode = System.getProperty("partitioner", "lpt");
            int window  = Integer.getInteger("partitioner.window", 2);
            int numPartitions = producer.partitionsFor(TOPIC).size();
            PartitionPlanner planner = "lpt".equalsIgnoreCase(mode)
                    ? new PartitionPlanner(numPartitions, window) : null;
            System.out.printf("[producer] partitioner=%s  partizioni=%d%s%n",
                    mode, numPartitions,
                    planner == null ? "" : "  finestra=" + window + "gg");

            long totale = 0;
            long start  = System.currentTimeMillis();

            // 3) un giorno alla volta viene mandato nel topic kafka
            for (Path giorno : giorni) {
                long perFile = 0;
                // piano calcolato SOLO sui giorni gia' ingeriti; null finche' manca storia
                Map<String, Integer> plan = planner == null ? null : planner.planFor();
                long[] perPartition = new long[numPartitions];

                try (TickReader reader = new TickReader(giorno)) {
                    Tick tick;
                    while ((tick = reader. next()) != null) {
                        String symbol = tick.getSymbol().toString();
                        Integer partition = plan == null ? null : plan.get(symbol);
                        producer.send(partition == null
                                ? new ProducerRecord<>(TOPIC, symbol, tick)
                                : new ProducerRecord<>(TOPIC, partition, symbol, tick));
                        if (planner != null) {
                            planner.record(symbol);
                            // la diagnostica conta solo i tick instradati dal piano; quelli
                            // lasciati all'hash (warmup, simboli mai visti) non sono tracciabili
                            // qui senza replicare murmur2
                            if (partition != null) perPartition[partition]++;
                        }
                        perFile++;
                    }
                }
                totale += perFile;

                if (planner == null) {
                    System.out.printf("  %-30s -> %,d tick%n", giorno.getFileName(), perFile);
                } else if (plan == null) {
                    // storia insufficiente: ha instradato Kafka, il contatore per partizione
                    // e' vuoto e stampare un max/mean qui darebbe un 1,00x privo di senso
                    System.out.printf("  %-30s -> %,d tick   (warmup: hash di default)%n",
                            giorno.getFileName(), perFile);
                    planner.endOfDay();
                } else {
                    System.out.printf("  %-30s -> %,d tick   max/mean=%.2fx%n",
                            giorno.getFileName(), perFile,
                            PartitionPlanner.imbalance(perPartition));
                    planner.endOfDay();
                }
            }

            long ms = System.currentTimeMillis() - start;
            System.out.printf("TOTALE: %,d tick in %,d ms (%,.0f tick/s)%n",
                              totale, ms, totale / (ms / 1000.0));
        }
    }
}
