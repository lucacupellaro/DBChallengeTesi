package Service;

import domain.Tick;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Job Flink che consuma i tick dal topic Kafka "ticks"
 * e li stampa a console. Le trasformazioni (window, aggregazioni, ecc.)
 * verranno aggiunte in futuro.
 */
public class FlinkJob {

    private static final String TOPIC = "ticks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "flink-tick-consumer";

    public void start() throws Exception {

        // 1) ambiente Flink in streaming
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2) source Kafka: legge dal topic "ticks", deserializza Avro tramite Schema Registry
        KafkaSource<Tick> kafkaSource = KafkaSource.<Tick>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                Tick.class, SCHEMA_REGISTRY_URL))
                .build();

        // 3) crea lo stream di Tick
        DataStream<Tick> tickStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),  // per ora niente event-time, si aggiunge dopo
                "kafka-ticks-source");

        // 4) per ora stampa a console — qui in futuro vanno le trasformazioni
        tickStream.print();

        // TODO: qui andranno
        //   .keyBy(tick -> tick.getSymbol().toString())
        //   .window(...)
        //   .aggregate(...)
        //   .sinkTo(...)

        // 5) avvia il job
        env.execute("TickConsumerJob");
    }
}
