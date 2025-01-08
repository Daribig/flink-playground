package daribig.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class PlaygroundJob {

    public static final String C_BOOTSTRAP_SERVER = "bootstrap.servers";
    public static final String C_KAFKA_INPUT_QUEUE = "input-queue";
    public static final String C_KAFKA_SINK_QUEUE = "output-queue";
    public static final String C_GROUP_ID_KEY = "group.id";
    public static final String C_GROUP_ID_VALUE = "java-client";

    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");

        var props = new Properties();
        props.put(C_BOOTSTRAP_SERVER, "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(C_GROUP_ID_KEY, C_GROUP_ID_VALUE);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setProperties(props)
            .setTopics(C_KAFKA_INPUT_QUEUE)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSink<String> kafkaSink = buildKafkaSink(C_KAFKA_SINK_QUEUE);

        StreamExecutionEnvironment pipeline = StreamExecutionEnvironment.getExecutionEnvironment();

        pipeline
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-event-source")
            .sinkTo(kafkaSink);
            // .print(); // sink records to stdout

        pipeline.disableOperatorChaining(); // better for learning - can clearly see all transformations in pipeline

        pipeline.execute("playground");

//        pollTopic(props);
    }

    private static KafkaSink<String> buildKafkaSink(String queueName) {
        Properties properties = new Properties();
        properties.put(C_BOOTSTRAP_SERVER, "localhost:9092");
        properties.put(C_GROUP_ID_KEY, C_GROUP_ID_VALUE);

        KafkaRecordSerializationSchema<String> recordSerialiser = KafkaRecordSerializationSchema.<String>builder()
                .setTopic(queueName)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        return KafkaSink.<String>builder()
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(recordSerialiser)
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .build();
    }

    /**
     * Exploring how to poll topic without using Flink's {@link KafkaSource} source.
     */
    private static void pollTopic(Properties props) {
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("event-queue"));
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                System.out.println("--- polling ---");
                records.forEach(record ->
                        System.out.println(
                                record.offset() + " -> " + record.value()));
            }
        }
    }
}