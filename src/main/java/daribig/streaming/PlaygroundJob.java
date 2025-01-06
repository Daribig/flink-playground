package daribig.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class PlaygroundJob {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");

        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "java-client");

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setProperties(props)
            .setTopics("event-queue")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "event-source")
            .print(); // sink records to stdout

        env.disableOperatorChaining(); // better for learning - can clearly see all transformations in pipeline

        env.execute("playground");

//        pollTopic(props);
    }

    /**
     * Exploring how to poll topic without using the {@link KafkaSource} source.
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