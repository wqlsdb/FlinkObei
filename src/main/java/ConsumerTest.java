import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerTest {
    public static void main(String[] args) throws Exception {

/*        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.80.59.10:9092,10.80.59.11:9092,10.80.59.12:9092")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setTopics("sc_caputer_change_topic")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                //.setStartingOffsets(OffsetsInitializer.earliest())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafkasource").print();
        env.execute();*/
    }
}
