import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class cdcGdCommodityPrice {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","obeiadmin");
        Properties properties = new Properties();
        properties.put("decimal.handling.mode","string");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                //test 10.80.59.23   root  !QAZxsw2   uat 10.80.20.62 obeidbuat !QAZxsw2
                .hostname("10.80.9.129").port(3306)
                .databaseList("obei_store_goods").tableList("obei_store_goods.t_gd_commodity_price")
                .username("kettle").password("C@iNPucd9wJ")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/checkpoint/cdcGdCommodityPrice");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                300, // 尝试重启的次数
                Time.of(60, TimeUnit.SECONDS) // 间隔
        ));

        FlinkRebalancePartitioner<Object> rebalancePartitioner = new FlinkRebalancePartitioner<>();
        KafkaSink<String> sinkKafka = KafkaSink.<String>builder()
                //10.80.59.10:9092,10.80.59.11:9092,10.80.59.12:9092    uat 10.80.20.97:9092,10.80.20.98:9092,10.80.20.99:9092
                .setBootstrapServers("10.80.9.11:9092,10.80.9.12:9092,10.80.9.13:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sc_caputer_change_topic_prod_test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(rebalancePartitioner)
                        .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("max.request.size", "2097152")
                //.setProperty("transaction.timeout.ms", "600000")
                .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource")
                .setParallelism(3);
        SingleOutputStreamOperator<String> mapStream = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                //before
                JSONObject before = jsonObject.getJSONObject("before");
                String commodity_code = before.getString("COMMODITY_CODE");
                String price = before.getString("PRICE");
                String untaxed_price = before.getString("UNTAXED_PRICE");
                String price_type = before.getString("PRICE_TYPE");
                String memo = before.getString("MEMO");
                String alive_flag = before.getString("ALIVE_FLAG");
                //after
                JSONObject after = jsonObject.getJSONObject("after");
                commodity_code = commodity_code + "->after: " + after.getString("COMMODITY_CODE");
                price = price + "->after: " + after.getString("PRICE");
                untaxed_price = untaxed_price + "->after: " + after.getString("UNTAXED_PRICE");
                price_type = price_type + "->after: " + after.getString("PRICE_TYPE");
                memo = memo + "->after: " + after.getString("MEMO");
                alive_flag = alive_flag + "->after: " + after.getString("ALIVE_FLAG");
                String Change = "{ " + commodity_code + "," + price + "," + untaxed_price + "," + untaxed_price + "," + price_type + "," + memo + "," + alive_flag + " }";
                return Change;
            }
        });
        mapStream.sinkTo(sinkKafka);

        env.execute("cdcGdCommodityPrice");
    }


}
