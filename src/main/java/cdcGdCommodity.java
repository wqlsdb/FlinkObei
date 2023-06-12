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
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
/*
* todo:出现中文乱码 执行的时候添加jvm参数即可
*  /opt/module/flink-1.17.0/bin/flink run -t yarn-per-job -Djobmanager.memory.process.size=4096m -Dtaskmanager.memory.process.size=4096m \
* -Denv.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" \
* -Dyarn.appmaster.vcores=4 -Dyarn.containers.vcores=1 -Dyarn.application.queue=flink -Dyarn.application.name=cdcGdCommodity -c cdcGdCommodity /data/myjars/original-SC_Realtime_Demand-1.0-SNAPSHOT.jar
* */

public class cdcGdCommodity {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","obeiadmin");
        //System.setProperty("file.encoding", "UTF-8");
        Properties properties = new Properties();
        properties.put("decimal.handling.mode","string");
        //properties.put("file.encoding","utf-8");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.80.9.129").port(3306)
                .databaseList("obei_store_goods").tableList("obei_store_goods.t_gd_commodity")
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
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/checkpoint/cdcGdCommodity");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                300, // 尝试重启的次数
                Time.of(60, TimeUnit.SECONDS) // 间隔
        ));

        //MyPartitioner myPartitioner = new MyPartitioner();
        //FlinkRebalancePartitioner<Object> objectFlinkRebalancePartitioner = FlinkRebalancePartitioner<>();
       // FlinkRebalancePartitioner<Object> flinkRebalancePartitioner = new FlinkRebalancePartitioner<>();
        FlinkRebalancePartitioner<Object> rebalancePartitioner = new FlinkRebalancePartitioner<>();
        KafkaSink<String> sinkKafka = KafkaSink.<String>builder()
                //.setBootstrapServers("10.80.59.10:9092,10.80.59.11:9092,10.80.59.12:9092")
                //.setBootstrapServers("10.80.59.69:9092,10.80.59.70:9092,10.80.59.71:9092")
                .setBootstrapServers(" 10.80.9.11:9092,10.80.9.12:9092,10.80.9.13:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sc_caputer_change_topic_prod")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(rebalancePartitioner)
                        .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("max.request.size", "2097152")
                .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource")
                .setParallelism(3);
        //dataStreamSource.print();
        KeyedStream<String, String> keyedStream = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).keyBy(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject source = jsonObject.getJSONObject("source");
            String commodity_code = source.getString("COMMODITY_CODE");
            return (commodity_code != null) ? commodity_code : "";
        });
        keyedStream.sinkTo(sinkKafka);
        env.execute("cdcGdCommodity");
    }

    /*public static class FlinkRebalancePartitioner<T> extends FlinkKafkaPartitioner<T> {
        private static final long serialVersionUID = -3785320239953858777L;
        private int parallelInstanceId;
        private int nextPartitionToSendTo;

        public FlinkRebalancePartitioner(){

        }

        @Override
        public void open(int parallelInstanceId, int parallelInstances) {
            //super.open(parallelInstanceId, parallelInstances);
            this.parallelInstanceId=parallelInstanceId;
            nextPartitionToSendTo = ThreadLocalRandom.current().nextInt(parallelInstances);
        }

        @Override
        public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            //return 0;
            Preconditions.checkArgument(partitions != null && partitions.length>0,"Partitions of the target topic is empty.");
            nextPartitionToSendTo=(nextPartitionToSendTo + 1) % partitions.length;
            return partitions[nextPartitionToSendTo];
        }
    }*/
}
