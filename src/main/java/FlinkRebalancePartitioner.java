import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

public class FlinkRebalancePartitioner<T> extends FlinkKafkaPartitioner<T> {
    private static final long serialVersionUID = -3785320239953858777L;
    private int parallelInstanceId;
    private int nextPartitionToSendTo;
    public FlinkRebalancePartitioner(){

    }

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        this.parallelInstanceId=parallelInstanceId;
        nextPartitionToSendTo = ThreadLocalRandom.current().nextInt(parallelInstances);
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length>0,"Partitions of the target topic is empty.");
        nextPartitionToSendTo=(nextPartitionToSendTo + 1) % partitions.length;
        return partitions[nextPartitionToSendTo];
    }
}
