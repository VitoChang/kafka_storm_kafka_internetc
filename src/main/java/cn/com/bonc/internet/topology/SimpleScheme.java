package cn.com.bonc.internet.topology;

import org.apache.storm.kafka.MessageMetadataScheme;
import org.apache.storm.kafka.Partition;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @Author:zhanglong
 * @Date:2018/1/16 17:46
 * @Param:
 * @Description:
 */
public class SimpleScheme implements MessageMetadataScheme {
    @Override
    public List<Object> deserializeMessageWithMetadata(ByteBuffer message, Partition partition, long offset) {
        byte[] bytes = Utils.toByteArray(message);
        return new Values(bytes,partition.topic,partition.partition,offset);
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("value","topic","partition","offset");
    }
}
