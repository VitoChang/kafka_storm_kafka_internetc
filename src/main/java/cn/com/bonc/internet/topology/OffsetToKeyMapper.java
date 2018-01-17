package cn.com.bonc.internet.topology;

import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @Author:zhanglong
 * @Date:2018/1/16 11:32
 * @Param:
 * @Description:
 */
public class OffsetToKeyMapper<k, v> implements TridentTupleToKafkaMapper {
    public final String keyFieldName;
    public final String msgFieldName;

    public OffsetToKeyMapper(String keyFieldName, String msgFieldName) {
        this.keyFieldName = keyFieldName;
        this.msgFieldName = msgFieldName;
    }

    @Override
    public k getKeyFromTuple(TridentTuple tridentTuple) {
        long offset = tridentTuple.getLongByField(keyFieldName);
        return (k) ("" + offset + ",0");
    }

    @Override
    public v getMessageFromTuple(TridentTuple tridentTuple) {
        return (v) tridentTuple.getValueByField(msgFieldName);
    }
}
