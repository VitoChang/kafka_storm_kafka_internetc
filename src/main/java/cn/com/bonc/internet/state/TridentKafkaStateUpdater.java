package cn.com.bonc.internet.state;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * @Author:zhanglong
 * @Date:2018/1/16 15:30
 * @Param:
 * @Description:
 */
public class TridentKafkaStateUpdater implements StateUpdater<TridentKafkaState> {
    @Override
    public void updateState(TridentKafkaState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
