package cn.com.bonc.internet.state;


import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;



public class TridentKafkaStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.kafka.trident.TridentKafkaStateFactory.class);
    private TridentTupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;
    private Properties producerProperties = new Properties();

    public TridentKafkaStateFactory() {
    }

    public TridentKafkaStateFactory withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaStateFactory withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public TridentKafkaStateFactory withProducerProperties(Properties props) {
        this.producerProperties = props;
        return this;
    }

    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        TridentKafkaState state = (new TridentKafkaState()).withKafkaTopicSelector(this.topicSelector).withTridentTupleToKafkaMapper(this.mapper);
        state.prepare(this.producerProperties);
        return state;
    }
}

