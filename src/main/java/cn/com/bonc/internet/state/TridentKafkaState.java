package cn.com.bonc.internet.state;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TridentKafkaState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.kafka.trident.TridentKafkaState.class);
    private KafkaProducer producer;
    private OutputCollector collector;
    private Properties options;
    private TridentTupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;

    public TridentKafkaState() {
    }

    public TridentKafkaState withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaState withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is Noop.");
    }

    public void commit(Long txid) {
        LOG.debug("commit is Noop.");
    }

    public void prepare(Properties options) {
        Validate.notNull(this.mapper, "mapper can not be null");
        Validate.notNull(this.topicSelector, "topicSelector can not be null");
        this.producer = new KafkaProducer(options);
        this.options=options;
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        String topic = null;
        Integer partition;
        int count =0;
        try {
            List<Future<RecordMetadata>> futures = new ArrayList(tuples.size());
            Iterator var11 = tuples.iterator();

            for(TridentTuple tuple : tuples) {
                topic = this.topicSelector.getTopic(tuple);
                partition = tuple.getIntegerByField("partition");
                if (topic != null) {
                    try {
                        count=0;
                        this.producer.send(new ProducerRecord(topic, partition, this.mapper.getKeyFromTuple(tuple), this.mapper.getMessageFromTuple(tuple))).get();
                    } catch (Exception e) {
                        String errorMsg = "Could not send(retry"+count+") message with partition=[" + partition + "] key=[" + mapper.getKeyFromTuple(tuple) + "]" + "value=[" + mapper.getMessageFromTuple(tuple) + "] to topic=[" + topic + "]";
                        LOG.warn(errorMsg);

                        while (true) {
                            producer.close();
                            try {
                                Thread.sleep(300);
                                producer = new KafkaProducer(options);
                                count++;
                                producer.send(new ProducerRecord(topic, partition, this.mapper.getKeyFromTuple(tuple), this.mapper.getMessageFromTuple(tuple))).get();
                                LOG.warn("send successfully in retry(" + count + ") message with partition =[" + partition + "] key=[" + mapper.getKeyFromTuple(tuple) + "]" + "value=[" + new String((byte[]) mapper.getMessageFromTuple(tuple)) + "]");
                                break;
                            }catch (Exception e1){
                                String errorMsg2="Could not send(retry"+count+") message with partition =["+partition+"] key=["+mapper.getKeyFromTuple(tuple)+"] value=["+ new String((byte[])mapper.getMessageFromTuple(tuple))+"]";
                                LOG.warn(errorMsg2,e1);
                            }
                        }
                    }
                } else {
                    LOG.warn("skipping key = " + this.mapper.getKeyFromTuple(tuple) + ", topic selector returned null.");
                }

            }
        } catch (Exception var10) {
            String errorMsg = "Could not send messages " + tuples + " to topic = " + topic;
            LOG.warn(errorMsg, var10);
            throw new FailedException(errorMsg, var10);
        }
    }
}

