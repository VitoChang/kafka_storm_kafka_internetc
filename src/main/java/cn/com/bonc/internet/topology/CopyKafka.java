package cn.com.bonc.internet.topology;

import cn.com.bonc.internet.state.TridentKafkaState;
import cn.com.bonc.internet.state.TridentKafkaStateFactory;
import cn.com.bonc.internet.state.TridentKafkaStateUpdater;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.MessageMetadataSchemeAsMultiScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.tuple.Fields;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class CopyKafka {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, IOException {
        if (args.length != 10 && args.length != 11) {
            System.out.println("arguments should be :" + "[spoutId,srcZk,srcTopic,groupId,numPartitions,desTopic,numWorks,name,fromBeginging(,initOffset)]");
            System.exit(1);
        }
        String spoutId = args[0];
        String srcZk = args[1];
        String srcTopic = args[2];
        String groupId = args[3];
        int numPartitions = Integer.parseInt(args[4]);
        String desBroker = args[5];
        String desTopic = args[6];
        int numWorkers = Integer.parseInt(args[7]);
        String name = args[8];

        boolean fromBeginning = Boolean.parseBoolean(args[9]);

        Map<Integer, Long> initOffsets = null;

        if (args.length == 11) {

            initOffsets = initOffsets(args[10]);


        }

        TridentTopology topology = new TridentTopology();
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory().withProducerProperties(newProducerProperties(desBroker)).withKafkaTopicSelector(new DefaultTopicSelector(desTopic)).withTridentTupleToKafkaMapper(new OffsetToKeyMapper("offset", "value"));
        StateUpdater<TridentKafkaState> updater = new TridentKafkaStateUpdater();


        for (int par = 0; par < numPartitions; par++) {
            TridentState stream = topology.newStream(spoutId + par, newOpaqueTridentKafkaSpout(srcZk, srcTopic, par, groupId, fromBeginning, initOffsets)).partitionPersist(stateFactory, new Fields("offset", "partition", "value"), updater, new Fields());
        }
        Config conf = new Config();
        conf.setMaxSpoutPending(1);
        conf.setNumWorkers(numWorkers);
        conf.setMessageTimeoutSecs(Integer.MAX_VALUE);
        StormSubmitter.submitTopology(name, conf, topology.build());
    }

    /**
     * @param srcZk
     * @param srcTopic
     * @param par
     * @param groupId
     * @param fromBeginning
     * @param initOffsets
     * @return
     */
    private static OpaqueTridentKafkaSpout newOpaqueTridentKafkaSpout(String srcZk, String srcTopic, int par, String groupId, boolean fromBeginning, Map<Integer, Long> initOffsets) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts(srcZk), srcTopic, par, groupId);
        kafkaConfig.firstPollOffsetStrategy = fromBeginning ? KafkaConfig.FirstPollOffsetStrategy.UNCUSTOMIZED_EARLIEST:kafkaConfig.firstPollOffsetStrategy.UNCUSTOMIZED_LATEST;
        if(initOffsets !=null && !initOffsets.isEmpty()){
            kafkaConfig.tpOffsetMap=initOffsets;
        }
kafkaConfig.scheme=new MessageMetadataSchemeAsMultiScheme(new SimpleScheme());
        return new OpaqueTridentKafkaSpout(kafkaConfig);
    }

    /**
     * 生产者配置
     *
     * @param bootstrapServers
     * @return
     */
    private static Properties newProducerProperties(final String bootstrapServers) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServers);
        prop.put("ack", "all");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        prop.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required" + "username=\"cb_admin\" password=\"e0d_9cA_b47\";");
        prop.put("security.protocol", "SASL_PLAINTEXT");
        prop.put("sasl.mechanism", "PLAIN");
        return prop;
    }


    /**
     * @param file 文件绝对路径
     * @return
     * @throws IOException
     */
    private static Map<Integer, Long> initOffsets(String file) throws IOException {
        Properties props = new Properties();
        FileReader fr = null;
        try {
            fr = new FileReader(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        props.load(fr);
        Map<Integer, Long> ret = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            ret.put(Integer.parseInt(key), Long.parseLong(props.getProperty(key)));
        }
        System.out.println("init_Offset:" + ret);
        return ret;
    }


}
