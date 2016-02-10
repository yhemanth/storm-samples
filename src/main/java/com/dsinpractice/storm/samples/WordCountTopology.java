package com.dsinpractice.storm.samples;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.Testing;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
/*import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.Testing;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
/*import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;*/

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This topology demonstrates Storm's HiveBolt library to write to Hive.
 */
public class WordCountTopology {

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", getKafkaSpout(), 1);

        builder.setBolt("split", new JavaSplitSentence(), 1).shuffleGrouping("kafka-spout");

        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));
        HiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields("word", "count"));

        HiveOptions options = new HiveOptions("thrift://localhost:9083", "default", "storm_words_table", mapper)
                .withBatchSize(1000)
                .withTxnsPerBatch(100)
                .withIdleTimeout(10);
        builder.setBolt("hive-bolt", new HiveBolt(options), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);

        if (args.length == 0) {
            System.out.println("Usage: storm jar <jar-file> <name> [enable-hook | cluster]");
            System.exit(-1);
        }

        if (args.length == 1) {
            submitToLocal(builder, conf, args[0], false);
        } else {
            if (args[1].equals("enable-hook")) {
                conf.putAll(Utils.readDefaultConfig());
                conf.setDebug(true);
                submitToLocal(builder, conf, args[0], true);
            } else {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
        }
    }

    private static void submitToLocal(TopologyBuilder builder, Config conf, String name, boolean enableHook)
            throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        if (enableHook) {
            conf.put(Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN,
                    "org.apache.atlas.storm.hook.StormAtlasHook");
        }

        conf.setMaxTaskParallelism(3);

        Map<String,Object> localClusterConf = new HashMap<>();
        localClusterConf.put("nimbus-daemon", true);
        ILocalCluster cluster = Testing.getLocalCluster(localClusterConf);

        cluster.submitTopology(name, conf, builder.createTopology());

        Thread.sleep(600000);

        cluster.shutdown();
    }

    private static IRichSpout getKafkaSpout() {
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        String topicName = "test-topic";
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    private static class JavaSplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String sentence = tuple.getString(0);
            String[] words = sentence.split(" ");
            for (String word : words) {
                basicOutputCollector.emit(new Values(word.toLowerCase()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }
}
