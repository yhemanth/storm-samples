package com.dsinpractice.storm.samples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("sh", "call_sentencesplitter.sh");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

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
            collector.emit(new Values(word, count, word.charAt(0)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count", "letter"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", getKafkaSpout(), 1);

        builder.setBolt("split", new SplitSentence(), 2).shuffleGrouping("kafka-spout");

        builder.setBolt("count", new WordCount(), 2).fieldsGrouping("split", new Fields("word"));
        HiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields("word", "count"))
                .withPartitionFields(new Fields("letter"));

        HiveOptions options = new HiveOptions("thrift://localhost:9083", "default", "storm_words_table", mapper)
                .withBatchSize(100)
                .withTxnsPerBatch(10)
                .withIdleTimeout(10);
        builder.setBolt("hive-bolt", new HiveBolt(options), 2).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }

    private static IRichSpout getKafkaSpout() {
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        String topicName = "test-topic";
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }
}
