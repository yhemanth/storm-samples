package com.dsinpractice.storm.samples;

/*
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
*/
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.Testing;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/*import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;*/

/**
 * This topology demonstrates Storm's HiveBolt library to write to Hive.
 */
public class WordCountTopology {

    public static final Character HDFS_PATH_SHORT_OPT = 'p';
    public static final Character TOPIC_NAME_SHORT_OPT = 't';
    public static final Character TOPOLOGY_NAME_SHORT_OPT = 'n';
    public static final Character CLUSTER_SHORT_OPT = 'c';
    public static final Character ENABLE_HOOK_SHORT_OPT = 'k';

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

    private static CommandLine parseCommandLineArgs(String[] args) throws ParseException {
        Options options = configureOptions();
        if (args.length == 0) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("storm jar <jarfile> <class> <options>", options);
            System.exit(-1);
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
    public static void main(String[] args) throws Exception {

        CommandLine commandLine = parseCommandLineArgs(args);

        TopologyBuilder builder = getTopologyBuilder(commandLine);

        Config conf = new Config();
        conf.setDebug(true);

        String topologyName = "test-topology";
        if (commandLine.hasOption(TOPOLOGY_NAME_SHORT_OPT)) {
            topologyName = commandLine.getOptionValue(TOPOLOGY_NAME_SHORT_OPT);
        }

        if (!commandLine.hasOption(CLUSTER_SHORT_OPT)) {
            if (commandLine.hasOption(ENABLE_HOOK_SHORT_OPT)) {
                conf.putAll(Utils.readDefaultConfig());
                conf.setDebug(true);
                submitToLocal(builder, conf, topologyName, true);
            } else {
                submitToLocal(builder, conf, topologyName, false);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(commandLine.getOptionValue(TOPOLOGY_NAME_SHORT_OPT), conf,
                    builder.createTopology());
        }
    }

    private static TopologyBuilder getTopologyBuilder(CommandLine commandLine) {
        IRichSpout kafkaSpout = getKafkaSpout(commandLine);
        HdfsBolt hdfsBolt = getHdfsBolt(commandLine);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout, 1);
        builder.setBolt("split", new JavaSplitSentence(), 1).shuffleGrouping("kafka-spout");
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("count");
        return builder;
    }

    private static HdfsBolt getHdfsBolt(CommandLine commandLine) {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(",");
        SyncPolicy countSyncPolicy = new CountSyncPolicy(100);
        FileSizeRotationPolicy fileSizeRotationPolicy = new FileSizeRotationPolicy(5.0f,
                FileSizeRotationPolicy.Units.MB);
        String filePath = "/user/storm/files";
        if (commandLine.hasOption(HDFS_PATH_SHORT_OPT)) {
            filePath = commandLine.getOptionValue(HDFS_PATH_SHORT_OPT);
        }
        DefaultFileNameFormat defaultFileNameFormat = new DefaultFileNameFormat().withPath(filePath);

        return new HdfsBolt().withFsUrl("hdfs://localhost.localdomain:8020").
                withFileNameFormat(defaultFileNameFormat).
                withRecordFormat(recordFormat).
                withSyncPolicy(countSyncPolicy).
                withRotationPolicy(fileSizeRotationPolicy);
    }

    private static Options configureOptions() {
        Options options = new Options();
        options.addOption(TOPIC_NAME_SHORT_OPT.toString(), "topic", true, "Kafka topic name");
        options.addOption(HDFS_PATH_SHORT_OPT.toString(), "path", true, "HDFS file path");
        options.addOption(TOPOLOGY_NAME_SHORT_OPT.toString(), "name", true, "Name of the topology");
        options.addOption(CLUSTER_SHORT_OPT.toString(), "cluster", true, "Run on cluster if specified");
        options.addOption(ENABLE_HOOK_SHORT_OPT.toString(), "hook", true, "Enable Storm Atlas Hook if specified");
        return options;
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

    private static IRichSpout getKafkaSpout(CommandLine commandLine) {
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        String topicName = "test-topic";
        if (commandLine.hasOption(TOPIC_NAME_SHORT_OPT)) {
            topicName = commandLine.getOptionValue(TOPIC_NAME_SHORT_OPT);
        }
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
