package com.thoughtworks.samples.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WebLogAnalysisTopology {

  public static void main(String[] args) throws InterruptedException {
    WebLogAnalysisTopology topology = new WebLogAnalysisTopology();
    topology.run(args);
  }

  private void run(String[] args) throws InterruptedException {
    Config config = configureTopology("/Users/hemanty/Downloads/datasets/access.log");
    TopologyBuilder topologyBuilder = buildTopology();
    submitTopology(config, topologyBuilder);
  }

  private void submitTopology(Config config, TopologyBuilder topologyBuilder) throws InterruptedException {
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("weblog-analysis", config, topologyBuilder.createTopology());
    Thread.sleep(10000);
    localCluster.shutdown();
  }

  private TopologyBuilder buildTopology() {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("log_entry_spout", new LogEntrySpout(), 5);
    return topologyBuilder;
  }

  private Config configureTopology(String arg) {
    Config config = new Config();
    config.setMaxTaskParallelism(3);
    config.put(LogEntrySpout.LOGENTRYSPOUT_SOURCE_FILENAME, arg);
    return config;
  }
}
