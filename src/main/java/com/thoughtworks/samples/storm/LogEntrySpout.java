package com.thoughtworks.samples.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

public class LogEntrySpout extends BaseRichSpout {
  public static final String LOGENTRYSPOUT_SOURCE_FILENAME = "logentryspout.source.filename";

  private LogEntrySource logEntrySource;
  private Throttle throttle;
  private SpoutOutputCollector spoutOutputCollector;

  @Override
  public void open(Map configuration, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    logEntrySource = new LogEntrySource((String)configuration.get(LOGENTRYSPOUT_SOURCE_FILENAME));
    throttle = new Throttle();
    this.spoutOutputCollector = spoutOutputCollector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("log_entry"));
  }

  @Override
  public void nextTuple() {
    throttle.throttle();
    String logEntry = logEntrySource.nextLogEntry();
    System.out.println("Emitting log entry " + logEntry);
    spoutOutputCollector.emit(new Values(logEntry));
  }
}
