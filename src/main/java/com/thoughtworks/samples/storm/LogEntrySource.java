package com.thoughtworks.samples.storm;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class LogEntrySource {

  private String sourceFileName;
  private BufferedReader sourceFileReader;

  public LogEntrySource(String sourceFileName) {
    this.sourceFileName = sourceFileName;
    open();
  }

  public String nextLogEntry() {
    String logEntry = read();
    if (reachedEnd(logEntry)) {
      reopen();
      logEntry = read();
    }
    return logEntry;
  }

  private boolean reachedEnd(String logEntry) {
    return logEntry == null;
  }

  private void reopen() {
    IOUtils.closeQuietly(sourceFileReader);
    open();
  }

  private String read() {
    try {
      return sourceFileReader.readLine();
    } catch (IOException e) {
      throw new RuntimeException("Could not read from file " + sourceFileName, e);
    }
  }

  private void open() {
    try {
      sourceFileReader = new BufferedReader(new FileReader(sourceFileName));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error while opening file " + sourceFileName, e);
    }
  }

}
