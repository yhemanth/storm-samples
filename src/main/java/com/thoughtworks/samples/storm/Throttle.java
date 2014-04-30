package com.thoughtworks.samples.storm;

public class Throttle {
  public void throttle() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
