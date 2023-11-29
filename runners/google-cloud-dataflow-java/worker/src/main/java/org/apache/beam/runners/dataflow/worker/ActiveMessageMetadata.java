package org.apache.beam.runners.dataflow.worker;

public class ActiveMessageMetadata {

  public String userStepName;
  public Long startTime;

  public ActiveMessageMetadata(String userStepName, Long startTime) {
    this.userStepName = userStepName;
    this.startTime = startTime;
  }
}