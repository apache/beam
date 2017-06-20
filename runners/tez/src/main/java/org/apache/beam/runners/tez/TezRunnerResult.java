package org.apache.beam.runners.tez;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.client.TezClient;
import org.joda.time.Duration;

/**
 * Result of executing a {@link org.apache.beam.sdk.Pipeline} with Tez.
 */
public class TezRunnerResult implements PipelineResult {

  private final TezClient client;
  private State state = State.UNKNOWN;

  public TezRunnerResult(TezClient client){
    this.client = client;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(null);
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    long timeout = (duration == null || duration.getMillis() < 1) ? Long.MAX_VALUE
        : System.currentTimeMillis() + duration.getMillis();
    try {
      while (client.getAppMasterStatus() != TezAppMasterStatus.SHUTDOWN && System.currentTimeMillis() < timeout) {
        Thread.sleep(500);
      }
      if (!client.getAppMasterStatus().equals(TezAppMasterStatus.SHUTDOWN)){
        return null;
      }
      return State.DONE;
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public State cancel() throws IOException {
    //TODO: CODE TO CANCEL PIPELINE
    return state;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }


}
