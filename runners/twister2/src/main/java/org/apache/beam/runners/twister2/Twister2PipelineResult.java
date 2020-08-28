/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.twister2;

import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.mortbay.log.Log;

/** Represents a Twister2 pipeline execution result. */
public class Twister2PipelineResult implements PipelineResult {

  PipelineResult.State state;

  public Twister2PipelineResult(Twister2JobState jobState) {
    state = mapToState(jobState);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    Log.debug(
        "Twister2 runner does not currently support wait with duration"
            + "default waitUntilFinish will be executed");
    return waitUntilFinish();
  }

  @Override
  public State waitUntilFinish() {
    return state;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public void setState(State state) {
    this.state = state;
  }

  /**
   * Map Twister2 job state to Beam result State.
   *
   * @param jobState twister2 job state
   * @return corresponding Beam result state
   */
  private State mapToState(Twister2JobState jobState) {
    switch (jobState.getJobstate()) {
      case RUNNING:
        return State.RUNNING;
      case COMPLETED:
        return State.DONE;
      case FAILED:
        return State.FAILED;
      default:
        return State.FAILED;
    }
  }
}
