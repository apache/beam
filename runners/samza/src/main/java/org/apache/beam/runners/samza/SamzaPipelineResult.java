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

package org.apache.beam.runners.samza;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The result from executing a Samza Pipeline.
 */
public class SamzaPipelineResult implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineResult.class);

  private final SamzaExecutionContext executionContext;
  private final ApplicationRunner runner;
  private final StreamApplication app;

  public SamzaPipelineResult(StreamApplication app,
                             ApplicationRunner runner,
                             SamzaExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.runner = runner;
    this.app = app;
  }

  @Override
  public State getState() {
    return getStateInfo().state;
  }

  @Override
  public State cancel() throws IOException {
    runner.kill(app);

    //TODO: runner.waitForFinish() after SAMZA-1653 done
    return getState();
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    //TODO: SAMZA-1653
    throw new UnsupportedOperationException(
        "waitUntilFinish(duration) is not supported by the SamzaRunner");
  }

  @Override
  public State waitUntilFinish() {
    if (runner instanceof LocalApplicationRunner) {
      try {
        ((LocalApplicationRunner) runner).waitForFinish();
      } catch (Exception e) {
        throw new Pipeline.PipelineExecutionException(e);
      }

      final StateInfo stateInfo = getStateInfo();
      if (stateInfo.state == State.FAILED) {
        throw stateInfo.error;
      }

      return stateInfo.state;
    } else {
      // TODO: SAMZA-1653 support waitForFinish in remote runner too
      throw new UnsupportedOperationException(
          "waitUntilFinish is not supported by the SamzaRunner when running remotely");
    }
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(executionContext.getMetricsContainer().getContainers());
  }

  private StateInfo getStateInfo() {
    final ApplicationStatus status = runner.status(app);
    switch (status.getStatusCode()) {
      case New:
        return new StateInfo(State.STOPPED);
      case Running:
        return new StateInfo(State.RUNNING);
      case SuccessfulFinish:
        return new StateInfo(State.DONE);
      case UnsuccessfulFinish:
        LOG.error(status.getThrowable().getMessage(), status.getThrowable());
        return new StateInfo(State.FAILED,
            new Pipeline.PipelineExecutionException(status.getThrowable()));
      default:
        return new StateInfo(State.UNKNOWN);
    }
  }

  private static class StateInfo {
    private final State state;
    private final Pipeline.PipelineExecutionException error;

    private StateInfo(State state) {
      this(state, null);
    }

    private StateInfo(State state, Pipeline.PipelineExecutionException error) {
      this.state = state;
      this.error = error;
    }
  }
}
