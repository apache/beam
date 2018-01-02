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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The result from executing a Samza Pipeline.
 */
public class SamzaPipelineResult implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineResult.class);

  private final CountDownLatch doneLatch = new CountDownLatch(1);
  private final AtomicReference<StateInfo> stateRef =
      new AtomicReference<>(new StateInfo(State.STOPPED));
  private final SamzaExecutionContext executionContext;

  public SamzaPipelineResult(SamzaExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  @Override
  public State getState() {
    return stateRef.get().state;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("Cancellation is not supported by the SamzaRunner");
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    try {
      if (!doneLatch.await(duration.getMillis(), TimeUnit.MILLISECONDS)) {
        return null;
      }
    } catch (InterruptedException e) {
      // Ignore
    }

    final StateInfo stateInfo = stateRef.get();
    if (stateInfo.error != null) {
      throw stateInfo.error;
    }

    return stateInfo.state;
  }

  @Override
  public State waitUntilFinish() {
    try {
      doneLatch.await();
    } catch (InterruptedException e) {
      // Ignore
    }

    final StateInfo stateInfo = stateRef.get();
    if (stateInfo.error != null) {
      throw stateInfo.error;
    }

    return stateInfo.state;
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(executionContext.getMetricsContainer().getContainers());
  }

  public void markStarted() {
    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.STOPPED) {
        LOG.warn(
            "Invalid state transition from {} to RUNNING. "
                + "Only valid transition is from STOPPED. Ignoring.",
            currentState.state);
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.RUNNING)));
  }

  public void markSuccess() {
    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.RUNNING) {
        LOG.warn(
            "Invalid state transition from {} to DONE. "
                + "Only valid transition is from RUNNING. Ignoring. ",
            currentState.state);
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.DONE)));

    doneLatch.countDown();
  }

  public void markFailure(Throwable error) {
    // TODO: do we need to unwrap error to find UserCodeException?
    final Pipeline.PipelineExecutionException wrappedException =
        new Pipeline.PipelineExecutionException(error);

    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.RUNNING) {
        LOG.warn(
            "Invalid state transition from {} to FAILED. "
                + "Only valid transition is from RUNNING. Ignoring. ",
            currentState.state);
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.FAILED, wrappedException)));

    doneLatch.countDown();
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
