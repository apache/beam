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
package org.apache.beam.runners.apex;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

/**
 * Result of executing a {@link Pipeline} with Apex in embedded mode.
 */
public class ApexRunnerResult implements PipelineResult {
  private final DAG apexDAG;
  private final LocalMode.Controller ctrl;
  private State state = State.UNKNOWN;

  public ApexRunnerResult(DAG dag, LocalMode.Controller ctrl) {
    this.apexDAG = dag;
    this.ctrl = ctrl;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    return null;
  }

  @Override
  public State cancel() throws IOException {
    ctrl.shutdown();
    state = State.CANCELLED;
    return state;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return ApexRunnerResult.waitUntilFinished(ctrl, duration);
  }

  @Override
  public State waitUntilFinish() {
    return ApexRunnerResult.waitUntilFinished(ctrl, null);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the DAG executed by the pipeline.
   * @return DAG from translation.
   */
  public DAG getApexDAG() {
    return apexDAG;
  }

  public static State waitUntilFinished(LocalMode.Controller ctrl, Duration duration) {
    // we need to rely on internal field for now
    // Apex should make it available through API in upcoming release.
    long timeout = (duration == null || duration.getMillis() < 1) ? Long.MAX_VALUE
        : System.currentTimeMillis() + duration.getMillis();
    Field appDoneField;
    try {
      appDoneField = ctrl.getClass().getDeclaredField("appDone");
      appDoneField.setAccessible(true);
      while (!appDoneField.getBoolean(ctrl) && System.currentTimeMillis() < timeout) {
        if (ApexRunner.ASSERTION_ERROR.get() != null) {
          throw ApexRunner.ASSERTION_ERROR.get();
        }
        Thread.sleep(500);
      }
      return appDoneField.getBoolean(ctrl) ? State.DONE : null;
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
