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
  public State waitUntilFinish(Duration duration) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the DAG executed by the pipeline.
   * @return
   */
  public DAG getApexDAG() {
    return apexDAG;
  }

}
