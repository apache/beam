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
package org.apache.beam.runners.flink;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

/**
 * Result of executing a {@link org.apache.beam.sdk.Pipeline} with Flink. This
 * has methods to query to job runtime and the final values of
 * {@link org.apache.beam.sdk.transforms.Aggregator}s.
 */
public class FlinkRunnerResult implements PipelineResult {

  private final Map<String, Object> aggregators;

  private final long runtime;

  FlinkRunnerResult(Map<String, Object> aggregators, long runtime) {
    this.aggregators = (aggregators == null || aggregators.isEmpty())
        ? Collections.<String, Object>emptyMap()
        : Collections.unmodifiableMap(aggregators);
    this.runtime = runtime;
  }

  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    // TODO provide a list of all accumulator step values
    Object value = aggregators.get(aggregator.getName());
    if (value != null) {
      return new AggregatorValues<T>() {
        @Override
        public Map<String, T> getValuesAtSteps() {
          return (Map<String, T>) aggregators;
        }
      };
    } else {
      throw new AggregatorRetrievalException("Accumulator results not found.",
          new RuntimeException("Accumulator does not exist."));
    }
  }

  @Override
  public String toString() {
    return "FlinkRunnerResult{"
        + "aggregators=" + aggregators
        + ", runtime=" + runtime
        + '}';
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("FlinkRunnerResult does not support cancel.");
  }

  @Override
  public State waitUntilFinish() {
    return State.DONE;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return State.DONE;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("The FlinkRunner does not currently support metrics.");
  }
}
