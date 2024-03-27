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

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;
import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.ACCUMULATOR_NAME;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * Result of executing a {@link org.apache.beam.sdk.Pipeline} with Flink. This has methods to query
 * to job runtime and the final values of the accumulators.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkRunnerResult implements PipelineResult {

  private final Map<String, Object> accumulators;

  private final long runtime;

  FlinkRunnerResult(Map<String, Object> accumulators, long runtime) {
    this.accumulators =
        (accumulators == null || accumulators.isEmpty())
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(accumulators);
    this.runtime = runtime;
  }

  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public String toString() {
    return "FlinkRunnerResult{" + "accumulators=" + accumulators + ", runtime=" + runtime + '}';
  }

  @Override
  public State cancel() {
    // We can only be called here when we are done.
    return State.DONE;
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
    return asAttemptedOnlyMetricResults(getMetricsContainerStepMap());
  }

  MetricsContainerStepMap getMetricsContainerStepMap() {
    return (MetricsContainerStepMap) accumulators.get(ACCUMULATOR_NAME);
  }
}
