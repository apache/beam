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
package org.apache.beam.runners.dataflow.worker;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;

/**
 * An implementation of {@link MetricsContainer} that reads the current execution state (tracked in
 * a field) to determine the current step. This allows the {@link MetricsEnvironment} to only be
 * updated once on entry to the entire stage, rather than in between every step.
 */
// not clear why the interface extends Serializable
// https://issues.apache.org/jira/browse/BEAM-6573
@SuppressFBWarnings("SE_BAD_FIELD")
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class DataflowMetricsContainer implements MetricsContainer {

  private final ExecutionStateTracker executionStateTracker;

  public DataflowMetricsContainer(ExecutionStateTracker executionStateTracker) {
    this.executionStateTracker = executionStateTracker;
  }

  private MetricsContainer getCurrentContainer() {
    DataflowOperationContext.DataflowExecutionState executionState =
        (DataflowOperationContext.DataflowExecutionState) executionStateTracker.getCurrentState();
    return executionState.getMetricsContainer();
  }

  @Override
  public Counter getCounter(MetricName metricName) {
    return getCurrentContainer().getCounter(metricName);
  }

  @Override
  public Distribution getDistribution(MetricName metricName) {
    return getCurrentContainer().getDistribution(metricName);
  }

  @Override
  public Gauge getGauge(MetricName metricName) {
    return getCurrentContainer().getGauge(metricName);
  }
}
