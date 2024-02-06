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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.THROTTLING_MSECS_METRIC_NAME;

import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics;
import org.apache.beam.runners.dataflow.worker.MetricsContainerRegistry;
import org.apache.beam.runners.dataflow.worker.StreamingDataflowWorker;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.StreamingStepMetricsContainer;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** Contains a few of the stage specific fields. E.g. metrics container registry, counters etc. */
@AutoValue
public abstract class StageInfo {
  public static StageInfo create(
      String stageName, String systemName, StreamingDataflowWorker worker) {
    NameContext nameContext = NameContext.create(stageName, null, systemName, null);
    CounterSet deltaCounters = new CounterSet();
    return new AutoValue_StageInfo(
        stageName,
        systemName,
        StreamingStepMetricsContainer.createRegistry(),
        new StreamingModeExecutionStateRegistry(worker),
        deltaCounters,
        deltaCounters.longSum(
            DataflowSystemMetrics.StreamingPerStageSystemCounterNames.THROTTLED_MSECS.counterName(
                nameContext)),
        deltaCounters.longSum(
            DataflowSystemMetrics.StreamingPerStageSystemCounterNames.TOTAL_PROCESSING_MSECS
                .counterName(nameContext)),
        deltaCounters.longSum(
            DataflowSystemMetrics.StreamingPerStageSystemCounterNames.TIMER_PROCESSING_MSECS
                .counterName(nameContext)));
  }

  public abstract String stageName();

  public abstract String systemName();

  public abstract MetricsContainerRegistry<StreamingStepMetricsContainer>
      metricsContainerRegistry();

  public abstract StreamingModeExecutionStateRegistry executionStateRegistry();

  public abstract CounterSet deltaCounters();

  public abstract Counter<Long, Long> throttledMsecs();

  public abstract Counter<Long, Long> totalProcessingMsecs();

  public abstract Counter<Long, Long> timerProcessingMsecs();

  public List<CounterUpdate> extractCounterUpdates() {
    List<CounterUpdate> counterUpdates = new ArrayList<>();
    Iterables.addAll(
        counterUpdates,
        StreamingStepMetricsContainer.extractMetricUpdates(metricsContainerRegistry()));
    Iterables.addAll(counterUpdates, executionStateRegistry().extractUpdates(false));
    for (CounterUpdate counterUpdate : counterUpdates) {
      translateKnownStepCounters(counterUpdate);
    }
    counterUpdates.addAll(
        deltaCounters().extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE));
    return counterUpdates;
  }

  /**
   * Checks if the step counter affects any per-stage counters. Currently 'throttled_millis' is the
   * only counter updated.
   */
  private void translateKnownStepCounters(CounterUpdate stepCounterUpdate) {
    CounterStructuredName structuredName =
        stepCounterUpdate.getStructuredNameAndMetadata().getName();
    if ((THROTTLING_MSECS_METRIC_NAME.getNamespace().equals(structuredName.getOriginNamespace())
            && THROTTLING_MSECS_METRIC_NAME.getName().equals(structuredName.getName()))
        || (StreamingDataflowWorker.BIGQUERY_STREAMING_INSERT_THROTTLE_TIME
                .getNamespace()
                .equals(structuredName.getOriginNamespace())
            && StreamingDataflowWorker.BIGQUERY_STREAMING_INSERT_THROTTLE_TIME
                .getName()
                .equals(structuredName.getName()))) {
      long msecs = DataflowCounterUpdateExtractor.splitIntToLong(stepCounterUpdate.getInteger());
      if (msecs > 0) {
        throttledMsecs().addValue(msecs);
      }
    }
  }

  public List<PerStepNamespaceMetrics> extractPerWorkerMetricValues() {
    List<PerStepNamespaceMetrics> metrics = new ArrayList<>();
    Iterables.addAll(
        metrics,
        StreamingStepMetricsContainer.extractPerWorkerMetricUpdates(metricsContainerRegistry()));
    return metrics;
  }
}
