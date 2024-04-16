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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTimeUtils.MillisProvider;

public final class DataflowExecutionStateSampler extends ExecutionStateSampler {

  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;
  private static final DataflowExecutionStateSampler INSTANCE =
      new DataflowExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  private final ConcurrentHashMap<String, DataflowExecutionStateTracker> activeTrackersByWorkId =
      new ConcurrentHashMap<>();
  // The maps within completeProcessingMetrics should not be modified.
  private final ConcurrentHashMap<String, Map<String, IntSummaryStatistics>>
      completedProcessingMetrics = new ConcurrentHashMap<>();

  public static DataflowExecutionStateSampler instance() {
    return INSTANCE;
  }

  @VisibleForTesting
  public static DataflowExecutionStateSampler newForTest(MillisProvider clock) {
    return new DataflowExecutionStateSampler(checkNotNull(clock));
  }

  public DataflowExecutionStateSampler(MillisProvider clock) {
    super(clock);
  }

  @Override
  public void addTracker(ExecutionStateTracker tracker) {
    if (!(tracker instanceof DataflowExecutionStateTracker)) {
      return;
    }
    DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    this.activeTrackersByWorkId.put(dfTracker.getWorkItemId(), dfTracker);
  }

  private static void mergeStepStatsMaps(
      Map<String, IntSummaryStatistics> map1, Map<String, IntSummaryStatistics> map2) {
    for (Entry<String, IntSummaryStatistics> steps : map2.entrySet()) {
      map1.compute(
          steps.getKey(),
          (k, v) -> {
            if (v == null) {
              return steps.getValue();
            }
            v.combine(steps.getValue());
            return v;
          });
    }
  }

  @Override
  public void removeTracker(ExecutionStateTracker tracker) {
    if (!(tracker instanceof DataflowExecutionContext.DataflowExecutionStateTracker)) {
      return;
    }
    DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    completedProcessingMetrics.put(
        dfTracker.getWorkItemId(), dfTracker.getProcessingTimesByStepCopy());
    activeTrackersByWorkId.remove(dfTracker.getWorkItemId());

    // Attribute any remaining time since the last sampling while removing the tracker.
    //
    // There is a race condition here; if sampling happens in the time between when we remove the
    // tracker from activeTrackers and read the lastSampleTicks value, the sampling time will
    // be lost for the tracker being removed. This is acceptable as sampling is already an
    // approximation of actual execution time.
    long millisSinceLastSample = clock.getMillis() - this.lastSampleTimeMillis;
    if (millisSinceLastSample > 0) {
      tracker.takeSample(millisSinceLastSample);
    }
  }

  @Override
  public void doSampling(long millisSinceLastSample) {
    for (DataflowExecutionStateTracker tracker : activeTrackersByWorkId.values()) {
      tracker.takeSample(millisSinceLastSample);
    }
  }

  public Optional<ActiveMessageMetadata> getActiveMessageMetadataForWorkId(String workId) {
    if (activeTrackersByWorkId.containsKey(workId)) {
      return Optional.ofNullable(
          activeTrackersByWorkId.get(workId).getActiveMessageMetadata().orElse(null));
    }
    return Optional.ofNullable(null);
  }

  public Map<String, IntSummaryStatistics> getProcessingDistributionsForWorkId(String workId) {
    Map<String, IntSummaryStatistics> result;
    DataflowExecutionStateTracker tracker = activeTrackersByWorkId.get(workId);
    if (tracker == null) {
      result = new HashMap<>();
    } else {
      result = tracker.getProcessingTimesByStepCopy();
    }
    mergeStepStatsMaps(result, completedProcessingMetrics.getOrDefault(workId, new HashMap<>()));
    return result;
  }

  public void resetForWorkId(String workId) {
    completedProcessingMetrics.remove(workId);
  }
}
