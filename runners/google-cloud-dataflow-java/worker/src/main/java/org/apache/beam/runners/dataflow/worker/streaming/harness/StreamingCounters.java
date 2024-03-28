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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.StreamingSystemCounterNames;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.sdk.annotations.Internal;

/** Streaming pipeline counters to report pipeline processing metrics to Dataflow backend. */
@Internal
@AutoValue
public abstract class StreamingCounters {

  public static StreamingCounters create() {
    CounterSet pendingDeltaCounters = new CounterSet();
    CounterSet pendingCumulativeCounters = new CounterSet();
    return new AutoValue_StreamingCounters(
        pendingDeltaCounters,
        pendingCumulativeCounters,
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_SHUFFLE_BYTES_READ.counterName()),
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_STATE_BYTES_READ.counterName()),
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_STATE_BYTES_WRITTEN.counterName()),
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_QUOTA_THROTTLING.counterName()),
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.TIME_AT_MAX_ACTIVE_THREADS.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_USED_MEMORY.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_MAX_MEMORY.counterName()),
        pendingCumulativeCounters.intSum(StreamingSystemCounterNames.ACTIVE_THREADS.counterName()),
        pendingCumulativeCounters.intSum(
            StreamingSystemCounterNames.TOTAL_ALLOCATED_THREADS.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.OUTSTANDING_BYTES.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.MAX_OUTSTANDING_BYTES.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.OUTSTANDING_BUNDLES.counterName()),
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.MAX_OUTSTANDING_BUNDLES.counterName()),
        pendingCumulativeCounters.intMax(
            StreamingSystemCounterNames.WINDMILL_MAX_WORK_ITEM_COMMIT_BYTES.counterName()),
        pendingCumulativeCounters.intSum(
            StreamingSystemCounterNames.MEMORY_THRASHING.counterName()));
  }

  public abstract CounterSet pendingDeltaCounters();

  public abstract CounterSet pendingCumulativeCounters();
  // Built-in delta counters.
  public abstract Counter<Long, Long> windmillShuffleBytesRead();

  public abstract Counter<Long, Long> windmillStateBytesRead();

  public abstract Counter<Long, Long> windmillStateBytesWritten();

  public abstract Counter<Long, Long> windmillQuotaThrottling();

  public abstract Counter<Long, Long> timeAtMaxActiveThreads();
  // Built-in cumulative counters.
  public abstract Counter<Long, Long> javaHarnessUsedMemory();

  public abstract Counter<Long, Long> javaHarnessMaxMemory();

  public abstract Counter<Integer, Integer> activeThreads();

  public abstract Counter<Integer, Integer> totalAllocatedThreads();

  public abstract Counter<Long, Long> outstandingBytes();

  public abstract Counter<Long, Long> maxOutstandingBytes();

  public abstract Counter<Long, Long> outstandingBundles();

  public abstract Counter<Long, Long> maxOutstandingBundles();

  public abstract Counter<Integer, Integer> windmillMaxObservedWorkItemCommitBytes();

  public abstract Counter<Integer, Integer> memoryThrashing();
}
