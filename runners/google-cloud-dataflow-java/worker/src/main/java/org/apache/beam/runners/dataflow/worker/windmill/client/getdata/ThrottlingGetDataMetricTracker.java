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
package org.apache.beam.runners.dataflow.worker.windmill.client.getdata;

import com.google.auto.value.AutoValue;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Wraps GetData calls to track metrics for the number of in-flight requests and throttles requests
 * when memory pressure is high.
 */
@Internal
@ThreadSafe
public final class ThrottlingGetDataMetricTracker {
  private static final String GET_STATE_DATA_RESOURCE_CONTEXT = "GetStateData";
  private static final String GET_SIDE_INPUT_RESOURCE_CONTEXT = "GetSideInputData";

  private final MemoryMonitor gcThrashingMonitor;
  private final AtomicInteger activeStateReads;
  private final AtomicInteger activeSideInputs;
  private final AtomicInteger activeHeartbeats;

  public ThrottlingGetDataMetricTracker(MemoryMonitor gcThrashingMonitor) {
    this.gcThrashingMonitor = gcThrashingMonitor;
    this.activeStateReads = new AtomicInteger();
    this.activeSideInputs = new AtomicInteger();
    this.activeHeartbeats = new AtomicInteger();
  }

  /**
   * Tracks a state data fetch. If there is memory pressure, may throttle requests. Returns an
   * {@link AutoCloseable} that will decrement the metric after the call is finished.
   */
  AutoCloseable trackStateDataFetchWithThrottling() {
    gcThrashingMonitor.waitForResources(GET_STATE_DATA_RESOURCE_CONTEXT);
    activeStateReads.getAndIncrement();
    return activeStateReads::getAndDecrement;
  }

  /**
   * Tracks a side input fetch. If there is memory pressure, may throttle requests. Returns an
   * {@link AutoCloseable} that will decrement the metric after the call is finished.
   */
  AutoCloseable trackSideInputFetchWithThrottling() {
    gcThrashingMonitor.waitForResources(GET_SIDE_INPUT_RESOURCE_CONTEXT);
    activeSideInputs.getAndIncrement();
    return activeSideInputs::getAndDecrement;
  }

  /**
   * Tracks heartbeat request metrics. Returns an {@link AutoCloseable} that will decrement the
   * metric after the call is finished.
   */
  public AutoCloseable trackHeartbeats(int numHeartbeats) {
    activeHeartbeats.getAndAdd(numHeartbeats);
    return () -> activeHeartbeats.getAndAdd(-numHeartbeats);
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    writer.println("  Side Inputs: " + activeSideInputs.get());
    writer.println("  State Reads: " + activeStateReads.get());
    writer.println("Heartbeat Keys Active: " + activeHeartbeats.get());
  }

  @VisibleForTesting
  ReadOnlySnapshot getMetricsSnapshot() {
    return ReadOnlySnapshot.create(
        activeSideInputs.get(), activeStateReads.get(), activeHeartbeats.get());
  }

  @VisibleForTesting
  @AutoValue
  abstract static class ReadOnlySnapshot {

    private static ReadOnlySnapshot create(
        int activeSideInputs, int activeStateReads, int activeHeartbeats) {
      return new AutoValue_ThrottlingGetDataMetricTracker_ReadOnlySnapshot(
          activeSideInputs, activeStateReads, activeHeartbeats);
    }

    abstract int activeSideInputs();

    abstract int activeStateReads();

    abstract int activeHeartbeats();
  }
}
