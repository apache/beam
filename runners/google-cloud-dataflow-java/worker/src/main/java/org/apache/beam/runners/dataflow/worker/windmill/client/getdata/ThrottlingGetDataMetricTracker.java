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
 * Wraps GetData calls that tracks metrics for the number of in-flight requests and throttles
 * requests when memory pressure is high.
 */
@Internal
@ThreadSafe
public final class ThrottlingGetDataMetricTracker {
  private final MemoryMonitor gcThrashingMonitor;
  private final GetDataMetrics getDataMetrics;

  public ThrottlingGetDataMetricTracker(MemoryMonitor gcThrashingMonitor) {
    this.gcThrashingMonitor = gcThrashingMonitor;
    this.getDataMetrics = GetDataMetrics.create();
  }

  /**
   * Tracks a GetData call. If there is memory pressure, may throttle requests. Returns an {@link
   * AutoCloseable} that will decrement the metric after the call is finished.
   */
  public AutoCloseable trackSingleCallWithThrottling(Type callType) {
    gcThrashingMonitor.waitForResources(callType.debugName);
    AtomicInteger getDataMetricTracker = getDataMetrics.getMetricFor(callType);
    getDataMetricTracker.getAndIncrement();
    return getDataMetricTracker::getAndDecrement;
  }

  /**
   * Tracks heartbeat request metrics. Returns an {@link AutoCloseable} that will decrement the
   * metric after the call is finished.
   */
  public AutoCloseable trackHeartbeats(int numHeartbeats) {
    getDataMetrics
        .activeHeartbeats()
        .getAndUpdate(currentActiveHeartbeats -> currentActiveHeartbeats + numHeartbeats);
    return () ->
        getDataMetrics.activeHeartbeats().getAndUpdate(existing -> existing - numHeartbeats);
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    getDataMetrics.printMetrics(writer);
  }

  @VisibleForTesting
  GetDataMetrics.ReadOnlySnapshot getMetricsSnapshot() {
    return getDataMetrics.snapshot();
  }

  public enum Type {
    STATE("GetStateData"),
    SIDE_INPUT("GetSideInputData"),
    HEARTBEAT("RefreshActiveWork");
    private final String debugName;

    Type(String debugName) {
      this.debugName = debugName;
    }

    public final String debugName() {
      return debugName;
    }
  }

  @AutoValue
  abstract static class GetDataMetrics {
    private static GetDataMetrics create() {
      return new AutoValue_ThrottlingGetDataMetricTracker_GetDataMetrics(
          new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
    }

    abstract AtomicInteger activeSideInputs();

    abstract AtomicInteger activeStateReads();

    abstract AtomicInteger activeHeartbeats();

    private ReadOnlySnapshot snapshot() {
      return ReadOnlySnapshot.create(
          activeSideInputs().get(), activeStateReads().get(), activeHeartbeats().get());
    }

    private AtomicInteger getMetricFor(Type callType) {
      switch (callType) {
        case STATE:
          return activeStateReads();
        case SIDE_INPUT:
          return activeSideInputs();
        case HEARTBEAT:
          return activeHeartbeats();

        default:
          // Should never happen, switch is exhaustive.
          throw new IllegalStateException("Unsupported CallType=" + callType);
      }
    }

    private void printMetrics(PrintWriter writer) {
      writer.println("  Side Inputs: " + activeSideInputs().get());
      writer.println("  State Reads: " + activeStateReads().get());
      writer.println("Heartbeat Keys Active: " + activeHeartbeats().get());
    }

    @AutoValue
    abstract static class ReadOnlySnapshot {

      private static ReadOnlySnapshot create(
          int activeSideInputs, int activeStateReads, int activeHeartbeats) {
        return new AutoValue_ThrottlingGetDataMetricTracker_GetDataMetrics_ReadOnlySnapshot(
            activeSideInputs, activeStateReads, activeHeartbeats);
      }

      abstract int activeSideInputs();

      abstract int activeStateReads();

      abstract int activeHeartbeats();
    }
  }
}
