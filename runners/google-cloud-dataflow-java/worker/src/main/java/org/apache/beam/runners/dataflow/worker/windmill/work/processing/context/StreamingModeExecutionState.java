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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Execution states in Streaming are shared between multiple map-task executors. Thus this class
 * needs to be thread safe for multiple writers. A single stage could have multiple executors
 * running concurrently.
 */
public class StreamingModeExecutionState extends DataflowOperationContext.DataflowExecutionState {

  /**
   * AtomicLong is used because this value is written in two places: 1. The sampling thread calls
   * takeSample to increment the time spent in this state 2. The reporting thread calls
   * extractUpdate which reads the current sum *AND* sets it to 0.
   */
  private final AtomicLong totalMillisInState;

  public StreamingModeExecutionState(
      NameContext nameContext,
      String stateName,
      MetricsContainer metricsContainer,
      ScopedProfiler.ProfileScope profileScope) {
    // TODO: Take in the requesting step name and side input index for streaming.
    super(nameContext, stateName, null, null, metricsContainer, profileScope);
    totalMillisInState = new AtomicLong();
  }

  /**
   * Take sample is only called by the ExecutionStateSampler thread. It is the only place that
   * increments totalMillisInState, however the reporting thread periodically calls extractUpdate
   * which will read the sum and reset it to 0, so totalMillisInState does have multiple writers.
   */
  @Override
  public void takeSample(long millisSinceLastSample) {
    totalMillisInState.addAndGet(millisSinceLastSample);
  }

  /**
   * Extract updates in the form of a {@link CounterUpdate}.
   *
   * <p>Non-final updates are extracted periodically and report the physical value as a delta. This
   * requires setting the totalMillisInState back to 0.
   *
   * <p>Final updates should never be requested from a Streaming job since the work unit never
   * completes.
   */
  @Override
  public @Nullable CounterUpdate extractUpdate(boolean isFinalUpdate) {
    // Streaming reports deltas, so isFinalUpdate doesn't matter, and should never be true.
    long sum = totalMillisInState.getAndSet(0);
    return sum == 0 ? null : createUpdate(false, sum);
  }
}
