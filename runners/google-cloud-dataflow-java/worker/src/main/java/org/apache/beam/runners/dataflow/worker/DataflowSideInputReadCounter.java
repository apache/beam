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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

/**
 * This class tracks time and bytes spent when consuming side inputs.
 *
 * <p>This class is designed to track consumption of side inputs across fused steps.
 *
 * <p>We represent a side input as a declaring step, and an input index. The declaring step is the
 * step that originally receives the side input for consumption, and the input index in which the
 * declaring step receives the side input that we want to identify. The declaring step originally
 * receives the side input, but it may not be the only step that spends time reading from this side
 * input. Therefore, to represent the actual consumption of a side input, it is necessary to use
 * three things: 1) the declaring step, 2) the input index, and 3) the currently executing step (as
 * it may be different from the declaring step).
 *
 * <p>The following table summarizes the two different steps tracked when it comes to side inputs:
 *
 * <table>
 *   <tr>
 *     <th>Side Input Read Counter tracks two steps:</th>
 *     <th>Declaring Step Name</th>
 *     <th>Requesting Step Name</th>
 *   </tr>
 *   <tr>
 *     <td>The value of these can vary because:</td>
 *     <td>IsmReader instances are shared between multiple threads for the same PCollectionView</td>
 *     <td>The same Lazy Iterable can be passed to and consumed by downstream steps as an output
 *     element.</td>
 *   </tr>
 *   <tr>
 *     <td>The current value is tracked by:</td>
 *     <td>DelegatingIsmReader and DataflowSideInputReadCounter</td>
 *     <td>The ExecutionStateTracker.</td>
 *   </tr>
 *   <tr>
 *     <td>The current value is read by:</td>
 *     <td>IsmReader and IsmPrefixReaderIterator</td>
 *     <td>DataflowSideInputReadCounter, in the checkStep function.</td>
 *   </tr>
 * </table>
 *
 * <p>This is the case for both batch pipelines, and streaming pipelines, although the underlying
 * storage layers are different (GCS for Batch, Windmill state for Streaming).
 *
 * <p>As an example of a pipeline where the declaring step and the consuming step of a side input
 * are not the same, suppose a pipeline of the following form:
 *
 * <p>SideInputPCollection -> View.AsIterable ------------------ | v MainInputPCollection --->
 * FirstParDo(...).withSideInput() -> AnotherParDo(...)
 *
 * <p>In this pipeline, the FirstParDo transform may simply emit the Iterable provided by the side
 * input, and the AnotherParDo may be the one that actually manipulates that Iterable. This is
 * possible because both ParDos will be fused, so they will simply exchange objects in memory.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataflowSideInputReadCounter implements SideInputReadCounter {
  private final DataflowExecutionContext executionContext;

  /** These two attributes describe the side input via a declaring step name, and an input index. */
  private final DataflowOperationContext declaringOperationContext;

  private final int sideInputIndex;

  /** Maps containing the byte counters, and execution states associated to different steps. */
  private final Map<NameContext, Counter<Long, Long>> byteCounters;

  private final Map<NameContext, DataflowExecutionState> executionStates;

  /**
   * {@link Counter}, {@link DataflowExecutionState}, and {@link NameContext} associated to the
   * latest step to have consumed the side input.
   */
  private Counter<Long, Long> currentCounter;

  private DataflowExecutionState currentExecutionState;
  private NameContext latestConsumingStepName;

  public DataflowSideInputReadCounter(
      DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext,
      int sideInputIndex) {
    this.executionContext = executionContext;
    this.sideInputIndex = sideInputIndex;
    this.declaringOperationContext = operationContext;
    byteCounters = new HashMap<>();
    executionStates = new HashMap<>();
    updateCurrentStateIfOutdated();
  }

  private void updateCurrentStateIfOutdated() {
    DataflowExecutionState currentState =
        (DataflowExecutionState) executionContext.getExecutionStateTracker().getCurrentState();
    if (currentState == null
        || currentState.getStepName().originalName() == null
        || Objects.equals(latestConsumingStepName, currentState.getStepName())) {
      // In this case, the step has not changed, and we'll just return.
      return;
    }
    if (!byteCounters.containsKey(currentState.getStepName())) {
      byteCounters.put(
          currentState.getStepName(),
          executionContext
              .getCounterFactory()
              .longSum(
                  CounterName.named("read-sideinput-byte-count")
                      .withOriginalName(declaringOperationContext.nameContext())
                      .withOrigin("SYSTEM")
                      .withOriginalRequestingStepName(currentState.getStepName().originalName())
                      .withInputIndex(sideInputIndex)));

      executionStates.put(
          currentState.getStepName(),
          executionContext
              .getExecutionStateRegistry()
              .getIOState(
                  declaringOperationContext.nameContext(),
                  "read-sideinput",
                  currentState.getStepName().originalName(),
                  sideInputIndex,
                  currentState.getMetricsContainer(),
                  currentState.getProfileScope()));
    }
    currentCounter = byteCounters.get(currentState.getStepName());
    currentExecutionState = executionStates.get(currentState.getStepName());
    latestConsumingStepName = currentState.getStepName();
  }

  @Override
  public void addBytesRead(long n) {
    if (currentCounter != null) {
      currentCounter.addValue(n);
    }
  }

  @Override
  public Closeable enter() {
    // Only update status from tracked thread to avoid race condition and inconsistent state updates
    if (executionContext.getExecutionStateTracker().getTrackedThread() != Thread.currentThread()) {
      return () -> {};
    }
    updateCurrentStateIfOutdated();
    return executionContext.getExecutionStateTracker().enterState(currentExecutionState);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sideInputIndex", sideInputIndex)
        .add("declaringStep", declaringOperationContext.nameContext())
        .toString();
  }
}
