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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates.notNull;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Manages the instances of {@link ExecutionState} */
public abstract class DataflowExecutionStateRegistry {

  /**
   * Generally, the execution states should be created when the MapTask is created, so this doesn't
   * need to be concurrently accessed. In fact, it could be immutable after the creation of the
   * MapTask. But, since it is theoretically possible for new states to be created, and the
   * execution sampler may be reading this at the same time, we use a concurrent map for safety.
   */
  private final Map<DataflowExecutionStateKey, DataflowOperationContext.DataflowExecutionState>
      createdStates = new ConcurrentSkipListMap<>();

  /**
   * Get an existing state or create a {@link DataflowOperationContext.DataflowExecutionState} that
   * represents time spent within a step.
   */
  public DataflowOperationContext.DataflowExecutionState getState(
      final NameContext nameContext,
      final String stateName,
      final MetricsContainer container,
      final ProfileScope profileScope) {
    return getStateInternal(nameContext, stateName, null, null, container, profileScope);
  }

  /**
   * Get an existing state or create a {@link DataflowOperationContext.DataflowExecutionState} that
   * represents the consumption of some kind of IO, such as reading of Side Input, or Shuffle data.
   *
   * <p>An IO-related ExcecutionState may represent: * A Side Input collection as declaringStep +
   * inputIndex. The consumption of the side input is represented by (declaringStep, inputIndex,
   * requestingStepName), where requestingStepName is the step that causes the IO to occur. * A
   * Shuffle IO as the GBK step for that shuffle. The consumption of the side input is represented
   * by (declaringStep, requestingStepName), where requestingStepName is the step that causes the IO
   * to occur.
   */
  public DataflowOperationContext.DataflowExecutionState getIOState(
      final NameContext nameContext,
      final String stateName,
      final String requestingStepName,
      final @Nullable Integer inputIndex,
      final @Nullable MetricsContainer container,
      final ProfileScope profileScope) {
    return getStateInternal(
        nameContext, stateName, requestingStepName, inputIndex, container, profileScope);
  }

  private DataflowOperationContext.DataflowExecutionState getStateInternal(
      final NameContext nameContext,
      final String stateName,
      final @Nullable String requestingStepName,
      final @Nullable Integer inputIndex,
      final @Nullable MetricsContainer container,
      final ProfileScope profileScope) {
    DataflowExecutionStateKey stateKey =
        DataflowExecutionStateKey.create(nameContext, stateName, requestingStepName, inputIndex);
    return createdStates.computeIfAbsent(
        stateKey,
        unused ->
            createState(
                nameContext, stateName, requestingStepName, inputIndex, container, profileScope));
  }

  /**
   * Internal method to create and register an ExecutionState.
   *
   * <p>Do not call this method directly. Instead, use the {@link
   * DataflowExecutionStateRegistry#getState}, and {@link DataflowExecutionStateRegistry#getIOState}
   * public methods, or implement your own public create method.
   */
  protected abstract DataflowOperationContext.DataflowExecutionState createState(
      NameContext nameContext,
      String stateName,
      @Nullable String requestingStepName,
      @Nullable Integer inputIndex,
      @Nullable MetricsContainer container,
      ProfileScope profileScope);

  public Iterable<CounterUpdate> extractUpdates(boolean isFinalUpdate) {
    return FluentIterable.from(createdStates.values())
        .transform(state -> state.extractUpdate(isFinalUpdate))
        .filter(notNull());
  }
}
