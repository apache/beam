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

import com.google.api.services.dataflow.model.CounterUpdate;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link OperationContext} for testing purposes. */
public class TestOperationContext extends DataflowOperationContext {

  /** ExecutionState for testing. */
  public static class TestDataflowExecutionState extends DataflowExecutionState {

    private long totalMillis = 0;

    public TestDataflowExecutionState(
        NameContext nameContext,
        String stateName,
        String requestingStepName,
        Integer sideInputIndex,
        MetricsContainer metricsContainer,
        ProfileScope profileScope) {
      super(
          nameContext,
          stateName,
          requestingStepName,
          sideInputIndex,
          metricsContainer,
          profileScope);
    }

    public TestDataflowExecutionState(NameContext nameContext, String stateName) {
      this(nameContext, stateName, null, null, null, NoopProfileScope.NOOP);
    }

    @Override
    public void takeSample(long millisSinceLastSample) {
      totalMillis += millisSinceLastSample;
    }

    public long getTotalMillis() {
      return totalMillis;
    }

    @Override
    public void reportLull(Thread trackedThread, long millis) {}

    @Nullable
    @Override
    public CounterUpdate extractUpdate(boolean isFinalUpdate) {
      return null;
    }
  }

  /** DataflowExecutionStateRegistry for testing. */
  public static class TestExecutionStateRegistry extends DataflowExecutionStateRegistry {

    @Override
    protected DataflowExecutionState createState(
        NameContext nameContext,
        String stateName,
        String requestingStepName,
        Integer sideInputIndex,
        MetricsContainer container,
        ProfileScope profileScope) {
      return new TestDataflowExecutionState(
          nameContext, stateName, requestingStepName, sideInputIndex, container, profileScope);
    }
  }

  private final MetricsContainerImpl metricsContainer;
  private final CounterSet counterSet;

  private TestOperationContext(
      CounterSet counterSet,
      NameContext nameContext,
      MetricsContainerImpl metricsContainer,
      ExecutionStateTracker executionStateTracker) {
    super(
        counterSet,
        nameContext,
        metricsContainer,
        executionStateTracker,
        new TestExecutionStateRegistry());
    this.metricsContainer = metricsContainer;
    this.counterSet = counterSet;
  }

  public static TestOperationContext create(
      CounterSet counterSet,
      NameContext nameContext,
      MetricsContainerImpl metricsContainer,
      ExecutionStateTracker executionStateTracker) {
    return new TestOperationContext(
        counterSet, nameContext, metricsContainer, executionStateTracker);
  }

  public static TestOperationContext create() {
    CounterSet counterSet = new CounterSet();
    NameContext nameContext = NameContextsForTests.nameContextForTest();
    return create(counterSet, nameContext);
  }

  public static TestOperationContext create(CounterSet counterSet) {
    return new TestOperationContext(
        counterSet,
        NameContextsForTests.nameContextForTest(),
        new MetricsContainerImpl(NameContextsForTests.SYSTEM_NAME),
        ExecutionStateTracker.newForTest());
  }

  public static TestOperationContext create(CounterSet counterSet, NameContext nameContext) {
    return new TestOperationContext(
        counterSet,
        nameContext,
        new MetricsContainerImpl(nameContext.systemName()),
        ExecutionStateTracker.newForTest());
  }

  @Override
  public MetricsContainerImpl metricsContainer() {
    return metricsContainer;
  }

  public CounterSet counterSet() {
    return counterSet;
  }

  public ExecutionState getStartState() {
    return newExecutionState(ExecutionStateTracker.START_STATE_NAME);
  }

  public ExecutionState getProcessState() {
    return newExecutionState(ExecutionStateTracker.PROCESS_STATE_NAME);
  }

  public ExecutionState getFinishState() {
    return newExecutionState(ExecutionStateTracker.FINISH_STATE_NAME);
  }

  public ExecutionState getAbortState() {
    return newExecutionState(ExecutionStateTracker.ABORT_STATE_NAME);
  }
}
