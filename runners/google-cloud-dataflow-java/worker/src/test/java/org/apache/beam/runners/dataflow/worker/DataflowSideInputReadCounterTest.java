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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowSideInputReadCounter}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class DataflowSideInputReadCounterTest {

  @Test
  public void testToStringReturnsWellFormedDescriptionString() {
    DataflowExecutionContext mockedExecutionContext = mock(DataflowExecutionContext.class);
    DataflowOperationContext mockedOperationContext = mock(DataflowOperationContext.class);
    final int siIndexId = 3;

    ExecutionStateTracker mockedExecutionStateTracker = mock(ExecutionStateTracker.class);
    when(mockedExecutionContext.getExecutionStateTracker()).thenReturn(mockedExecutionStateTracker);
    Thread mockedThreadObject = mock(Thread.class);
    when(mockedExecutionStateTracker.getTrackedThread()).thenReturn(mockedThreadObject);

    DataflowSideInputReadCounter testObject =
        new DataflowSideInputReadCounter(mockedExecutionContext, mockedOperationContext, siIndexId);

    assertThat(
        testObject.toString(),
        equalTo("DataflowSideInputReadCounter{sideInputIndex=3, declaringStep=null}"));
  }

  @Test
  public void testAddBytesReadSkipsAddingCountersIfCurrentCounterIsNull() {
    DataflowExecutionContext mockedExecutionContext = mock(DataflowExecutionContext.class);
    DataflowOperationContext mockedOperationContext = mock(DataflowOperationContext.class);
    final int siIndexId = 3;

    ExecutionStateTracker mockedExecutionStateTracker = mock(ExecutionStateTracker.class);
    when(mockedExecutionContext.getExecutionStateTracker()).thenReturn(mockedExecutionStateTracker);
    Thread mockedThreadObject = mock(Thread.class);
    when(mockedExecutionStateTracker.getTrackedThread()).thenReturn(mockedThreadObject);

    DataflowSideInputReadCounter testObject =
        new DataflowSideInputReadCounter(mockedExecutionContext, mockedOperationContext, siIndexId);

    try {
      testObject.addBytesRead(10);
    } catch (Exception e) {
      fail(
          "Supposedly, we tried to add bytes to counter and that shouldn't happen. Ex.: "
              + e.toString());
    }
  }

  @Test
  public void testAddBytesReadUpdatesCounter() {
    DataflowExecutionContext mockedExecutionContext = mock(DataflowExecutionContext.class);
    DataflowOperationContext mockedOperationContext = mock(DataflowOperationContext.class);
    final int siIndexId = 3;

    ExecutionStateTracker mockedExecutionStateTracker = mock(ExecutionStateTracker.class);
    when(mockedExecutionContext.getExecutionStateTracker()).thenReturn(mockedExecutionStateTracker);

    Thread mockedThreadObject = mock(Thread.class);
    when(mockedExecutionStateTracker.getTrackedThread()).thenReturn(mockedThreadObject);

    DataflowExecutionState mockedExecutionState = mock(DataflowExecutionState.class);
    when(mockedExecutionStateTracker.getCurrentState()).thenReturn(mockedExecutionState);

    NameContext mockedNameContext = mock(NameContext.class);
    when(mockedExecutionState.getStepName()).thenReturn(mockedNameContext);

    when(mockedNameContext.originalName()).thenReturn("DummyName");

    NameContext mockedDeclaringNameContext = mock(NameContext.class);
    when(mockedOperationContext.nameContext()).thenReturn(mockedDeclaringNameContext);

    when(mockedDeclaringNameContext.originalName()).thenReturn("DummyDeclaringName");

    CounterFactory mockedCounterFactory = mock(CounterFactory.class);
    when(mockedExecutionContext.getCounterFactory()).thenReturn(mockedCounterFactory);

    Counter<Long, Long> mockedCounter = mock(Counter.class);
    when(mockedCounterFactory.longSum(any())).thenReturn(mockedCounter);

    when(mockedExecutionContext.getExecutionStateRegistry())
        .thenReturn(mock(DataflowExecutionStateRegistry.class));

    DataflowSideInputReadCounter testObject =
        new DataflowSideInputReadCounter(mockedExecutionContext, mockedOperationContext, siIndexId);

    testObject.addBytesRead(10l);

    verify(mockedCounter).addValue(10l);
  }

  @Test
  public void testEnterEntersStateIfCalledFromTrackedThread() {
    DataflowExecutionContext mockedExecutionContext = mock(DataflowExecutionContext.class);
    DataflowOperationContext mockedOperationContext = mock(DataflowOperationContext.class);
    final int siIndexId = 3;

    ExecutionStateTracker mockedExecutionStateTracker = mock(ExecutionStateTracker.class);
    when(mockedExecutionContext.getExecutionStateTracker()).thenReturn(mockedExecutionStateTracker);

    Thread mockedThreadObject = Thread.currentThread();
    when(mockedExecutionStateTracker.getTrackedThread()).thenReturn(mockedThreadObject);

    DataflowExecutionState mockedExecutionState = mock(DataflowExecutionState.class);
    when(mockedExecutionStateTracker.getCurrentState()).thenReturn(mockedExecutionState);

    NameContext mockedNameContext = mock(NameContext.class);
    when(mockedExecutionState.getStepName()).thenReturn(mockedNameContext);

    when(mockedNameContext.originalName()).thenReturn("DummyName");

    NameContext mockedDeclaringNameContext = mock(NameContext.class);
    when(mockedOperationContext.nameContext()).thenReturn(mockedDeclaringNameContext);

    when(mockedDeclaringNameContext.originalName()).thenReturn("DummyDeclaringName");

    CounterFactory mockedCounterFactory = mock(CounterFactory.class);
    when(mockedExecutionContext.getCounterFactory()).thenReturn(mockedCounterFactory);

    Counter<Long, Long> mockedCounter = mock(Counter.class);
    when(mockedCounterFactory.longSum(any())).thenReturn(mockedCounter);

    DataflowExecutionStateRegistry mockedExecutionStateRegistry =
        mock(DataflowExecutionStateRegistry.class);
    when(mockedExecutionContext.getExecutionStateRegistry())
        .thenReturn(mockedExecutionStateRegistry);

    DataflowExecutionState mockedCounterExecutionState = mock(DataflowExecutionState.class);
    when(mockedExecutionStateRegistry.getIOState(any(), any(), any(), any(), any(), any()))
        .thenReturn(mockedCounterExecutionState);
    DataflowSideInputReadCounter testObject =
        new DataflowSideInputReadCounter(mockedExecutionContext, mockedOperationContext, siIndexId);

    testObject.enter();

    verify(mockedExecutionStateTracker).enterState(mockedCounterExecutionState);
  }

  @Test
  public void testEnterDoesntEnterStateIfCalledFromDifferentThread() {
    DataflowExecutionContext mockedExecutionContext = mock(DataflowExecutionContext.class);
    DataflowOperationContext mockedOperationContext = mock(DataflowOperationContext.class);
    final int siIndexId = 3;

    ExecutionStateTracker mockedExecutionStateTracker = mock(ExecutionStateTracker.class);
    when(mockedExecutionContext.getExecutionStateTracker()).thenReturn(mockedExecutionStateTracker);

    Thread mockedThreadObject = mock(Thread.class);
    when(mockedExecutionStateTracker.getTrackedThread()).thenReturn(mockedThreadObject);

    DataflowExecutionState mockedExecutionState = mock(DataflowExecutionState.class);
    when(mockedExecutionStateTracker.getCurrentState()).thenReturn(mockedExecutionState);

    NameContext mockedNameContext = mock(NameContext.class);
    when(mockedExecutionState.getStepName()).thenReturn(mockedNameContext);

    when(mockedNameContext.originalName()).thenReturn("DummyName");

    NameContext mockedDeclaringNameContext = mock(NameContext.class);
    when(mockedOperationContext.nameContext()).thenReturn(mockedDeclaringNameContext);

    when(mockedDeclaringNameContext.originalName()).thenReturn("DummyDeclaringName");

    CounterFactory mockedCounterFactory = mock(CounterFactory.class);
    when(mockedExecutionContext.getCounterFactory()).thenReturn(mockedCounterFactory);

    Counter<Long, Long> mockedCounter = mock(Counter.class);
    when(mockedCounterFactory.longSum(any())).thenReturn(mockedCounter);

    DataflowExecutionStateRegistry mockedExecutionStateRegistry =
        mock(DataflowExecutionStateRegistry.class);
    when(mockedExecutionContext.getExecutionStateRegistry())
        .thenReturn(mockedExecutionStateRegistry);

    DataflowExecutionState mockedCounterExecutionState = mock(DataflowExecutionState.class);
    when(mockedExecutionStateRegistry.getIOState(any(), any(), any(), any(), any(), any()))
        .thenReturn(mockedCounterExecutionState);
    DataflowSideInputReadCounter testObject =
        new DataflowSideInputReadCounter(mockedExecutionContext, mockedOperationContext, siIndexId);

    testObject.enter();

    verify(mockedExecutionStateTracker, never()).enterState(any());
  }
}
