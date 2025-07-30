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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.translation.streaming.ParDoStateUpdateFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.Tuple2;

/** Tests for {@link AbstractInOutIterator}. */
public class AbstractInOutIteratorTest {

  @Mock private SparkProcessContext<String, Integer, String> mockContext;
  @Mock private DoFnRunner<Integer, String> mockDoFnRunner;
  @Mock private TimerInternals.TimerData mockTimer;
  @Mock private BoundedWindow mockWindow;
  @Mock private ParDoStateUpdateFn.SparkTimerInternalsIterator mockTimerDataIterator;

  private static final String TEST_KEY = "testKey";
  private static final String TIMER_ID = "testTimerId";
  private static final String TIMER_FAMILY_ID = "testTimerFamilyId";
  private static final Instant TEST_TIMESTAMP = new Instant(42L);
  private static final Instant TEST_OUTPUT_TIMESTAMP = new Instant(84L);
  private static final TimeDomain TEST_TIME_DOMAIN = TimeDomain.EVENT_TIME;

  private StateNamespace testNamespace;

  /** Test implementation of {@link AbstractInOutIterator}. */
  private static class TestAbstractInOutIterator<K, InputT, OutputT>
      extends AbstractInOutIterator<K, InputT, OutputT> {

    protected TestAbstractInOutIterator(SparkProcessContext<K, InputT, OutputT> ctx) {
      super(ctx);
    }

    @Override
    protected Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {
      // Simple implementation as this method is not used in the test
      return this.endOfData();
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    testNamespace = StateNamespaces.window((Coder) IntervalWindow.getCoder(), mockWindow);

    when(mockContext.getDoFnRunner()).thenReturn(mockDoFnRunner);
    when(mockContext.getKey()).thenReturn(TEST_KEY);
    when(mockTimer.getTimerId()).thenReturn(TIMER_ID);
    when(mockTimer.getTimerFamilyId()).thenReturn(TIMER_FAMILY_ID);
    when(mockTimer.getNamespace()).thenReturn(testNamespace);
    when(mockTimer.getTimestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockTimer.getOutputTimestamp()).thenReturn(TEST_OUTPUT_TIMESTAMP);
    when(mockTimer.getDomain()).thenReturn(TEST_TIME_DOMAIN);
  }

  @Test
  public void testFireTimer() {
    // Test basic timer functionality
    TestAbstractInOutIterator<String, Integer, String> iterator =
        new TestAbstractInOutIterator<>(mockContext);

    iterator.fireTimer(mockTimer);

    // Verify that DoFnRunner.onTimer was called with the correct arguments
    verify(mockDoFnRunner)
        .onTimer(
            TIMER_ID,
            TIMER_FAMILY_ID,
            TEST_KEY,
            mockWindow,
            TEST_TIMESTAMP,
            TEST_OUTPUT_TIMESTAMP,
            TEST_TIME_DOMAIN);

    // Verify that timer data iterator deletion was not called (no timer iterator was set in this
    // test)
    verify(mockTimerDataIterator, never()).deleteTimer(mockTimer);
  }

  @Test
  public void testFireTimerWithTimerDataIterator() {
    // Test when timer data iterator is available
    when(mockContext.getTimerDataIterator()).thenReturn(mockTimerDataIterator);

    TestAbstractInOutIterator<String, Integer, String> iterator =
        new TestAbstractInOutIterator<>(mockContext);

    iterator.fireTimer(mockTimer);

    // Verify that DoFnRunner.onTimer was called with the correct arguments
    verify(mockDoFnRunner)
        .onTimer(
            TIMER_ID,
            TIMER_FAMILY_ID,
            TEST_KEY,
            mockWindow,
            TEST_TIMESTAMP,
            TEST_OUTPUT_TIMESTAMP,
            TEST_TIME_DOMAIN);

    // Verify that the timer data iterator's deleteTimer method was called
    verify(mockTimerDataIterator).deleteTimer(mockTimer);
  }

  @Test
  public void testFireTimerWithInvalidNamespace() {
    // Not WindowNamespace
    StateNamespace invalidNamespace = mock(StateNamespace.class);
    TimerInternals.TimerData invalidTimer = mock(TimerInternals.TimerData.class);
    when(invalidTimer.getNamespace()).thenReturn(invalidNamespace);

    TestAbstractInOutIterator<String, Integer, String> iterator =
        new TestAbstractInOutIterator<>(mockContext);

    assertThrows(IllegalArgumentException.class, () -> iterator.fireTimer(invalidTimer));
  }
}
