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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.base.MoreObjects;
import java.util.Collections;
import org.apache.beam.runners.core.BaseExecutionContext.StepContext;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link StatefulDoFnRunnerTest}. */
@RunWith(JUnit4.class)
public class StatefulDoFnRunnerTest {

  @Mock StepContext mockStepContext;

  @Mock TimerInternals mockTimerInternals;

  private InMemoryLongSumAggregator droppedDueToLateness;
  private AggregatorFactory aggregatorFactory;
  private WindowingStrategy<?, ?> windowingStrategy;
  private InMemoryStateInternals<String> stateInternals;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockStepContext.timerInternals()).thenReturn(mockTimerInternals);
    droppedDueToLateness = new InMemoryLongSumAggregator("droppedDueToLateness");

    aggregatorFactory = new AggregatorFactory() {
      @Override
      public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
          Class<?> fnClass, ExecutionContext.StepContext stepContext, String aggregatorName,
          Combine.CombineFn<InputT, AccumT, OutputT> combine) {
        return (Aggregator<InputT, OutputT>) droppedDueToLateness;
      }
    };
    windowingStrategy = WindowingStrategy.of(new GlobalWindows());
    stateInternals = new InMemoryStateInternals<>("hello");
    when(mockStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);
  }

  @Test
  public void testLateDataDroping() {
    when(mockTimerInternals.currentInputWatermarkTime())
        .thenReturn(BoundedWindow.TIMESTAMP_MAX_VALUE);

    DoFn<KV<String, Integer>, Integer> fn = new MyDoFn();

    DoFnRunner<KV<String, Integer>, Integer> runner = DoFnRunners.statefulDoFnRunner(
        fn, getSimpleDoFnRunner(fn), mockStepContext, aggregatorFactory, windowingStrategy);

    runner.startBundle();

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), Instant.now(),
            new IntervalWindow(new Instant(0), new Instant(10)), PaneInfo.NO_FIRING));

    assertEquals(1L, droppedDueToLateness.sum);

    runner.finishBundle();
  }

  @Test
  public void testGc() {
    when(mockTimerInternals.currentInputWatermarkTime())
        .thenReturn(new Instant(1));
    DoFn<KV<String, Integer>, Integer> fn = new MyDoFn();

    DoFnRunner<KV<String, Integer>, Integer> runner = DoFnRunners.statefulDoFnRunner(
        fn, getSimpleDoFnRunner(fn), mockStepContext, aggregatorFactory, windowingStrategy);

    IntervalWindow intervalWindow = new IntervalWindow(new Instant(0), new Instant(10));
    Instant elementTime = new Instant(1);

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), elementTime, intervalWindow, PaneInfo.NO_FIRING));

    InMemoryStateInternals.InMemoryValue<Integer> valueState
        = (InMemoryStateInternals.InMemoryValue<Integer>) stateInternals
        .inMemoryState.values().iterator().next();

    assertEquals(new Integer(1), valueState.read());

    runner.onTimer(StatefulDoFnRunner.GC_TIMER_ID, intervalWindow,
        elementTime, TimeDomain.EVENT_TIME);

    assertTrue(valueState.isCleared());

  }

  private SimpleDoFnRunner<KV<String, Integer>, Integer> getSimpleDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn) {
    return new SimpleDoFnRunner<>(
        null,
        fn,
        NullSideInputReader.empty(),
        null,
        null,
        Collections.<TupleTag<?>>emptyList(),
        mockStepContext,
        null,
        windowingStrategy);
  }

  private static class MyDoFn extends DoFn<KV<String, Integer>, Integer> {

    final String stateId = "foo";

    @StateId(stateId)
    private final StateSpec<Object, ValueState<Integer>> intState =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext c, @StateId(stateId) ValueState<Integer> state) {
      Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
      state.write(currentValue + 1);
    }
  };

  private static class InMemoryLongSumAggregator implements Aggregator<Long, Long> {
    private final String name;
    private long sum = 0;

    public InMemoryLongSumAggregator(String name) {
      this.name = name;
    }

    @Override
    public void addValue(Long value) {
      sum += value;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Combine.CombineFn<Long, ?, Long> getCombineFn() {
      return Sum.ofLongs();
    }
  }

}
