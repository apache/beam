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
package org.apache.beam.sdk.extensions.euphoria.beam;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.extensions.euphoria.beam.window.BeamWindowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.TimeInterval;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.CountTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TriggerContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Simple test suite for RBK.
 */
public class ReduceByKeyTest {

  @Test
  public void testSimpleRBK() {
    final Flow flow = Flow.create();

    final ListDataSource<Integer> input =
        ListDataSource.unbounded(Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 4));

    final ListDataSink<Pair<Integer, Integer>> output = ListDataSink.get();

    ReduceByKey.of(flow.createInput(input, e -> 1000L * e))
        .keyBy(i -> i % 2)
        .reduceBy(Sums.ofInts())
        .windowBy(BeamWindowing.of(
            FixedWindows.of(org.joda.time.Duration.standardHours(1)),
            AfterWatermark.pastEndOfWindow(),
            AccumulationMode.DISCARDING_FIRED_PANES))
        .output()
        .persist(output);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(), Pair.of(0, 8), Pair.of(1, 7));
  }

  @Test
  public void testEventTime() {

    Flow flow = Flow.create();
    ListDataSource<Pair<Integer, Long>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of(1, 300L),
                Pair.of(2, 600L),
                Pair.of(3, 900L),
                Pair.of(2, 1300L),
                Pair.of(3, 1600L),
                Pair.of(1, 1900L),
                Pair.of(3, 2300L),
                Pair.of(2, 2600L),
                Pair.of(1, 2900L),
                Pair.of(2, 3300L),
                Pair.of(2, 300L),
                Pair.of(4, 600L),
                Pair.of(3, 900L),
                Pair.of(4, 1300L),
                Pair.of(2, 1600L),
                Pair.of(3, 1900L),
                Pair.of(4, 2300L),
                Pair.of(1, 2600L),
                Pair.of(3, 2900L),
                Pair.of(4, 3300L),
                Pair.of(3, 3600L)));

    ListDataSink<Pair<Integer, Long>> sink = ListDataSink.get();
    Dataset<Pair<Integer, Long>> input = flow.createInput(source);
    input = AssignEventTime.of(input).using(Pair::getSecond).output();

    ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(BeamWindowing.of(
            FixedWindows.of(org.joda.time.Duration.standardSeconds(1)),
            AfterWatermark.pastEndOfWindow(),
            AccumulationMode.DISCARDING_FIRED_PANES))
        .output()
        .persist(sink);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        Pair.of(2, 2L),
        Pair.of(4, 1L), // first window
        Pair.of(2, 2L),
        Pair.of(4, 1L), // second window
        Pair.of(2, 1L),
        Pair.of(4, 1L), // third window
        Pair.of(2, 1L),
        Pair.of(4, 1L), // fourth window
        Pair.of(1, 1L),
        Pair.of(3, 2L), // first window
        Pair.of(1, 1L),
        Pair.of(3, 2L), // second window
        Pair.of(1, 2L),
        Pair.of(3, 2L), // third window
        Pair.of(3, 1L)); // fourth window
  }

  @Test
  @Ignore
  public void testElementTimestamp() {

    Flow flow = Flow.create();
    ListDataSource<Pair<Integer, Long>> source =
        ListDataSource.bounded(
            Arrays.asList(
                // ~ Pair.of(value, time)
                Pair.of(1, 10_123L),
                Pair.of(2, 11_234L),
                Pair.of(3, 12_345L),
                // ~ note: exactly one element for the window on purpose (to test out
                // all is well even in case our `.combineBy` user function is not called.)
                Pair.of(4, 21_456L)));
    ListDataSink<Integer> sink = ListDataSink.get();
    Dataset<Pair<Integer, Long>> input = flow.createInput(source);

    input = AssignEventTime.of(input).using(Pair::getSecond).output();
    Dataset<Pair<String, Integer>> reduced =
        ReduceByKey.of(input)
            .keyBy(e -> "", TypeHint.ofString())
            .valueBy(Pair::getFirst, TypeHint.ofInt())
            .combineBy(Sums.ofInts(), TypeHint.ofInt())
            .windowBy(BeamWindowing.of(
                FixedWindows.of(org.joda.time.Duration.standardSeconds(5)),
                AfterWatermark.pastEndOfWindow(),
                AccumulationMode.DISCARDING_FIRED_PANES))
            .output();
    // ~ now use a custom windowing with a trigger which does
    // the assertions subject to this test (use RSBK which has to
    // use triggering, unlike an optimized RBK)
    Dataset<Pair<String, Integer>> output =
        ReduceStateByKey.of(reduced)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .stateFactory(SumState::new)
            .mergeStatesBy(SumState::combine)
            .windowBy(new AssertingWindowing<>())
            .output();
    FlatMap.of(output)
        .using(
            (UnaryFunctor<Pair<String, Integer>, Integer>)
                (elem, context) -> context.collect(elem.getSecond()))
        .output()
        .persist(sink);

    TestUtils.createExecutor().execute(flow);
    DatasetAssert.unorderedEquals(sink.getOutputs(), 4, 6);
  }

  static class AssertingWindowing<T> implements Windowing<T, TimeInterval> {

    @Override
    public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
      // ~ we expect the 'element time' to be the end of the window which produced the
      // element in the preceding upstream (stateful and windowed) operator
      assertTrue(
          "Invalid timestamp " + el.getTimestamp(),
          el.getTimestamp() == 15_000L - 1 || el.getTimestamp() == 25_000L - 1);
      return Collections.singleton(new TimeInterval(0, Long.MAX_VALUE));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Trigger<TimeInterval> getTrigger() {
      return new CountTrigger(1) {
        @Override
        public boolean isStateful() {
          return false;
        }

        @Override
        public TriggerResult onElement(long time, Window window, TriggerContext ctx) {
          // ~ we expect the 'time' to be the end of the window which produced the
          // element in the preceding upstream (stateful and windowed) operator
          assertTrue("Invalid timestamp " + time, time == 15_000L - 1 || time == 25_000L - 1);
          return super.onElement(time, window, ctx);
        }
      };
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof AssertingWindowing;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  static class SumState implements State<Integer, Integer> {

    private final ValueStorage<Integer> sum;

    SumState(StateContext context, Collector<Integer> collector) {
      sum =
          context
              .getStorageProvider()
              .getValueStorage(ValueStorageDescriptor.of("sum-state", Integer.class, 0));
    }

    static void combine(SumState target, Iterable<SumState> others) {
      for (SumState other : others) {
        target.add(other.sum.get());
      }
    }

    @Override
    public void add(Integer element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush(Collector<Integer> context) {
      context.collect(sum.get());
    }

    @Override
    public void close() {
      sum.clear();
    }
  }
}
