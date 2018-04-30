/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.type.TypeHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.testing.DatasetAssert;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Simple test suite for RBK.
 */
public class ReduceByKeyTest {

  private BeamExecutor createExecutor() {
    String[] args = {"--runner=DirectRunner"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return new BeamExecutor(options).withAllowedLateness(Duration.ofHours(1));
  }

  @Test
  public void testSimpleRBK() {
    final Flow flow = Flow.create();

    final ListDataSource<Integer> input = ListDataSource.unbounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(2, 3, 4));

    final ListDataSink<Pair<Integer, Integer>> output = ListDataSink.get();

    ReduceByKey.of(flow.createInput(input, e -> 1000L * e))
        .keyBy(i -> i % 2)
        .reduceBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofHours(1)))
        .output()
        .persist(output);

    BeamExecutor executor = createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(0, 8), Pair.of(1, 7));
  }

  @Test
  public void testEventTime() {

    Flow flow = Flow.create();
    ListDataSource<Pair<Integer, Long>> source = ListDataSource.unbounded(Arrays.asList(
        Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
        Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
        Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
        Pair.of(2, 3300L),
        Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
        Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
        Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
        Pair.of(4, 3300L), Pair.of(3, 3600L)));

    ListDataSink<Pair<Integer, Long>> sink = ListDataSink.get();
    Dataset<Pair<Integer, Long>> input = flow.createInput(source);
    input = AssignEventTime.of(input).using(Pair::getSecond).output();
    ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output()
        .persist(sink);

    BeamExecutor executor = createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(sink.getOutputs(),
            Pair.of(2, 2L), Pair.of(4, 1L),  // first window
            Pair.of(2, 2L), Pair.of(4, 1L),  // second window
            Pair.of(2, 1L), Pair.of(4, 1L),  // third window
            Pair.of(2, 1L), Pair.of(4, 1L),  // fourth window
            Pair.of(1, 1L), Pair.of(3, 2L),  // first window
            Pair.of(1, 1L), Pair.of(3, 2L),  // second window
            Pair.of(1, 2L), Pair.of(3, 2L),  // third window
            Pair.of(3, 1L));                 // fourth window
  }

  static class AssertingWindowing<T> implements Windowing<T, TimeInterval> {
    @Override
    public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
      // ~ we expect the 'element time' to be the end of the window which produced the
      // element in the preceding upstream (stateful and windowed) operator
      assertTrue("Invalid timestamp " + el.getTimestamp(),
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
          assertTrue("Invalid timestamp " + time,
              time == 15_000L - 1 || time == 25_000L - 1);
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
      sum = context.getStorageProvider().getValueStorage(
          ValueStorageDescriptor.of("sum-state", Integer.class, 0));
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

    static void combine(SumState target, Iterable<SumState> others) {
      for (SumState other : others) {
        target.add(other.sum.get());
      }
    }
  }

  @Test
  @Ignore
  public void testElementTimestamp() {

    Flow flow = Flow.create();
    ListDataSource<Pair<Integer, Long>> source = ListDataSource.bounded(Arrays.asList(
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
            .windowBy(Time.of(Duration.ofSeconds(5)))
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
        .using((UnaryFunctor<Pair<String, Integer>, Integer>)
            (elem, context) -> context.collect(elem.getSecond()))
        .output()
        .persist(sink);

    createExecutor().execute(flow);
    DatasetAssert.unorderedEquals(sink.getOutputs(), 4, 6);
  }

}
