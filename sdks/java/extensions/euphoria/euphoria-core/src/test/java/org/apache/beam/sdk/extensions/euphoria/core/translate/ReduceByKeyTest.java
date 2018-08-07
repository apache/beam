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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Ignore;
import org.junit.Test;

/** Simple test suite for RBK. */
public class ReduceByKeyTest {

  @Test
  public void testSimpleRBK() {
    final Flow flow = Flow.create();

    final ListDataSource<Integer> input =
        ListDataSource.unbounded(Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 4));

    final ListDataSink<KV<Integer, Integer>> output = ListDataSink.get();

    ReduceByKey.of(flow.createInput(input, e -> 1000L * e))
        .keyBy(i -> i % 2)
        .reduceBy(Sums.ofInts())
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
        .triggeredBy(AfterWatermark.pastEndOfWindow())
        .discardingFiredPanes()
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(), KV.of(0, 8), KV.of(1, 7));
  }

  @Test
  public void testEventTime() {

    Flow flow = Flow.create();
    ListDataSource<KV<Integer, Long>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                KV.of(1, 300L),
                KV.of(2, 600L),
                KV.of(3, 900L),
                KV.of(2, 1300L),
                KV.of(3, 1600L),
                KV.of(1, 1900L),
                KV.of(3, 2300L),
                KV.of(2, 2600L),
                KV.of(1, 2900L),
                KV.of(2, 3300L),
                KV.of(2, 300L),
                KV.of(4, 600L),
                KV.of(3, 900L),
                KV.of(4, 1300L),
                KV.of(2, 1600L),
                KV.of(3, 1900L),
                KV.of(4, 2300L),
                KV.of(1, 2600L),
                KV.of(3, 2900L),
                KV.of(4, 3300L),
                KV.of(3, 3600L)));

    ListDataSink<KV<Integer, Long>> sink = ListDataSink.get();
    Dataset<KV<Integer, Long>> input = flow.createInput(source);
    input = AssignEventTime.of(input).using(KV::getValue).output();

    ReduceByKey.of(input)
        .keyBy(KV::getKey)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
        .triggeredBy(AfterWatermark.pastEndOfWindow())
        .discardingFiredPanes()
        .output()
        .persist(sink);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        KV.of(2, 2L),
        KV.of(4, 1L), // first window
        KV.of(2, 2L),
        KV.of(4, 1L), // second window
        KV.of(2, 1L),
        KV.of(4, 1L), // third window
        KV.of(2, 1L),
        KV.of(4, 1L), // fourth window
        KV.of(1, 1L),
        KV.of(3, 2L), // first window
        KV.of(1, 1L),
        KV.of(3, 2L), // second window
        KV.of(1, 2L),
        KV.of(3, 2L), // third window
        KV.of(3, 1L)); // fourth window
  }

  @Test
  @Ignore
  public void testElementTimestamp() {

    Flow flow = Flow.create();
    ListDataSource<KV<Integer, Long>> source =
        ListDataSource.bounded(
            Arrays.asList(
                // ~ KV.of(value, time)
                KV.of(1, 10_123L),
                KV.of(2, 11_234L),
                KV.of(3, 12_345L),
                // ~ note: exactly one element for the window on purpose (to test out
                // all is well even in case our `.combineBy` user function is not called.)
                KV.of(4, 21_456L)));
    ListDataSink<Integer> sink = ListDataSink.get();
    Dataset<KV<Integer, Long>> input = flow.createInput(source);

    input = AssignEventTime.of(input).using(KV::getValue).output();
    Dataset<KV<String, Integer>> reduced =
        ReduceByKey.of(input)
            .keyBy(e -> "", TypeDescriptors.strings())
            .valueBy(KV::getKey, TypeDescriptors.integers())
            .combineBy(Sums.ofInts(), TypeDescriptors.integers())
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes()
            .output();
    // ~ now use a custom windowing with a trigger which does
    // the assertions subject to this test (use RSBK which has to
    // use triggering, unlike an optimized RBK)
    Dataset<KV<String, Integer>> output =
        ReduceStateByKey.of(reduced)
            .keyBy(KV::getKey)
            .valueBy(KV::getValue)
            .stateFactory(SumState::new)
            .mergeStatesBy(SumState::combine)
            //.windowBy(new AssertingWindowing<>()) //TODO apply Beam windowing
            .output();
    FlatMap.of(output)
        .using(
            (UnaryFunctor<KV<String, Integer>, Integer>)
                (elem, context) -> context.collect(elem.getValue()))
        .output()
        .persist(sink);

    BeamRunnerWrapper.ofDirect().executeSync(flow);
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
