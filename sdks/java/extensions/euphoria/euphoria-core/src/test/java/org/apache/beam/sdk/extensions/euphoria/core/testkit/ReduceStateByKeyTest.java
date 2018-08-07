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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.TimeInterval;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateFactory;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.CountTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TriggerContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/** Test operator {@code ReduceStateByKey}. */
@Processing(Processing.Type.ALL)
public class ReduceStateByKeyTest extends AbstractOperatorTest {

  // ~ formats the triples and orders the result in natural order
  static List<String> prepareComparison(List<Triple<TimeInterval, Integer, String>> xs) {
    List<String> flat = new ArrayList<>();
    for (Triple<TimeInterval, Integer, String> o : xs) {
      String buf =
          "("
              + o.getFirst().getStartMillis() / 1_000L
              + "-"
              + o.getFirst().getEndMillis() / 1_000L
              + "): "
              + o.getSecond()
              + ": "
              + o.getThird();
      flat.add(buf);
    }
    flat.sort(Comparator.naturalOrder());
    return flat;
  }

  @Test
  public void testAccumulators() {
    execute(
        new AbstractTestCase<Integer, KV<String, Integer>>() {
          @Override
          protected Dataset<KV<String, Integer>> getOutput(Dataset<Integer> input) {
            return ReduceStateByKey.of(input)
                .keyBy(e -> "")
                .valueBy(e -> e)
                .stateFactory(CountingSortState::new)
                .mergeStatesBy((target, others) -> {})
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(9, 8, 7, 5, 4, 1, 2, 6, 3, 5);
          }

          @Override
          public List<KV<String, Integer>> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9)
                .stream()
                .map(i -> KV.of("", i))
                .collect(Collectors.toList());
          }

          @Override
          public void validateAccumulators(SnapshotProvider snapshots) {
            Map<String, Long> counters = snapshots.getCounterSnapshots();
            assertEquals(Long.valueOf(10), counters.get("flushed"));
          }
        });
  }

  @Test
  public void testSortNoWindow() {
    execute(
        new AbstractTestCase<Integer, KV<String, Integer>>() {
          @Override
          protected Dataset<KV<String, Integer>> getOutput(Dataset<Integer> input) {
            return ReduceStateByKey.of(input)
                .keyBy(e -> "")
                .valueBy(e -> e)
                .stateFactory(SortState::new)
                .mergeStatesBy(SortState::combine)
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(9, 8, 7, 5, 4, 1, 2, 6, 3, 5);
          }

          @Override
          public void validate(List<KV<String, Integer>> outputs) {
            assertEquals(10, outputs.size());
            List<Integer> values = outputs.stream().map(KV::getValue).collect(Collectors.toList());

            assertEquals(Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9), values);
          }

          @Override
          public List<KV<String, Integer>> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9)
                .stream()
                .map(i -> KV.of("", i))
                .collect(Collectors.toList());
          }
        });
  }

  @Test
  public void testSortWindowed() {
    execute(
        new AbstractTestCase<Integer, Triple<Integer, Integer, Integer>>() {
          @Override
          protected Dataset<Triple<Integer, Integer, Integer>> getOutput(Dataset<Integer> input) {
            Dataset<KV<Integer, Integer>> output =
                ReduceStateByKey.of(input)
                    .keyBy(e -> e % 3)
                    .valueBy(e -> e)
                    .stateFactory(SortState::new)
                    .mergeStatesBy(SortState::combine)
                    //.windowBy(new ReduceByKeyTest.TestWindowing())
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();
            return FlatMap.of(output)
                .using(
                    (UnaryFunctor<KV<Integer, Integer>, Triple<Integer, Integer, Integer>>)
                        (elem, c) ->
                            c.collect(
                                Triple.of(
                                    ((IntWindow) c.getWindow()).getValue(),
                                    elem.getKey(),
                                    elem.getValue())))
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(
                9,
                8,
                10,
                11 /* third window, key 0, 2, 1, 2 */,
                5,
                6,
                7,
                4 /* second window, key 2, 0, 1, 1 */,
                8,
                9 /* third window, key 2, 0 */,
                6,
                5 /* second window, key 0, 2 */);
          }

          @Override
          public void validate(List<Triple<Integer, Integer, Integer>> outputs) {
            assertEquals(12, outputs.size());

            // map (window, key) -> list(data)
            Map<KV<Integer, Integer>, List<Triple<Integer, Integer, Integer>>> windowKeyMap =
                outputs
                    .stream()
                    .collect(Collectors.groupingBy(p -> KV.of(p.getFirst(), p.getSecond())));

            // two windows, three keys
            assertEquals(6, windowKeyMap.size());

            List<Integer> list;

            // second window, key 0
            list = flatten(windowKeyMap.get(KV.of(1, 0)));
            assertEquals(Arrays.asList(6, 6), list);
            // second window, key 1
            list = flatten(windowKeyMap.get(KV.of(1, 1)));
            assertEquals(Arrays.asList(4, 7), list);
            // second window, key 2
            list = flatten(windowKeyMap.get(KV.of(1, 2)));
            assertEquals(Arrays.asList(5, 5), list);

            // third window, key 0
            list = flatten(windowKeyMap.get(KV.of(2, 0)));
            assertEquals(Arrays.asList(9, 9), list);
            // third window, key 1
            list = flatten(windowKeyMap.get(KV.of(2, 1)));
            assertEquals(Collections.singletonList(10), list);
            // third window, key 2
            list = flatten(windowKeyMap.get(KV.of(2, 2)));
            assertEquals(Arrays.asList(8, 8, 11), list);
          }

          List<Integer> flatten(List<Triple<Integer, Integer, Integer>> l) {
            return l.stream().map(Triple::getThird).collect(Collectors.toList());
          }
        });
  }

  @Test
  public void testCountWindowing() {
    execute(
        new AbstractTestCase<KV<String, Integer>, KV<Integer, Long>>() {
          @Override
          protected List<KV<String, Integer>> getInput() {
            return Arrays.asList(
                KV.of("1-one", 1),
                KV.of("2-one", 2),
                KV.of("1-two", 4),
                KV.of("1-three", 8),
                KV.of("1-four", 10),
                KV.of("2-two", 10),
                KV.of("1-five", 18),
                KV.of("2-three", 20),
                KV.of("1-six", 22),
                KV.of("1-seven", 23),
                KV.of("1-eight", 23));
          }

          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<KV<String, Integer>> input) {
            return ReduceStateByKey.of(input)
                .keyBy(e -> e.getKey().charAt(0) - '0')
                .valueBy(KV::getKey)
                .stateFactory((StateFactory<String, Long, CountState<String>>) CountState::new)
                .mergeStatesBy(CountState::combine)
                // TODO: .timedBy(KV::getValue) and make the assertion in the validation phase stronger
                // .windowBy(Count.of(3))
                // TODO rewrite windowing into beam once ReduceStateByKey is supported
                .output();
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            // ~ prepare the output for comparison
            return Arrays.asList(KV.of(1, 3L), KV.of(1, 3L), KV.of(1, 2L), KV.of(2, 3L));
          }
        });
  }

  // ---------------------------------------------------------------------------------

  @Test
  public void testTimeWindowing() {
    execute(
        new AbstractTestCase<KV<String, Integer>, Triple<TimeInterval, Integer, String>>() {

          @Override
          protected List<KV<String, Integer>> getInput() {
            return Arrays.asList(
                KV.of("1-one", 1),
                KV.of("2-one", 2),
                KV.of("1-two", 4),
                KV.of("1-three", 8), // end of first window
                KV.of("1-four", 10), // end of second window
                KV.of("2-two", 10),
                KV.of("1-five", 18), // end of third window
                KV.of("2-three", 20),
                KV.of("1-six", 22)); // end of fourth window
          }

          @Override
          protected Dataset<Triple<TimeInterval, Integer, String>> getOutput(
              Dataset<KV<String, Integer>> input) {
            input = AssignEventTime.of(input).using(e -> e.getValue()).output();
            Dataset<KV<Integer, String>> reduced =
                ReduceStateByKey.of(input)
                    .keyBy(e -> e.getKey().charAt(0) - '0')
                    .valueBy(KV::getKey)
                    .stateFactory(AccState<String>::new)
                    .mergeStatesBy(AccState::combine)
                    //.windowBy(Time.of(Duration.ofMillis(5)))
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();

            return FlatMap.of(reduced)
                .using(
                    (UnaryFunctor<KV<Integer, String>, Triple<TimeInterval, Integer, String>>)
                        (elem, context) ->
                            context.collect(
                                Triple.of(
                                    (TimeInterval) context.getWindow(),
                                    elem.getKey(),
                                    elem.getValue())))
                .output();
          }

          @Override
          public List<Triple<TimeInterval, Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(new TimeInterval(0, 5), 1, "1-one"),
                Triple.of(new TimeInterval(0, 5), 1, "1-two"),
                Triple.of(new TimeInterval(0, 5), 2, "2-one"),
                Triple.of(new TimeInterval(5, 10), 1, "1-three"),
                Triple.of(new TimeInterval(10, 15), 1, "1-four"),
                Triple.of(new TimeInterval(10, 15), 2, "2-two"),
                Triple.of(new TimeInterval(15, 20), 1, "1-five"),
                Triple.of(new TimeInterval(20, 25), 2, "2-three"),
                Triple.of(new TimeInterval(20, 25), 1, "1-six"));
          }
        });
  }

  @Test
  public void testTimeSlidingWindowing() {
    execute(
        new AbstractTestCase<KV<String, Integer>, Triple<TimeInterval, Integer, String>>() {

          @Override
          protected List<KV<String, Integer>> getInput() {
            return Arrays.asList(
                KV.of("1-one", 1),
                KV.of("2-ten", 6),
                KV.of("1-two", 8),
                KV.of("1-three", 10),
                KV.of("2-eleven", 10),
                KV.of("1-four", 18),
                KV.of("2-twelve", 24),
                KV.of("1-five", 22));
          }

          @Override
          protected Dataset<Triple<TimeInterval, Integer, String>> getOutput(
              Dataset<KV<String, Integer>> input) {
            input = AssignEventTime.of(input).using(e -> e.getValue()).output();
            Dataset<KV<Integer, String>> reduced =
                ReduceStateByKey.of(input)
                    .keyBy(e -> e.getKey().charAt(0) - '0')
                    .valueBy(e -> e.getKey().substring(2))
                    .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                    .mergeStatesBy(AccState::combine)
                    //.windowBy(TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5)))
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();

            return FlatMap.of(reduced)
                .using(
                    (UnaryFunctor<KV<Integer, String>, Triple<TimeInterval, Integer, String>>)
                        (elem, context) ->
                            context.collect(
                                Triple.of(
                                    (TimeInterval) context.getWindow(),
                                    elem.getKey(),
                                    elem.getValue())))
                .output();
          }

          @Override
          public List<Triple<TimeInterval, Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(new TimeInterval(-5, 5), 1, "one"),
                Triple.of(new TimeInterval(0, 10), 1, "one"),
                Triple.of(new TimeInterval(0, 10), 1, "two"),
                Triple.of(new TimeInterval(5, 15), 1, "two"),
                Triple.of(new TimeInterval(5, 15), 1, "three"),
                Triple.of(new TimeInterval(10, 20), 1, "three"),
                Triple.of(new TimeInterval(10, 20), 1, "four"),
                Triple.of(new TimeInterval(15, 25), 1, "four"),
                Triple.of(new TimeInterval(15, 25), 1, "five"),
                Triple.of(new TimeInterval(20, 30), 1, "five"),
                Triple.of(new TimeInterval(0, 10), 2, "ten"),
                Triple.of(new TimeInterval(5, 15), 2, "ten"),
                Triple.of(new TimeInterval(5, 15), 2, "eleven"),
                Triple.of(new TimeInterval(10, 20), 2, "eleven"),
                Triple.of(new TimeInterval(15, 25), 2, "twelve"),
                Triple.of(new TimeInterval(20, 30), 2, "twelve"));
          }
        });
  }

  // ---------------------------------------------------------------------------------

  @Test
  public void testSessionWindowing0() {
    execute(
        new AbstractTestCase<KV<String, Integer>, Triple<TimeInterval, Integer, String>>() {

          @Override
          protected List<KV<String, Integer>> getInput() {
            return Arrays.asList(
                KV.of("1-one", 1),
                KV.of("2-one", 2),
                KV.of("1-two", 4),
                KV.of("1-three", 8),
                KV.of("1-four", 10),
                KV.of("2-two", 10),
                KV.of("1-five", 18),
                KV.of("2-three", 20),
                KV.of("1-six", 22));
          }

          @Override
          protected Dataset<Triple<TimeInterval, Integer, String>> getOutput(
              Dataset<KV<String, Integer>> input) {
            input = AssignEventTime.of(input).using(e -> e.getValue()).output();
            Dataset<KV<Integer, String>> reduced =
                ReduceStateByKey.of(input)
                    .keyBy(e -> e.getKey().charAt(0) - '0')
                    .valueBy(KV::getKey)
                    .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                    .mergeStatesBy(AccState::combine)
                    //.windowBy(Session.of(Duration.ofMillis(5)))
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();

            return FlatMap.of(reduced)
                .using(
                    (UnaryFunctor<KV<Integer, String>, Triple<TimeInterval, Integer, String>>)
                        (elem, context) ->
                            context.collect(
                                Triple.of(
                                    (TimeInterval) context.getWindow(),
                                    elem.getKey(),
                                    elem.getValue())))
                .output();
          }

          @Override
          public List<Triple<TimeInterval, Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(new TimeInterval(1, 15), 1, "1-four"),
                Triple.of(new TimeInterval(1, 15), 1, "1-one"),
                Triple.of(new TimeInterval(1, 15), 1, "1-three"),
                Triple.of(new TimeInterval(1, 15), 1, "1-two"),
                Triple.of(new TimeInterval(10, 15), 2, "2-two"),
                Triple.of(new TimeInterval(18, 27), 1, "1-five"),
                Triple.of(new TimeInterval(18, 27), 1, "1-six"),
                Triple.of(new TimeInterval(2, 7), 2, "2-one"),
                Triple.of(new TimeInterval(20, 25), 2, "2-three"));
          }
        });
  }

  @Test
  public void testElementTimestamp() {
    execute(
        new AbstractTestCase<KV<Integer, Long>, Integer>() {

          @Override
          protected List<KV<Integer, Long>> getInput() {
            return Arrays.asList(
                // ~ KV.of(value, time)
                KV.of(1, 10_123L), KV.of(2, 11_234L), KV.of(3, 12_345L), KV.of(4, 21_456L));
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<KV<Integer, Long>> input) {
            // ~ this operator is supposed to emit elements internally with a timestamp
            // which equals the end of the time window
            input = AssignEventTime.of(input).using(KV::getValue).output();
            Dataset<KV<String, Integer>> reduced =
                ReduceStateByKey.of(input)
                    .keyBy(e -> "")
                    .valueBy(KV::getKey)
                    .stateFactory(ReduceByKeyTest.SumState::new)
                    .mergeStatesBy(ReduceByKeyTest.SumState::combine)
                    //.windowBy(Time.of(Duration.ofSeconds(5)))
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();
            // ~ now use a custom windowing with a trigger which does
            // the assertions subject to this test (use RSBK which has to
            // use triggering, unlike an optimized RBK)
            Dataset<KV<String, Integer>> output =
                ReduceStateByKey.of(reduced)
                    .keyBy(KV::getKey)
                    .valueBy(KV::getValue)
                    .stateFactory(ReduceByKeyTest.SumState::new)
                    .mergeStatesBy(ReduceByKeyTest.SumState::combine)
                    //.windowBy(new TimeAssertingWindowing<>())
                    // TODO rewrite windowing into beam once ReduceStateByKey is supported
                    .output();
            return FlatMap.of(output)
                .using(
                    (UnaryFunctor<KV<String, Integer>, Integer>)
                        (elem, context) -> context.collect(elem.getValue()))
                .output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(4, 6);
          }
        });
  }

  @Test
  public void testReduceStateByKeyWithWrongHashCodeImpl() {
    execute(
        new AbstractTestCase<KV<Word, Long>, KV<Word, Long>>() {

          @Override
          protected Dataset<KV<Word, Long>> getOutput(Dataset<KV<Word, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return ReduceStateByKey.of(input)
                .keyBy(KV::getKey)
                .valueBy(KV::getKey)
                .stateFactory((StateFactory<Word, Long, CountState<Word>>) CountState::new)
                .mergeStatesBy(CountState::combine)
                //.windowBy(Time.of(Duration.ofSeconds(1)))
                // TODO rewrite windowing into beam once ReduceStateByKey is supported
                .output();
          }

          @Override
          protected List<KV<Word, Long>> getInput() {
            return Arrays.asList(
                KV.of(new Word("euphoria"), 300L),
                KV.of(new Word("euphoria"), 600L),
                KV.of(new Word("spark"), 900L),
                KV.of(new Word("euphoria"), 1300L),
                KV.of(new Word("flink"), 1600L),
                KV.of(new Word("spark"), 1900L));
          }

          @Override
          public List<KV<Word, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(new Word("euphoria"), 2L),
                KV.of(new Word("spark"), 1L), // first window
                KV.of(new Word("euphoria"), 1L),
                KV.of(new Word("spark"), 1L), // second window
                KV.of(new Word("flink"), 1L));
          }
        });
  }

  static class SortState implements State<Integer, Integer> {

    final ListStorage<Integer> data;

    SortState(StateContext context, Collector<Integer> collector) {
      this.data =
          context
              .getStorageProvider()
              .getListStorage(ListStorageDescriptor.of("data", Integer.class));
    }

    static void combine(SortState target, Iterable<SortState> others) {
      for (SortState other : others) {
        target.data.addAll(other.data.get());
      }
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    public void flush(Collector<Integer> context) {
      List<Integer> list = Lists.newArrayList(data.get());
      Collections.sort(list);
      for (Integer i : list) {
        context.collect(i);
      }
    }

    protected int flushAndGetSize(Collector<Integer> context) {
      List<Integer> list = Lists.newArrayList(data.get());
      Collections.sort(list);
      for (Integer i : list) {
        context.collect(i);
      }
      return list.size();
    }

    @Override
    public void close() {
      data.clear();
    }
  }

  static class CountingSortState extends SortState {

    public CountingSortState(StateContext context, Collector<Integer> collector) {
      super(context, collector);
    }

    @Override
    public void flush(Collector<Integer> context) {
      int num = flushAndGetSize(context);
      context.getCounter("flushed").increment(num);
    }
  }

  // ~ ------------------------------------------------------------------------------

  private static class CountState<InputT> implements State<InputT, Long> {

    final ValueStorage<Long> count;

    CountState(StateContext context, Collector<Long> collector) {
      this.count =
          context
              .getStorageProvider()
              .getValueStorage(ValueStorageDescriptor.of("count-state", Long.class, 0L));
    }

    public static <InputT> void combine(
        CountState<InputT> target, Iterable<CountState<InputT>> others) {

      for (CountState<InputT> other : others) {
        target.add(other);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(InputT element) {
      count.set(count.get() + 1);
    }

    @Override
    public void flush(Collector<Long> context) {
      context.collect(count.get());
    }

    void add(CountState<InputT> other) {
      count.set(count.get() + other.count.get());
    }

    @Override
    public void close() {
      count.clear();
    }
  }

  private static class AccState<V> implements State<V, V> {

    final ListStorage<V> vals;

    @SuppressWarnings("unchecked")
    AccState(StateContext context, Collector<V> collector) {
      vals =
          context
              .getStorageProvider()
              .getListStorage(ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    public static <V> void combine(AccState<V> target, Iterable<AccState<V>> others) {
      for (AccState<V> other : others) {
        target.add(other);
      }
    }

    @Override
    public void add(V element) {
      vals.add(element);
    }

    @Override
    public void flush(Collector<V> context) {
      for (V value : vals.get()) {
        context.collect(value);
      }
    }

    void add(AccState<V> other) {
      this.vals.addAll(other.vals.get());
    }

    @Override
    public void close() {
      vals.clear();
    }
  }

  // ------------------------------------

  static class TimeAssertingWindowing<T> implements Windowing<T, TimeInterval> {

    @Override
    public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> input) {
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
        public Trigger.TriggerResult onElement(long time, Window window, TriggerContext ctx) {
          // ~ we expect the 'time' to be the end of the window which produced the
          // element in the preceding upstream (stateful and windowed) operator
          assertTrue("Invalid timestamp " + time, time == 15_000L - 1 || time == 25_000L - 1);
          return super.onElement(time, window, ctx);
        }
      };
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TimeAssertingWindowing;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  /** String with invalid hash code implementation returning constant. */
  public static class Word {

    private final String str;

    Word(String str) {
      this.str = str;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Word)) {
        return false;
      }

      Word word = (Word) o;

      return !(str != null ? !str.equals(word.str) : word.str != null);
    }

    @Override
    public int hashCode() {
      return 42;
    }

    @Override
    public String toString() {
      return str;
    }
  }
}
