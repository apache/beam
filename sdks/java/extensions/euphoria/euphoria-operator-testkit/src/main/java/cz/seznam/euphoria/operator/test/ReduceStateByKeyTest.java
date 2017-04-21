/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test operator {@code ReduceStateByKey}.
 */
@Processing(Processing.Type.ALL)
public class ReduceStateByKeyTest extends AbstractOperatorTest {

  static class WPair<W, K, V> extends Pair<K, V> {
    private final W window;
    WPair(W window, K first, V second) {
      super(first, second);
      this.window = window;
    }
    public W getWindow() {
      return window;
    }
  }

  static class SortState extends State<Integer, Integer> {

    final ListStorage<Integer> data;

    SortState(Context<Integer> c, StorageProvider storageProvider) {
      super(c);
      this.data = storageProvider.getListStorage(
          ListStorageDescriptor.of("data", Integer.class));
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    public void flush() {
      List<Integer> list = Lists.newArrayList(data.get());
      Collections.sort(list);
      for (Integer i : list) {
        this.getContext().collect(i);
      }
    }

    @Override
    public void close() {
      data.clear();
    }

    static void combine(SortState target, Iterable<SortState> others) {
      for (SortState other : others) {
        target.data.addAll(other.data.get());
      }
    }

  }

  @Test
  public void testSortNoWindow() {
    execute(new AbstractTestCase<Integer, Pair<String, Integer>>() {
      @Override
      protected Dataset<Pair<String, Integer>> getOutput(Dataset<Integer> input) {
        return ReduceStateByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .mergeStatesBy(SortState::combine)
            .setNumPartitions(1)
            .setPartitioner(e -> 0)
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(9, 8, 7, 5, 4).add(1, 2, 6, 3, 5).build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Pair<String, Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<Pair<String, Integer>> first = partitions.get(0);
        assertEquals(10, first.size());
        List<Integer> values = first.stream().map(Pair::getSecond)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9), values);
      }
    });
  }

  @Test
  public void testSortWindowed() {
    execute(new AbstractTestCase<Integer, WPair<Integer, Integer, Integer>>() {
      @Override
      protected Dataset<WPair<Integer, Integer, Integer>> getOutput(Dataset<Integer> input) {
        Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .mergeStatesBy(SortState::combine)
            .setNumPartitions(1)
            .setPartitioner(e -> 0)
            .windowBy(new ReduceByKeyTest.TestWindowing())
            .output();
        return FlatMap.of(output)
            .using((UnaryFunctor<Pair<Integer, Integer>, WPair<Integer, Integer, Integer>>)
                (elem, c) -> c.collect(new WPair<>(((IntWindow) c.getWindow()).getValue(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(9, 8, 10, 11 /* third window, key 0, 2, 1, 2 */,
                  5, 6, 7, 4 /* second window, key 2, 0, 1, 1 */)
            .add(8, 9 /* third window, key 2, 0 */,
                  6, 5 /* second window, key 0, 2 */)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<WPair<Integer, Integer, Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<WPair<Integer, Integer, Integer>> first = partitions.get(0);
        assertEquals(12, first.size());

        // map (window, key) -> list(data)
        Map<Pair<Integer, Integer>, List<WPair<Integer, Integer, Integer>>>
            windowKeyMap = first.stream()
            .collect(Collectors.groupingBy(p -> Pair.of(p.getWindow(), p.getFirst())));

        // two windows, three keys
        assertEquals(6, windowKeyMap.size());

        List<Integer> list;

        // second window, key 0
        list = flatten(windowKeyMap.get(Pair.of(1, 0)));
        assertEquals(Arrays.asList(6, 6), list);
        // second window, key 1
        list = flatten(windowKeyMap.get(Pair.of(1, 1)));
        assertEquals(Arrays.asList(4, 7), list);
        // second window, key 2
        list = flatten(windowKeyMap.get(Pair.of(1, 2)));
        assertEquals(Arrays.asList(5, 5), list);

        // third window, key 0
        list = flatten(windowKeyMap.get(Pair.of(2, 0)));
        assertEquals(Arrays.asList(9, 9), list);
        // third window, key 1
        list = flatten(windowKeyMap.get(Pair.of(2, 1)));
        assertEquals(Collections.singletonList(10), list);
        // third window, key 2
        list = flatten(windowKeyMap.get(Pair.of(2, 2)));
        assertEquals(Arrays.asList(8, 8, 11), list);

      }

      List<Integer> flatten(List<WPair<Integer, Integer, Integer>> l) {
        return l.stream().map(Pair::getSecond).collect(Collectors.toList());
      }
    });
  }

  // ---------------------------------------------------------------------------------

  private static class CountState extends State<String, Long> {
    final ValueStorage<Long> count;
    CountState(Context<Long> context, StorageProvider storageProvider)
    {
      super(context);
      this.count = storageProvider.getValueStorage(
          ValueStorageDescriptor.of("count-state", Long.class, 0L));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(String element) {
      count.set(count.get() + 1);
    }

    @Override
    public void flush() {
      getContext().collect(count.get());
    }

    void add(CountState other) {
      count.set(count.get() + other.count.get());
    }

    @Override
    public void close() {
      count.clear();
    }

    public static void combine(CountState target, Iterable<CountState> others) {
      for (CountState other : others) {
        target.add(other);
      }
    }
  }

  @Test
  public void testCountWindowing() {
    execute(new AbstractTestCase<Pair<String, Integer>, Pair<Integer, Long>>() {
      @Override
      protected Partitions<Pair<String, Integer>> getInput() {
        return Partitions.add(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),
            Pair.of("1-four",  10),
            Pair.of("2-two",   10),
            Pair.of("1-five",  18),
            Pair.of("2-three", 20),
            Pair.of("1-six",   22),
            Pair.of("1-seven", 23),
            Pair.of("1-eight", 23))
            .build();
      }

      @Override
      protected Dataset<Pair<Integer, Long>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        return ReduceStateByKey.of(input)
            .keyBy(e -> e.getFirst().charAt(0) - '0')
            .valueBy(Pair::getFirst)
            .stateFactory((StateFactory<String, Long, CountState>) CountState::new)
            .mergeStatesBy(CountState::combine)
            // FIXME .timedBy(Pair::getSecond) and make the assertion in the validation phase stronger
            .windowBy(Count.of(3))
            .setNumPartitions(1)
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Pair<Integer, Long>> partitions) {
        // ~ prepare the output for comparison
        List<String> flat = new ArrayList<>();
        for (Pair<Integer, Long> o : partitions.get(0)) {
          flat.add("[" + o.getFirst() + "]: " + o.getSecond());
        }
        flat.sort(Comparator.naturalOrder());
        Assert.assertEquals(
            Util.sorted(asList(
                "[1]: 3",
                "[1]: 3",
                "[1]: 2",
                "[2]: 3"),
                Comparator.naturalOrder()),
            flat);
      }
    });
  }

  // ---------------------------------------------------------------------------------

  private static class AccState<VALUE> extends State<VALUE, VALUE> {
    final ListStorage<VALUE> vals;
    @SuppressWarnings("unchecked")
    AccState(Context<VALUE> context,
             StorageProvider storageProvider)
    {
      super(context);
      vals = storageProvider.getListStorage(
          ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    @Override
    public void add(VALUE element) {
      vals.add(element);
    }

    @Override
    public void flush() {
      for (VALUE value : vals.get()) {
        getContext().collect(value);
      }
    }

    void add(AccState<VALUE> other) {
      this.vals.addAll(other.vals.get());
    }

    @Override
    public void close() {
      vals.clear();
    }

    public static <VALUE>
    void combine(AccState<VALUE> target, Iterable<AccState<VALUE>> others) {
      for (AccState<VALUE> other : others) {
        target.add(other);
      }
    }
  }

  @Test
  public void testTimeWindowing() {
    execute(new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {

      @Override
      protected Partitions<Pair<String, Integer>> getInput() {
        return Partitions.add(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),
            Pair.of("1-four",  10),
            Pair.of("2-two",   10),
            Pair.of("1-five",  18),
            Pair.of("2-three", 20),
            Pair.of("1-six",   22))
            .build();
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(Time.of(Duration.ofSeconds(5)),
                        (Pair<String, Integer> e) -> e.getSecond() * 1_000L)
                .setNumPartitions(1)
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Triple<TimeInterval, Integer, String>> partitions) {
        Assert.assertEquals(
            Util.sorted(asList(
                "(0-5): 1: 1-one",
                "(0-5): 1: 1-two",
                "(0-5): 2: 2-one",
                "(5-10): 1: 1-three",
                "(10-15): 1: 1-four",
                "(10-15): 2: 2-two",
                "(15-20): 1: 1-five",
                "(20-25): 2: 2-three",
                "(20-25): 1: 1-six"),
                Comparator.naturalOrder()),
            prepareComparison(partitions.get(0)));
      }
    });
  }

  @Test
  public void testTimeSlidingWindowing() {
    execute(new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {

      @Override
      protected Partitions<Pair<String, Integer>> getInput() {
        return Partitions.add(
            Pair.of("1-one",   1),
            Pair.of("2-ten",   6),
            Pair.of("1-two", 8),
            Pair.of("1-three",  10),
            Pair.of("2-eleven",   10),
            Pair.of("1-four",  18),
            Pair.of("2-twelve", 24),
            Pair.of("1-five",   22))
            .build();
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>> getOutput(Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(e -> e.getFirst().substring(2))
                .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(TimeSliding.of(Duration.ofSeconds(10), Duration.ofSeconds(5)),
                        (Pair<String, Integer> e) -> e.getSecond() * 1_000L)
                .setNumPartitions(2)
                .setPartitioner(element -> {
                  assert element == 1 || element == 2;
                  return element - 1;
                })
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Triple<TimeInterval, Integer, String>>
                               partitions) {
        Assert.assertEquals(
            Util.sorted(asList(
                "(-5-5): 1: one",
                "(0-10): 1: one",
                "(0-10): 1: two",
                "(5-15): 1: two",
                "(5-15): 1: three",
                "(10-20): 1: three",
                "(10-20): 1: four",
                "(15-25): 1: four",
                "(15-25): 1: five",
                "(20-30): 1: five"),
                Comparator.naturalOrder()),
            prepareComparison(partitions.get(0)));
        Assert.assertEquals(
            Util.sorted(asList(
                "(0-10): 2: ten",
                "(5-15): 2: ten",
                "(5-15): 2: eleven",
                "(10-20): 2: eleven",
                "(15-25): 2: twelve",
                "(20-30): 2: twelve"),
                Comparator.naturalOrder()),
            prepareComparison(partitions.get(1)));
      }
    });
  }

  @Test
  public void testSessionWindowing0() {
    execute(new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {

      @Override
      protected Partitions<Pair<String, Integer>> getInput() {
        return Partitions.add(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),
            Pair.of("1-four",  10),
            Pair.of("2-two",   10),
            Pair.of("1-five",  18),
            Pair.of("2-three", 20),
            Pair.of("1-six",   22))
            .build();
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(Session.of(Duration.ofSeconds(5)),
                        (Pair<String, Integer> e) -> e.getSecond() * 1_000L)
                .setNumPartitions(1)
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Triple<TimeInterval, Integer, String>>
                               partitions) {
        Assert.assertEquals(
            Util.sorted(asList(
                "(1-15): 1: 1-four",
                "(1-15): 1: 1-one",
                "(1-15): 1: 1-three",
                "(1-15): 1: 1-two",
                "(10-15): 2: 2-two",
                "(18-27): 1: 1-five",
                "(18-27): 1: 1-six",
                "(2-7): 2: 2-one",
                "(20-25): 2: 2-three"),
                Comparator.naturalOrder()),
            prepareComparison(partitions.get(0)));
      }
    });
  }

  // ~ formats the triples and orders the result in natural order
  static List<String> prepareComparison(List<Triple<TimeInterval, Integer, String>> xs) {
    List<String> flat = new ArrayList<>();
    for (Triple<TimeInterval, Integer, String> o : xs) {
      String buf = "(" +
          o.getFirst().getStartMillis() / 1_000L +
          "-" +
          o.getFirst().getEndMillis() / 1_000L +
          "): " +
          o.getSecond() + ": " + o.getThird();
      flat.add(buf);
    }
    flat.sort(Comparator.naturalOrder());
    return flat;
  }

  // ~ ------------------------------------------------------------------------------

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
          assertTrue(time == 15_000L - 1 || time == 25_000L - 1);
          return super.onElement(time, window, ctx);
        }
      };
    }
  }

  @Test
  public void testElementTimestamp() {
    execute(new AbstractTestCase<Pair<Integer, Long>, Integer>() {
      @Override
      protected Partitions<Pair<Integer, Long>> getInput() {
        return Partitions.add(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(3, 12_345L),
            Pair.of(4, 21_456L))
            .build();
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        // ~ this operator is supposed to emit elements internally with a timestamp
        // which equals the end of the time window
        Dataset<Pair<String, Integer>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> "")
                .valueBy(Pair::getFirst)
                .stateFactory(ReduceByKeyTest.SumState::new)
                .mergeStatesBy(ReduceByKeyTest.SumState::combine)
                .windowBy(Time.of(Duration.ofSeconds(5)),
                          (ExtractEventTime<Pair<String, Integer>>) Pair::getSecond)
                .output();
        // ~ now use a custom windowing with a trigger which does
        // the assertions subject to this test (use RSBK which has to
        // use triggering, unlike an optimized RBK)
        Dataset<Pair<String, Integer>> output =
            ReduceStateByKey.of(reduced)
                .keyBy(Pair::getFirst)
                .valueBy(Pair::getSecond)
                .stateFactory(ReduceByKeyTest.SumState::new)
                .mergeStatesBy(ReduceByKeyTest.SumState::combine)
                .windowBy(new TimeAssertingWindowing<>())
                .output();
        return FlatMap.of(output)
            .using((UnaryFunctor<Pair<String, Integer>, Integer>)
                (elem, context) -> context.collect(elem.getSecond()))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(2, partitions.get(0).size());
        Assert.assertEquals(asList(4, 6), Util.sorted(partitions.get(0), Comparator.naturalOrder()));
      }
    });
  }
}
