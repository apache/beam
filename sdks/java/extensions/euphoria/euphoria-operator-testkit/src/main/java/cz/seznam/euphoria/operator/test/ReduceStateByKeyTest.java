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
package cz.seznam.euphoria.operator.test;

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
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test operator {@code ReduceStateByKey}.
 */
@Processing(Processing.Type.ALL)
public class ReduceStateByKeyTest extends AbstractOperatorTest {

  static class SortState implements State<Integer, Integer> {

    final ListStorage<Integer> data;

    SortState(StateContext context, Collector<Integer> collector) {
      this.data = context.getStorageProvider().getListStorage(
          ListStorageDescriptor.of("data", Integer.class));
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

    protected int flush_(Collector<Integer> context) {
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

    static void combine(SortState target, Iterable<SortState> others) {
      for (SortState other : others) {
        target.data.addAll(other.data.get());
      }
    }
  }

  static class CountingSortState extends SortState {
    public CountingSortState(StateContext context, Collector<Integer> collector) {
      super(context, collector);
    }
    @Override
    public void flush(Collector<Integer> context) {
      int num = flush_(context);
      context.getCounter("flushed").increment(num);
    }
  }

  @Test
  public void testAccumulators() {
    execute(new AbstractTestCase<Integer, Pair<String, Integer>>() {
      @Override
      protected Dataset<Pair<String, Integer>> getOutput(Dataset<Integer> input) {
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
      public List<Pair<String, Integer>> getUnorderedOutput() {
        return Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9).stream()
            .map(i -> Pair.of("", i))
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
    execute(new AbstractTestCase<Integer, Pair<String, Integer>>() {
      @Override
      protected Dataset<Pair<String, Integer>> getOutput(Dataset<Integer> input) {
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
      public void validate(List<Pair<String, Integer>> outputs) {
        assertEquals(10, outputs.size());
        List<Integer> values = outputs.stream().map(Pair::getSecond)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9), values);
      }


      @Override
      public List<Pair<String, Integer>> getUnorderedOutput() {
        return Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9)
            .stream()
            .map(i -> Pair.of("", i))
            .collect(Collectors.toList());
      }
    });
  }

  @Test
  public void testSortWindowed() {
    execute(new AbstractTestCase<Integer, Triple<Integer, Integer, Integer>>() {
      @Override
      protected Dataset<Triple<Integer, Integer, Integer>> getOutput(Dataset<Integer> input) {
        Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .mergeStatesBy(SortState::combine)
            .windowBy(new ReduceByKeyTest.TestWindowing())
            .output();
        return FlatMap.of(output)
            .using((UnaryFunctor<Pair<Integer, Integer>, Triple<Integer, Integer, Integer>>)
                (elem, c) -> c.collect(Triple.of(((IntWindow) c.getWindow()).getValue(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(
            9, 8, 10, 11 /* third window, key 0, 2, 1, 2 */,
            5, 6, 7, 4 /* second window, key 2, 0, 1, 1 */,
            8, 9 /* third window, key 2, 0 */,
            6, 5 /* second window, key 0, 2 */);
      }

      @Override
      public void validate(List<Triple<Integer, Integer, Integer>> outputs) {
        assertEquals(12, outputs.size());

        // map (window, key) -> list(data)
        Map<Pair<Integer, Integer>, List<Triple<Integer, Integer, Integer>>>
            windowKeyMap = outputs.stream()
            .collect(Collectors.groupingBy(p -> Pair.of(p.getFirst(), p.getSecond())));

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

      List<Integer> flatten(List<Triple<Integer, Integer, Integer>> l) {
        return l.stream().map(Triple::getThird).collect(Collectors.toList());
      }


    });
  }

  // ---------------------------------------------------------------------------------

  private static class CountState<IN> implements State<IN, Long> {
    final ValueStorage<Long> count;
    CountState(StateContext context, Collector<Long> collector) {
      this.count = context.getStorageProvider().getValueStorage(
          ValueStorageDescriptor.of("count-state", Long.class, 0L));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(IN element) {
      count.set(count.get() + 1);
    }

    @Override
    public void flush(Collector<Long> context) {
      context.collect(count.get());
    }

    void add(CountState<IN> other) {
      count.set(count.get() + other.count.get());
    }

    @Override
    public void close() {
      count.clear();
    }

    public static <IN> void combine(CountState<IN> target, Iterable<CountState<IN>> others) {
      for (CountState<IN> other : others) {
        target.add(other);
      }
    }
  }

  @Test
  public void testCountWindowing() {
    execute(new AbstractTestCase<Pair<String, Integer>, Pair<Integer, Long>>() {
      @Override
      protected List<Pair<String, Integer>> getInput() {
        return Arrays.asList(
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
            Pair.of("1-eight", 23));
      }

      @Override
      protected Dataset<Pair<Integer, Long>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        return ReduceStateByKey.of(input)
            .keyBy(e -> e.getFirst().charAt(0) - '0')
            .valueBy(Pair::getFirst)
            .stateFactory((StateFactory<String, Long, CountState<String>>) CountState::new)
            .mergeStatesBy(CountState::combine)
            // FIXME .timedBy(Pair::getSecond) and make the assertion in the validation phase stronger
            .windowBy(Count.of(3))
            .output();
      }

      @Override
      public List<Pair<Integer, Long>> getUnorderedOutput() {
        // ~ prepare the output for comparison
        return Arrays.asList(
            Pair.of(1, 3L),
            Pair.of(1, 3L),
            Pair.of(1, 2L),
            Pair.of(2, 3L));
      }
    });
  }

  // ---------------------------------------------------------------------------------

  private static class AccState<VALUE> implements State<VALUE, VALUE> {
    final ListStorage<VALUE> vals;
    @SuppressWarnings("unchecked")
    AccState(StateContext context, Collector<VALUE> collector) {
      vals = context.getStorageProvider().getListStorage(
          ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    @Override
    public void add(VALUE element) {
      vals.add(element);
    }

    @Override
    public void flush(Collector<VALUE> context) {
      for (VALUE value : vals.get()) {
        context.collect(value);
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
      protected List<Pair<String, Integer>> getInput() {
        return Arrays.asList(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),  // end of first window
            Pair.of("1-four",  10), // end of second window
            Pair.of("2-two",   10),
            Pair.of("1-five",  18), // end of third window
            Pair.of("2-three", 20),
            Pair.of("1-six",   22));  // end of fourth window
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        input = AssignEventTime.of(input).using(e -> e.getSecond()).output();
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .stateFactory(AccState<String>::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(Time.of(Duration.ofMillis(5)))
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
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
    execute(new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {

      @Override
      protected List<Pair<String, Integer>> getInput() {
        return Arrays.asList(
            Pair.of("1-one",   1),
            Pair.of("2-ten",   6),
            Pair.of("1-two", 8),
            Pair.of("1-three",  10),
            Pair.of("2-eleven",   10),
            Pair.of("1-four",  18),
            Pair.of("2-twelve", 24),
            Pair.of("1-five",   22));
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>> getOutput(Dataset<Pair<String, Integer>> input) {
        input = AssignEventTime.of(input).using(e -> e.getSecond()).output();
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(e -> e.getFirst().substring(2))
                .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5)))
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
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

  @Test
  public void testSessionWindowing0() {
    execute(new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {

      @Override
      protected List<Pair<String, Integer>> getInput() {
        return Arrays.asList(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),
            Pair.of("1-four",  10),
            Pair.of("2-two",   10),
            Pair.of("1-five",  18),
            Pair.of("2-three", 20),
            Pair.of("1-six",   22));
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, String>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        input = AssignEventTime.of(input).using(e -> e.getSecond()).output();
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .stateFactory((StateFactory<String, String, AccState<String>>) AccState::new)
                .mergeStatesBy(AccState::combine)
                .windowBy(Session.of(Duration.ofMillis(5)))
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, String>, Triple<TimeInterval, Integer, String>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
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
          assertTrue("Invalid timestamp " + time,
              time == 15_000L - 1 || time == 25_000L - 1);
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

  @Test
  public void testElementTimestamp() {
    execute(new AbstractTestCase<Pair<Integer, Long>, Integer>() {

      @Override
      protected List<Pair<Integer, Long>> getInput() {
        return Arrays.asList(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(3, 12_345L),
            Pair.of(4, 21_456L));
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        // ~ this operator is supposed to emit elements internally with a timestamp
        // which equals the end of the time window
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        Dataset<Pair<String, Integer>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> "")
                .valueBy(Pair::getFirst)
                .stateFactory(ReduceByKeyTest.SumState::new)
                .mergeStatesBy(ReduceByKeyTest.SumState::combine)
                .windowBy(Time.of(Duration.ofSeconds(5)))
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
      public List<Integer> getUnorderedOutput() {
        return Arrays.asList(4, 6);
      }
    });
  }

  // ------------------------------------

  @Test
  public void testReduceStateByKeyWithWrongHashCodeImpl() {
    execute(new AbstractTestCase<Pair<Word, Long>, Pair<Word, Long>>() {

      @Override
      protected Dataset<Pair<Word, Long>> getOutput(Dataset<Pair<Word, Long>> input) {
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        return ReduceStateByKey.of(input)
                .keyBy(Pair::getFirst)
                .valueBy(Pair::getFirst)
                .stateFactory((StateFactory<Word, Long, CountState<Word>>) CountState::new)
                .mergeStatesBy(CountState::combine)
                .windowBy(Time.of(Duration.ofSeconds(1)))
                .output();
      }

      @Override
      protected List<Pair<Word, Long>> getInput() {
        return Arrays.asList(
            Pair.of(new Word("euphoria"), 300L),
            Pair.of(new Word("euphoria"), 600L),
            Pair.of(new Word("spark"), 900L),
            Pair.of(new Word("euphoria"), 1300L),
            Pair.of(new Word("flink"), 1600L),
            Pair.of(new Word("spark"), 1900L));
      }

      @Override
      public List<Pair<Word, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(new Word("euphoria"), 2L), Pair.of(new Word("spark"), 1L),  // first window
            Pair.of(new Word("euphoria"), 1L), Pair.of(new Word("spark"), 1L),  // second window
            Pair.of(new Word("flink"), 1L));
      }
    });
  }

  /**
   * String with invalid hash code implementation returning constant.
   */
  public static class Word {

    private final String str;

    Word(String str) {
      this.str = str;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Word)) return false;

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
