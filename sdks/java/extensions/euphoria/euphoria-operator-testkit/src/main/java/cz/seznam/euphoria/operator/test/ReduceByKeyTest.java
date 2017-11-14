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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test operator {@code ReduceByKey}.
 */
@Processing(Processing.Type.ALL)
public class ReduceByKeyTest extends AbstractOperatorTest {

  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  @Test
  public void testReductionType0() {
    execute(new AbstractTestCase<Integer, Pair<Integer, HashSet<Integer>>>(
        /* don't parallelize this test, because it doesn't work
         * well with count windows */
        1) {
      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
      }

      @Override
      protected Dataset<Pair<Integer, HashSet<Integer>>>
      getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<Integer, HashSet<Integer>>) Sets::newHashSet)
            .windowBy(Count.of(3))
            .output();
      }

      @Override
      public List<Pair<Integer, HashSet<Integer>>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, Sets.newHashSet(2, 4, 6)),
            Pair.of(1, Sets.newHashSet(1, 3, 5)),
            Pair.of(1, Sets.newHashSet(7, 9)));
      }
    });
  }


  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  @Test
  public void testReductionType0WithSortedValues() {
    execute(new AbstractTestCase<Integer, ArrayList<Pair<Integer, ArrayList<Integer>>>>(
        /* don't parallelize this test, because it doesn't work
         * well with count windows */
        1) {
      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1);
      }

      @Override
      protected Dataset<ArrayList<Pair<Integer, ArrayList<Integer>>>>
      getOutput(Dataset<Integer> input) {
        Dataset<Pair<Integer, ArrayList<Integer>>> reducedByWindow = ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<Integer, ArrayList<Integer>>) Lists::newArrayList)
            .withSortedValues(Integer::compare)
            .windowBy(Count.of(3))
            .output();

        return ReduceWindow.of(reducedByWindow)
            .reduceBy(Lists::newArrayList)
            .withSortedValues((l, r) -> {
              int cmp = l.getFirst().compareTo(r.getFirst());
              if (cmp == 0) {
                int firstLeft = l.getSecond().get(0);
                int firstRight = r.getSecond().get(0);
                cmp = Integer.compare(firstLeft, firstRight);
              }
              return cmp;
            })
            .windowBy(GlobalWindowing.get())
            .output();
      }

      @Override
      public void validate(
          List<ArrayList<Pair<Integer, ArrayList<Integer>>>> outputs) throws AssertionError {

        assertEquals(1, outputs.size());
        assertEquals(Lists.newArrayList(
            Pair.of(0, Lists.newArrayList(2)),
            Pair.of(0, Lists.newArrayList(4, 6, 8)),
            Pair.of(1, Lists.newArrayList(1, 3)),
            Pair.of(1, Lists.newArrayList(5, 7, 9))),
            outputs.get(0));
      }

    });
  }


  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  @Test
  public void testReductionType0MultiValues() {
    execute(new AbstractTestCase<Integer, Pair<Integer, Integer>>(
        /* don't parallelize this test, because it doesn't work
         * well with count windows */
        1) {

      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
      }

      @Override
      protected Dataset<Pair<Integer, Integer>>
      getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy((Iterable<Integer> in, Collector<Integer> ctx) -> {
              int sum = 0;
              for (Integer i : in) {
                sum += i;
                ctx.collect(sum);
              }
            })
            .windowBy(Count.of(3))
            .output();
      }

      @Override
      public void validate(List<Pair<Integer, Integer>> output) {
        HashMap<Integer, List<Integer>> byKey = new HashMap<>();
        for (Pair<Integer, Integer> p : output) {
          List<Integer> xs = byKey.computeIfAbsent(p.getFirst(), k -> new ArrayList<>());
          xs.add(p.getSecond());
        }

        assertEquals(2, byKey.size());

        assertNotNull(byKey.get(0));
        assertEquals(3, byKey.get(0).size());
        assertEquals(Arrays.asList(2, 6, 12), byKey.get(0));

        // ~ cannot make any assumption about the order of the elements in the windows
        // (on batch) since we have no event-time order for the test data
        assertNotNull(byKey.get(1));
        assertEquals(
            Sets.newHashSet(1, 4, 9, 7, 16),
            byKey.get(1).stream().collect(Collectors.toSet()));
      }

      @Override
      public List<Pair<Integer, Integer>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, 12),
            Pair.of(1, 9),
            Pair.of(1, 16));
      }
    });
  }


  @Test
  public void testEventTime() {
    execute(new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Pair<Integer, Long>> input) {
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        return ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();
      }

      @Override
      protected List<Pair<Integer, Long>> getInput() {
        return Arrays.asList(
            Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
            Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
            Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
            Pair.of(2, 3300L),
            Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
            Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
            Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
            Pair.of(4, 3300L), Pair.of(3, 3600L));
      }

      @Override
      public List<Pair<Integer, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, 2L), Pair.of(4, 1L),  // first window
            Pair.of(2, 2L), Pair.of(4, 1L),  // second window
            Pair.of(2, 1L), Pair.of(4, 1L),  // third window
            Pair.of(2, 1L), Pair.of(4, 1L),  // fourth window
            Pair.of(1, 1L), Pair.of(3, 2L),  // first window
            Pair.of(1, 1L), Pair.of(3, 2L),  // second window
            Pair.of(1, 2L), Pair.of(3, 2L),  // third window
            Pair.of(3, 1L));                 // fourth window
      }
    });
  }

  static class TestWindowing implements Windowing<Integer, IntWindow> {
    @Override
    public Iterable<IntWindow> assignWindowsToElement(WindowedElement<?, Integer> input) {
      return Collections.singleton(new IntWindow(input.getElement() / 4));
    }

    @Override
    public Trigger<IntWindow> getTrigger() {
      return NoopTrigger.get();
    }
  }

  @Test
  public void testReduceWithWindowing() {
    execute(new AbstractTestCase<Integer, Pair<Integer, Long>>() {
      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(new TestWindowing())
            .output();
      }

      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(
            1, 2, 3 /* first window, keys 1, 2, 0 */,
            4, 5, 6, 7 /* second window, keys 1, 2, 0, 1 */,
            8, 9, 10 /* third window, keys 2, 0, 1 */,
            5, 6, 7 /* second window, keys 2, 0, 1 */,
            8, 9, 10, 11 /* third window, keys 2, 0, 1, 2 */,
            12, 13, 14, 15 /* fourth window, keys 0, 1, 2, 0 */);
      }

      @Override
      public List<Pair<Integer, Long>> getUnorderedOutput() {
        return Arrays.asList(Pair.of(0, 1L), Pair.of(2, 1L) /* first window */,
            Pair.of(0, 2L), Pair.of(2, 2L) /* second window */,
            Pair.of(0, 2L), Pair.of(2, 3L) /* third window */,
            Pair.of(0, 2L), Pair.of(2, 1L) /* fourth window */,
            Pair.of(1, 1L) /* first window*/,
            Pair.of(1, 3L) /* second window */,
            Pair.of(1, 2L) /* third window */,
            Pair.of(1, 1L) /* fourth window */);
      }
    });
  }

  // ~ Makes no sense to test UNBOUNDED input without windowing defined.
  // It would run infinitely without providing any result.
  @Processing(Processing.Type.BOUNDED)
  @Test
  public void testReduceWithoutWindowing() {
    execute(new AbstractTestCase<String, Pair<String, Long>>() {
      @Override
      protected List<String> getInput() {
        String[] words =
            "one two three four one two three four one two three one two one".split(" ");
        return Arrays.asList(words);
      }

      @Override
      public List<Pair<String, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of("one", 5L),
            Pair.of("two", 4L),
            Pair.of("three", 3L),
            Pair.of("four", 2L));
      }

      @Override
      protected Dataset<Pair<String, Long>> getOutput(Dataset<String> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .output();
      }
    });
  }

  // ----------------------------------------------------------------------------

  // ~ every instance is unique: this allows us to exercise merging
  static final class CWindow extends Window<CWindow> {

    static int _idCounter = 0;
    static final Object _idCounterMutex = new Object();
    static int new_id() {
      synchronized (_idCounterMutex) {
        return ++_idCounter;
      }
    }

    private final int _id;

    private final int bucket;

    public CWindow(int bucket) {
      this._id = new_id();
      this.bucket = bucket;
    }

    @Override
    public int hashCode() {
      return this._id;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof CWindow && this._id == ((CWindow) obj)._id;
    }

    @Override
    public int compareTo(CWindow that) {
      return Integer.compare(this._id, that._id);
    }

    @Override
    public String toString() {
      return "CWindow{" +
          "bucket=" + bucket +
          ", identity=" + _id +
          '}';
    }
  }

  // count windowing; firing based on window.bucket (size of the window)
  static final class CWindowTrigger implements Trigger<CWindow> {
    private final ValueStorageDescriptor<Long> countDesc =
        ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y);

    @Override
    public TriggerResult onElement(long time, CWindow w, TriggerContext ctx) {
      ValueStorage<Long> cnt = ctx.getValueStorage(countDesc);
      cnt.set(cnt.get() + 1);
      if (cnt.get() >= w.bucket) {
        return TriggerResult.FLUSH_AND_PURGE;
      }
      return TriggerResult.NOOP;
    }

    @Override
    public TriggerResult onTimer(long time, CWindow w, TriggerContext ctx) {
      return TriggerResult.NOOP;
    }

    @Override
    public void onClear(CWindow window, TriggerContext ctx) {
      ctx.getValueStorage(countDesc).clear();
    }

    @Override
    public void onMerge(CWindow w, TriggerContext.TriggerMergeContext ctx) {
      ctx.mergeStoredState(countDesc);
    }
  }

  static final class CWindowing<T> implements MergingWindowing<T, CWindow> {
    private final int size;

    CWindowing(int size) {
      this.size = size;
    }

    @Override
    public Iterable<CWindow> assignWindowsToElement(WindowedElement<?, T> input) {
      return Sets.newHashSet(new CWindow(size));
    }

    @Override
    public Collection<Pair<Collection<CWindow>, CWindow>>
    mergeWindows(Collection<CWindow> actives) {
      Map<Integer, List<CWindow>> byMergeType = new HashMap<>();
      for (CWindow cw : actives) {
        byMergeType.computeIfAbsent(cw.bucket, k -> new ArrayList<>()).add(cw);
      }
      List<Pair<Collection<CWindow>, CWindow>> merges = new ArrayList<>();
      for (List<CWindow> siblings : byMergeType.values()) {
        if (siblings.size() >= 2) {
          merges.add(Pair.of(siblings, siblings.get(0)));
        }
      }
      return merges;
    }

    @Override
    public Trigger<CWindow> getTrigger() {
      return new CWindowTrigger();
    }
  }

  @Test
  public void testMergingAndTriggering() {
    execute(new AbstractTestCase<Pair<String, Long>, Pair<String, Long>>(1) {

      @Override
      protected List<Pair<String, Long>> getInput() {
        return Arrays.asList(
                Pair.of("a",      20L),
                Pair.of("c",    3000L),
                Pair.of("b",      10L),
                Pair.of("b",     100L),
                Pair.of("a",    4000L),
                Pair.of("c",     300L),
                Pair.of("b",    1000L),
                Pair.of("b",   50000L),
                Pair.of("a",  100000L),
                Pair.of("a",     800L),
                Pair.of("a",      80L));
      }

      @Override
      protected Dataset<Pair<String, Long>>
      getOutput(Dataset<Pair<String, Long>> input) {
        return ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .windowBy(new CWindowing<>(3))
            .output();
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Pair<String, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of("a",         880L),
            Pair.of("a",      104020L),
            Pair.of("b",        1110L),
            Pair.of("b",       50000L),
            Pair.of("c",        3300L));
      }
    });
  }

  @Test
  public void testSessionWindowing() {
    execute(new AbstractTestCase<
        Pair<String, Integer>,
        Triple<TimeInterval, Integer, HashSet<String>>>() {

      @Override
      protected List<Pair<String, Integer>> getInput() {
        return Arrays.asList(
            Pair.of("1-one", 1),
            Pair.of("2-one", 2),
            Pair.of("1-two", 4),
            Pair.of("1-three", 8),
            Pair.of("1-four", 10),
            Pair.of("2-two", 10),
            Pair.of("1-five", 18),
            Pair.of("2-three", 20),
            Pair.of("1-six", 22));
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, HashSet<String>>> getOutput
          (Dataset<Pair<String, Integer>> input) {
        input = AssignEventTime.of(input).using(e -> e.getSecond()).output();
        Dataset<Pair<Integer, HashSet<String>>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .reduceBy((ReduceFunction<String, HashSet<String>>) Sets::newHashSet)
                .windowBy(Session.of(Duration.ofMillis(5)))
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, HashSet<String>>, Triple<TimeInterval, Integer, HashSet<String>>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      public List<Triple<TimeInterval, Integer, HashSet<String>>> getUnorderedOutput() {
        return Arrays.asList(
            Triple.of(new TimeInterval(1, 15), 1, Sets.newHashSet("1-four", "1-one", "1-three", "1-two")),
            Triple.of(new TimeInterval(10, 15), 2, Sets.newHashSet("2-two")),
            Triple.of(new TimeInterval(18, 27), 1, Sets.newHashSet("1-five", "1-six")),
            Triple.of(new TimeInterval(2, 7), 2, Sets.newHashSet("2-one")),
            Triple.of(new TimeInterval(20, 25), 2, Sets.newHashSet("2-three")));
      }

    });
  }

  // ~ ------------------------------------------------------------------------------

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
  public void testElementTimestamp() {
    class AssertingWindowing<T> implements Windowing<T, TimeInterval> {
      @Override
      public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
        // ~ we expect the 'element time' to be the end of the window which produced the
        // element in the preceding upstream (stateful and windowed) operator
        assertTrue(el.getTimestamp() == 15_000L - 1 || el.getTimestamp() == 25_000L - 1);
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
            assertTrue(time == 15_000L - 1 || time == 25_000L - 1);
            return super.onElement(time, window, ctx);
          }
        };
      }
    }

    execute(new AbstractTestCase<Pair<Integer, Long>, Integer>() {
      @Override
      protected List<Pair<Integer, Long>> getInput() {
        return Arrays.asList(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(3, 12_345L),
            // ~ note: exactly one element for the window on purpose (to test out
            // all is well even in case our `.combineBy` user function is not called.)
            Pair.of(4, 21_456L));
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        // ~ this operator is supposed to emit elements internally with a timestamp
        // which equals the emission (== end in this case) of the time window
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        Dataset<Pair<String, Integer>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> "")
                .valueBy(Pair::getFirst)
                .combineBy(Sums.ofInts())
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

  @Test
  public void testReduceByKeyWithWrongHashCodeImpl() {
    execute(new AbstractTestCase<Pair<Word, Long>, Pair<Word, Long>>() {

      @Override
      protected Dataset<Pair<Word, Long>> getOutput(Dataset<Pair<Word, Long>> input) {
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        return ReduceByKey.of(input)
                .keyBy(Pair::getFirst)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
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

  @Test
  public void testAccumulators() {
    execute(new AbstractTestCase<Integer, Pair<Integer, Integer>>() {
      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(1, 2, 3, 4, 5);
      }

      @Override
      protected Dataset<Pair<Integer, Integer>>
      getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy((ReduceFunctor<Integer, Integer>) (elems, collector) -> {
              int sum = 0;
              for (Integer elem : elems) {
                sum += elem;
                if (elem % 2 == 0) {
                  collector.getCounter("evens").increment();
                } else {
                  collector.getCounter("odds").increment();
                }
              }
              collector.collect(sum);
            })
            .windowBy(GlobalWindowing.get())
            .output();
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Pair<Integer, Integer>> getUnorderedOutput() {
        return Arrays.asList(Pair.of(1, 9), Pair.of(0, 6));
      }

      @Override
      public void validateAccumulators(SnapshotProvider snapshots) {
        Map<String, Long> counters = snapshots.getCounterSnapshots();
        assertEquals(Long.valueOf(2), counters.get("evens"));
        assertEquals(Long.valueOf(3), counters.get("odds"));
      }
    });
  }

  /**
   * String with invalid hash code implementation returning constant.
   */
  private static class Word {
    private final String str;

    public Word(String str) {
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
