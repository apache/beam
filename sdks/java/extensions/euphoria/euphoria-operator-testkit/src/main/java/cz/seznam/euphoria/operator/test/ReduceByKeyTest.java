/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Test operator {@code ReduceByKey}.
 */
@Processing(Processing.Type.ALL)
public class ReduceByKeyTest extends AbstractOperatorTest {

  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  @Test
  public void testReductionType0() {
    execute(new AbstractTestCase<Integer, Pair<Integer, HashSet<Integer>>>() {
      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(asList(1, 2, 3, 4, 5, 6, 7, 9)).build();
      }

      @Override
      protected Dataset<Pair<Integer, HashSet<Integer>>>
      getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy(Sets::newHashSet)
            .windowBy(Count.of(3))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Pair<Integer, HashSet<Integer>>> partitions) {
        List<Pair<Integer, HashSet<Integer>>> ps = partitions.get(0);
        HashMap<Integer, List<HashSet<Integer>>> byKey = new HashMap<>();
        for (Pair<Integer, HashSet<Integer>> p : ps) {
          List<HashSet<Integer>> xs =
              byKey.computeIfAbsent(p.getFirst(), k -> new ArrayList<>());
          xs.add(p.getSecond());
        }

        assertEquals(2, byKey.size());

        assertNotNull(byKey.get(0));
        assertEquals(1, byKey.get(0).size());
        assertEquals(Sets.newHashSet(2, 4, 6), byKey.get(0).get(0));

        // ~ cannot make any assumption about the order of the elements in the windows
        // (on batch) since we have no event-time order for the test data; however, we
        // can verify the window sizes
        assertNotNull(byKey.get(1));
        assertEquals(
            Sets.newHashSet(3, 2),
            byKey.get(1).stream().map(HashSet::size).collect(Collectors.toSet()));
      }
    });
  }

  @Test
  public void testEventTime() {
    execute(new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Pair<Integer, Long>> input) {
        return ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .setPartitioner(e -> e % 2)
            .windowBy(Time.of(Duration.ofSeconds(1)), Pair::getSecond)
            .output();
      }

      @Override
      protected Partitions<Pair<Integer, Long>> getInput() {
        return Partitions
            .add(Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
                Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
                Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
                Pair.of(2, 3300L))
            .add(Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
                Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
                Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
                Pair.of(4, 3300L), Pair.of(3, 3600L))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, Long>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, 2L), Pair.of(4, 1L),  // first window
            Pair.of(2, 2L), Pair.of(4, 1L),  // second window
            Pair.of(2, 1L), Pair.of(4, 1L),  // third window
            Pair.of(2, 1L), Pair.of(4, 1L)), // fourth window
            first);
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, 1L), Pair.of(3, 2L),  // first window
            Pair.of(1, 1L), Pair.of(3, 2L),  // second window
            Pair.of(1, 2L), Pair.of(3, 2L),  // third window
            Pair.of(3, 1L)),                 // fourth window
            second);
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
            .setPartitioner(e -> e % 2)
            .windowBy(new TestWindowing())
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(
            Arrays.asList(1, 2, 3 /* first window, keys 1, 2, 0 */,
                4, 5, 6, 7 /* second window, keys 1, 2, 0, 1 */,
                8, 9, 10 /* third window, keys 2, 0, 1 */))
            .add(Arrays.asList(5, 6, 7 /* second window, keys 2, 0, 1 */,
                8, 9, 10, 11 /* third window, keys 2, 0, 1, 2 */,
                12, 13, 14, 15 /* fourth window, keys 0, 1, 2, 0 */))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, Long>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, 1L), Pair.of(2, 1L) /* first window */,
            Pair.of(0, 2L), Pair.of(2, 2L) /* second window */,
            Pair.of(0, 2L), Pair.of(2, 3L) /* third window */,
            Pair.of(0, 2L), Pair.of(2, 1L) /* fourth window */),
            first);
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(Pair.of(1, 1L) /* first window*/,
            Pair.of(1, 3L) /* second window */,
            Pair.of(1, 2L) /* third window */,
            Pair.of(1, 1L) /* fourth window */),
            second);
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
      protected Partitions<String> getInput() {
        String[] words =
            "one two three four one two three four one two three one two one".split(" ");
        return Partitions.add(words).build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Pair<String, Long>> partitions) {
        assertEquals(1, partitions.size());
        HashMap<String, Long> actual = new HashMap<>();
        for (Pair<String, Long> p : partitions.get(0)) {
          actual.put(p.getFirst(), p.getSecond());
        }

        HashMap<String, Long> expected = new HashMap<>();
        expected.put("one", 5L);
        expected.put("two", 4L);
        expected.put("three", 3L);
        expected.put("four", 2L);
        assertEquals(expected, actual);
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
  static final class CWindow extends Window implements Comparable<CWindow> {

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
    public TriggerResult onMerge(CWindow w, TriggerContext.TriggerMergeContext ctx) {
      ctx.mergeStoredState(countDesc);
      if (ctx.getValueStorage(countDesc).get() >= w.bucket) {
        return TriggerResult.FLUSH_AND_PURGE;
      }
      return TriggerResult.NOOP;
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
    execute(new AbstractTestCase<Pair<String, Long>, Pair<String, Long>>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      protected Partitions<Pair<String, Long>> getInput() {
        return Partitions
            .add(
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
                Pair.of("a",      80L))
            .build();
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
      public void validate(Partitions<Pair<String, Long>> partitions) {
        assertEquals(1, partitions.size());
        Assert.assertEquals(
            Lists.newArrayList(
                Pair.of("a",         880L),
                Pair.of("a",      104020L),
                Pair.of("b",        1110L),
                Pair.of("b",       50000L),
                Pair.of("c",        3300L)),
            Util.sorted(partitions.get(0), (o1, o2) -> {
              int cmp = o1.getFirst().compareTo(o2.getFirst());
              if (cmp == 0) {
                cmp = Long.compare(o1.getSecond(), o2.getSecond());
              }
              return cmp;
            }));
      }
    });
  }

  @Test
  public void testSessionWindowing() {
    execute(new AbstractTestCase<
        Pair<String, Integer>,
        Triple<TimeInterval, Integer, HashSet<String>>>() {
      @Override
      protected Partitions<Pair<String, Integer>> getInput() {
        return Partitions.add(
            Pair.of("1-one", 1),
            Pair.of("2-one", 2),
            Pair.of("1-two", 4),
            Pair.of("1-three", 8),
            Pair.of("1-four", 10),
            Pair.of("2-two", 10),
            Pair.of("1-five", 18),
            Pair.of("2-three", 20),
            Pair.of("1-six", 22))
            .build();
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, HashSet<String>>> getOutput
          (Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, HashSet<String>>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .reduceBy(Sets::newHashSet)
                .windowBy(Session.of(Duration.ofSeconds(5)),
                        e -> e.getSecond() * 1_000L)
                .setNumPartitions(1)
                .output();

        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<Integer, HashSet<String>>, Triple<TimeInterval, Integer, HashSet<String>>>)
                (elem, context) -> context.collect(Triple.of((TimeInterval) context.getWindow(), elem.getFirst(), elem.getSecond())))
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Triple<TimeInterval, Integer, HashSet<String>>> partitions) {
        // ~ prepare the output for comparison
        List<String> flat = new ArrayList<>();
        for (Triple<TimeInterval, Integer, HashSet<String>> o : partitions.get(0)) {
          StringBuilder buf = new StringBuilder();
          buf.append("(")
              .append(o.getFirst().getStartMillis() / 1_000L)
              .append("-")
              .append(o.getFirst().getEndMillis() / 1_000L)
              .append("): ");
          buf.append(o.getSecond()).append(": ");
          ArrayList<String> xs = new ArrayList<>(o.getThird());
          xs.sort(Comparator.naturalOrder());
          Joiner.on(", ").appendTo(buf, xs);
          flat.add(buf.toString());
        }
        flat.sort(Comparator.naturalOrder());
        Assert.assertEquals(
            Util.sorted(asList(
                "(1-15): 1: 1-four, 1-one, 1-three, 1-two",
                "(10-15): 2: 2-two",
                "(18-27): 1: 1-five, 1-six",
                "(2-7): 2: 2-one",
                "(20-25): 2: 2-three"),
                Comparator.naturalOrder()),
            flat);
      }
    });
  }

  // ~ ------------------------------------------------------------------------------

  static class SumState extends State<Integer, Integer> {
    private final ValueStorage<Integer> sum;

    SumState(Context<Integer> context, StorageProvider storageProvider) {
      super(context);
      sum = storageProvider.getValueStorage(
          ValueStorageDescriptor.of("sum-state", Integer.class, 0));
    }

    @Override
    public void add(Integer element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush() {
      getContext().collect(sum.get());
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
      protected Partitions<Pair<Integer, Long>> getInput() {
        return Partitions.add(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(3, 12_345L),
            // ~ note: exactly one element for the window on purpose (to test out
            // all is well even in case our `.combineBy` user function is not called.)
            Pair.of(4, 21_456L))
            .build();
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        // ~ this operator is supposed to emit elements internally with a timestamp
        // which equals the emission (== end in this case) of the time window
        Dataset<Pair<String, Integer>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> "")
                .valueBy(Pair::getFirst)
                .combineBy(Sums.ofInts())
                .windowBy(Time.of(Duration.ofSeconds(5)), Pair::getSecond)
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
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        Assert.assertEquals(asList(4, 6), Util.sorted(partitions.get(0), Comparator.naturalOrder()));
      }
    });
  }
}
