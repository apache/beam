
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
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
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test operator {@code ReduceByKey}.
 */
public class ReduceByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testEventTime(true),
        testEventTime(false),
        testStreamReduceWithWindowing(),
        testReduceWithoutWindowing(true),
        testReduceWithoutWindowing(false),
        testSessionWindowing0(),
        testMergingAndTriggering()
    );
  }

  TestCase testEventTime(boolean batch) {
    return new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Pair<Integer, Long>> input) {
        return ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .setPartitioner(e -> e % 2)
            .windowBy(Time.of(Duration.ofSeconds(1))
                .using(Pair::getSecond))
            .output();
      }

      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        if (batch) {
          return ListDataSource.bounded(
              Arrays.asList(Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
                  Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
                  Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
                  Pair.of(2, 3300L)),
              Arrays.asList(Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
                  Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
                  Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
                  Pair.of(4, 3300L), Pair.of(3, 3600L)));
        } else {
          return ListDataSource.unbounded(
              Arrays.asList(Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
                  Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
                  Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
                  Pair.of(2, 3300L)),
              Arrays.asList(Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
                  Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
                  Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
                  Pair.of(4, 3300L), Pair.of(3, 3600L)));
        }
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
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

    };
  }

  static class TestWindowing implements Windowing<Integer, IntWindow> {
    @Override
    public Set<IntWindow> assignWindowsToElement(WindowedElement<?, Integer> input) {
      return Collections.singleton(new IntWindow(input.get() / 4));
    }

    @Override
    public Trigger<Integer, IntWindow> getTrigger() {
      return NoopTrigger.get();
    }
  }

  TestCase testStreamReduceWithWindowing() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

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
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3 /* first window, keys 1, 2, 0 */,
                          4, 5, 6, 7 /* second window, keys 1, 2, 0, 1 */,
                          8, 9, 10 /* third window, keys 2, 0, 1 */),
            Arrays.asList(5, 6, 7 /* second window, keys 2, 0, 1 */,
                          8, 9, 10, 11 /* third window, keys 2, 0, 1, 2 */,
                          12, 13, 14, 15 /* fourth window, keys 0, 1, 2, 0 */));
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
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


    };
  }

  TestCase testReduceWithoutWindowing(boolean batch) {
    return new AbstractTestCase<String, Pair<String, Long>>() {
      @Override
      protected DataSource<String> getDataSource() {
        String[] words =
            "one two three four one two three four one two three one two one".split(" ");
        return batch
            ? ListDataSource.bounded(Arrays.asList(words))
            : ListDataSource.unbounded(Arrays.asList(words));
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Pair<String, Long>>> partitions) {
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
    };
  }

  // ----------------------------------------------------------------------------

  // ~ every instance is unique: this allows us to exercise merging
  static final class CWindow extends Window {

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
    public String toString() {
      return "CWindow{" +
          "bucket=" + bucket +
          ", identity=" + _id +
          '}';
    }
  }

  // count windowing; firing based on window.bucket (size of the window)
  static final class CWindowTrigger<T> implements Trigger<T, CWindow> {
    private final ValueStorageDescriptor<Long> countDesc =
        ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y);

    @Override
    public TriggerResult onElement(long time, T element, CWindow w, TriggerContext ctx) {
      ValueStorage<Long> cnt = ctx.getValueStorage(countDesc);
      cnt.set(cnt.get() + 1);
      if (cnt.get() >= w.bucket) {
        return TriggerResult.FLUSH_AND_PURGE;
      }
      return TriggerResult.NOOP;
    }

    @Override
    public TriggerResult onTimeEvent(long time, CWindow w, TriggerContext ctx) {
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
    UnaryFunction<T, Integer> typeFn;

    CWindowing(UnaryFunction<T, Integer> typeFn) {
      this.typeFn = typeFn;
    }

    @Override
    public Set<CWindow> assignWindowsToElement(WindowedElement<?, T> input) {
      return Sets.newHashSet(new CWindow(typeFn.apply(input.get())));
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
    public Trigger<T, CWindow> getTrigger() {
      return new CWindowTrigger<T>();
    }
  }

  TestCase<Triple<String,Integer,Long>> testMergingAndTriggering() {
    return new AbstractTestCase<Triple<String, Integer, Long>, Triple<String, Integer, Long>>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      protected DataSource<Triple<String, Integer, Long>> getDataSource() {
        return ListDataSource.unbounded(asList(
            Triple.of("a", 3,      10L),
            Triple.of("a", 2,      20L),
            Triple.of("a", 2,     200L),
            Triple.of("a", 2,      30L),
            Triple.of("b", 3,      10L),
            Triple.of("b", 4,      20L),
            Triple.of("a", 3,     100L),
            Triple.of("c", 3,   1_000L),
            Triple.of("b", 4,     200L),
            Triple.of("a", 3,   1_000L),
            Triple.of("a", 3,  10_000L),
            Triple.of("b", 4,   2_000L),
            Triple.of("b", 4,  20_000L),
            Triple.of("b", 4, 200_000L),
            Triple.of("a", 3, 100_000L)
        ));
      }

      @Override
      protected Dataset<Triple<String, Integer, Long>>
      getOutput(Dataset<Triple<String, Integer, Long>> input) {
        Dataset<Pair<String, Long>> reduced =
            ReduceByKey.of(input)
                .keyBy(Triple::getFirst)
                .valueBy(Triple::getThird)
                .combineBy(Sums.ofLongs())
                .windowBy(new CWindowing<>(Triple::getSecond))
                .output();
        return FlatMap.of(reduced)
            .using((UnaryFunctor<Pair<String, Long>, Triple<String, Integer, Long>>)
                (elem, context) -> {
                  int bucket = ((CWindow) context.getWindow()).bucket;
                  context.collect(Triple.of(elem.getFirst(), bucket, elem.getSecond()));
                })
            .output();
      }

      @Override
      public void validate(List<List<Triple<String, Integer, Long>>> partitions) {
        assertEquals(1, partitions.size());
        // expected:
        //   w:3  => [a:10L + a:100L + a:1000L], [a:10000L + a:100000L]
        //   w:2  => [a:20L + a:200L] [a:30L]
        //   w:3  => [b:10L]
        //   w:4  => [b:20L + b:200L + b:2000L + b:20000L]
        //   w:4  => [b:200000L]
        //   w:3  => [c:1000L]
        assertEquals(
            Lists.newArrayList(
                Triple.of("a",  2,      30L),
                Triple.of("a",  2,     220L),
                Triple.of("a",  3,   1_110L),
                Triple.of("a",  3, 110_000L),
                Triple.of("b",  3,      10L),
                Triple.of("b",  4,  22_220L),
                Triple.of("b",  4, 200_000L),
                Triple.of("c",  3,   1_000L)),
            sorted(partitions.get(0), (o1, o2) -> {
              int cmp = o1.getFirst().compareTo(o2.getFirst());
              if (cmp == 0) {
                cmp = Long.compare(o1.getThird(), o2.getThird());
              }
              return cmp;
            }));
      }
    };
  }

  public static <T> List<T> sorted(Collection<T> xs, Comparator<T> c) {
    ArrayList<T> list = new ArrayList<>(xs);
    list.sort(c);
    return list;
  }

  TestCase<Triple<TimeInterval,Integer,HashSet<String>>> testSessionWindowing0() {
    return new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, HashSet<String>>>() {
      @Override
      protected DataSource<Pair<String, Integer>> getDataSource() {
        return ListDataSource.unbounded(asList(
            Pair.of("1-one",   1),
            Pair.of("2-one",   2),
            Pair.of("1-two",   4),
            Pair.of("1-three", 8),
            Pair.of("1-four",  10),
            Pair.of("2-two",   10),
            Pair.of("1-five",  18),
            Pair.of("2-three", 20),
            Pair.of("1-six",   22)));
      }

      @Override
      protected Dataset<Triple<TimeInterval, Integer, HashSet<String>>> getOutput
          (Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, HashSet<String>>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .reduceBy(Sets::newHashSet)
                .windowBy(Session.of(Duration.ofSeconds(5))
                    .using(e -> e.getSecond() * 1_000L))
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
      public void validate(List<List<Triple<TimeInterval, Integer, HashSet<String>>>> partitions) {
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
        assertEquals(
            sorted(asList(
                "(1-15): 1: 1-four, 1-one, 1-three, 1-two",
                "(10-15): 2: 2-two",
                "(18-27): 1: 1-five, 1-six",
                "(2-7): 2: 2-one",
                "(20-25): 2: 2-three"),
                Comparator.naturalOrder()),
            flat);
      }
    };
  }
}
