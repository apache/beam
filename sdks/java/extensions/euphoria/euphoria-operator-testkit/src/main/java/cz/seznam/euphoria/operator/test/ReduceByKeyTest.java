
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
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
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
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;

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

import static cz.seznam.euphoria.operator.test.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Test operator {@code ReduceByKey}.
 */
public class ReduceByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testStreamReduceWithWindowing(),
        testReductionType0(false),
        testReductionType0(true),
        testEventTime(false),
        testEventTime(true),
        testReduceWithoutWindowing(false),
        testReduceWithoutWindowing(true),
        testMergingAndTriggering(false),
        testMergingAndTriggering(true),
        testSessionWindowing(false),
        testSessionWindowing(true),
        testElementTimestamp(false),
        testElementTimestamp(true),
        testElementTimestampEarlyTriggeredStreaming()
    );
  }

  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  TestCase<Pair<Integer, HashSet<Integer>>> testReductionType0(boolean batch) {
    return new AbstractTestCase<Integer, Pair<Integer, HashSet<Integer>>>() {
      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.of(batch, asList(1, 2, 3, 4, 5, 6, 7, 9));
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
      public void validate(List<List<Pair<Integer, HashSet<Integer>>>> partitions) {
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
    };
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
            .windowBy(Time.of(Duration.ofSeconds(1)), Pair::getSecond)
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
    public Trigger<IntWindow> getTrigger() {
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
    public Set<CWindow> assignWindowsToElement(WindowedElement<?, T> input) {
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

  TestCase<Pair<String, Long>> testMergingAndTriggering(boolean batch) {
    return new AbstractTestCase<Pair<String, Long>, Pair<String, Long>>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      protected DataSource<Pair<String, Long>> getDataSource() {
        return ListDataSource.of(batch, asList(
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
            Pair.of("a",      80L)
        ));
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

      @Override
      public void validate(List<List<Pair<String, Long>>> partitions) {
        assertEquals(1, partitions.size());
        assertEquals(
            Lists.newArrayList(
                Pair.of("a",         880L),
                Pair.of("a",      104020L),
                Pair.of("b",        1110L),
                Pair.of("b",       50000L),
                Pair.of("c",        3300L)),
            sorted(partitions.get(0), (o1, o2) -> {
              int cmp = o1.getFirst().compareTo(o2.getFirst());
              if (cmp == 0) {
                cmp = Long.compare(o1.getSecond(), o2.getSecond());
              }
              return cmp;
            }));
      }
    };
  }

  TestCase<Triple<TimeInterval,Integer,HashSet<String>>>
  testSessionWindowing(boolean batch) {
    return new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, HashSet<String>>>() {
      @Override
      protected DataSource<Pair<String, Integer>> getDataSource() {
        return ListDataSource.of(batch, asList(
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

  // ~ ------------------------------------------------------------------------------

  static class SumState extends State<Integer, Integer> {
    private final ValueStorage<Integer> sum;

    SumState(Context<Integer> context, StorageProvider storageProvider) {
      super(context, storageProvider);
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

    static SumState combine(Iterable<SumState> states) {
      SumState target = null;
      for (SumState state : states) {
        if (target == null) {
          target = new SumState(state.getContext(), state.getStorageProvider());
        }
        target.add(state.sum.get());
      }
      return target;
    }
  }

  TestCase<Integer> testElementTimestamp(boolean batch) {
    class AssertingWindowing<T> implements Windowing<T, TimeInterval> {
      @Override
      public Set<TimeInterval> assignWindowsToElement(WindowedElement<?, T> input) {
        // FIXME: #16648 once WindowedElement has the timestamp make the same
        // assumption on that timestamp as in the trigger below
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
            assertTrue(time == 15_000L || time == 25_000L);
            return super.onElement(time, window, ctx);
          }
        };
      }
    }

    return new AbstractTestCase<Pair<Integer, Long>, Integer>() {
      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        return ListDataSource.of(batch, asList(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(3, 12_345L),
            // ~ note: exactly one element for the window on purpose (to test out
            // all is well even in case our `.combineBy` user function is not called.)
            Pair.of(4, 21_456L)));
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
                .combineStateBy(SumState::combine)
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
      public void validate(List<List<Integer>> partitions) {
        assertEquals(asList(4, 6), sorted(partitions.get(0), Comparator.naturalOrder()));
      }
    };
  }

  static List<Long> TETETS_SEEN_TIMES = Collections.synchronizedList(new ArrayList<>());

  TestCase<Integer> testElementTimestampEarlyTriggeredStreaming() {
    class TimeCollectingWindowing<T> implements Windowing<T, TimeInterval> {
      @Override
      public Set<TimeInterval> assignWindowsToElement(WindowedElement<?, T> input) {
        // FIXME: #16648 once WindowedElement has the timestamp make the same
        // assumption on that timestamp as in the trigger below
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
            TETETS_SEEN_TIMES.add(time);
            return super.onElement(time, window, ctx);
          }
        };
      }
    }

    TETETS_SEEN_TIMES.clear();
    return new AbstractTestCase<Pair<Integer, Long>, Integer>() {
      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        return ListDataSource.unbounded(asList(
            // ~ Pair.of(value, time)
            Pair.of(1, 10_123L),
            Pair.of(2, 11_234L),
            Pair.of(8, 16_345L),
            Pair.of(9, 17_789L),
            // ~ note: exactly one element for the window on purpose (to test out
            // all is well even in case our `.combineBy` user function is not called.)
            Pair.of(50, 21_456L)));
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        // ~ this operator is supposed to emit elements internally with a
        // timestamp which equals the emission of the time windows
        Dataset<Pair<String, Integer>> reduced =
            ReduceByKey.of(input)
                .keyBy(e -> "")
                .valueBy(Pair::getFirst)
                .combineBy(Sums.ofInts())
                .windowBy(Time.of(Duration.ofSeconds(10))
                    .earlyTriggering(Duration.ofSeconds(5)), Pair::getSecond)
                .output();
        // ~ now use a custom windowing with a trigger which does
        // the assertions subject to this test (use RSBK which has to
        // use triggering, unlike an optimized RBK)
        Dataset<Pair<String, Integer>> output =
            ReduceStateByKey.of(reduced)
                .keyBy(Pair::getFirst)
                .valueBy(Pair::getSecond)
                .stateFactory(SumState::new)
                .combineStateBy(SumState::combine)
                .windowBy(new TimeCollectingWindowing<>())
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
      public void validate(List<List<Integer>> partitions) {
        // ~ the last window (containing the last element) gets emitted twice due to
        // being "triggered" twice (early triggering plus the end of window) and being
        // at the "end of the stream". this is an abnormal situation as streams are
        // never-ending.
        assertTrue(partitions.get(0).size() > 3);

        ArrayList<Long> times = new ArrayList<>(TETETS_SEEN_TIMES);
        times.sort(Comparator.naturalOrder());
        assertEquals(asList(15_000L, 20_000L, 25_000L, 30_000L), times);
      }
    };
  }
}
