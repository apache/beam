package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cz.seznam.euphoria.operator.test.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code ReduceStateByKey}.
 */
public class ReduceStateByKeyTest extends OperatorTest {

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

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testBatchSortNoWindow(),
        testSortWindowed(false),
        testSortWindowed(true),
        testSessionWindowing0()
    );
  }

  static class SortState extends State<Integer, Integer> {

    final ListStorage<Integer> data;

    SortState(        
        Context<Integer> c,
        StorageProvider storageProvider) {

      super(c, storageProvider);
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

    static SortState combine(Iterable<SortState> states) {
      SortState ret = null;
      for (SortState state : states) {
        if (ret == null) {
          ret = new SortState(
              state.getContext(),
              state.getStorageProvider());
        }
        ret.data.addAll(state.data.get());
      }
      return ret;
    }

  }

  TestCase testBatchSortNoWindow() {
    return new AbstractTestCase<Integer, Pair<String, Integer>>() {

      @Override
      protected Dataset<Pair<String, Integer>> getOutput(Dataset<Integer> input) {
        return ReduceStateByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .combineStateBy(SortState::combine)
            .setNumPartitions(1)
            .setPartitioner(e -> 0)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.bounded(
            Arrays.asList(9, 8, 7, 5, 4),
            Arrays.asList(1, 2, 6, 3, 5)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Pair<String, Integer>>> partitions) {
        assertEquals(1, partitions.size());
        List<Pair<String, Integer>> first = partitions.get(0);
        assertEquals(10, first.size());
        List<Integer> values = first.stream().map(Pair::getSecond)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9), values);
      }

    };
  }

  TestCase testSortWindowed(boolean batch) {
    return new AbstractTestCase<Integer, WPair<Integer, Integer, Integer>>() {

      @Override
      protected Dataset<WPair<Integer, Integer, Integer>> getOutput(Dataset<Integer> input) {
        Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .combineStateBy(SortState::combine)
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
      protected DataSource<Integer> getDataSource() {
        if (batch) {
          return ListDataSource.bounded(
              Arrays.asList(9, 8, 10, 11 /* third window, key 0, 2, 1, 2 */,
                  5, 6, 7, 4 /* second window, key 2, 0, 1, 1 */),
              Arrays.asList(8, 9 /* third window, key 2, 0 */,
                  6, 5 /* second window, key 0, 2 */));
        } else {
          return ListDataSource.unbounded(
              Arrays.asList(9, 8, 10, 11 /* third window, key 0, 2, 1, 2 */,
                  5, 6, 7, 4 /* second window, key 2, 0, 1, 1 */),
              Arrays.asList(8, 9 /* third window, key 2, 0 */,
                  6, 5 /* second window, key 0, 2 */));
        }
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<WPair<Integer, Integer, Integer>>> partitions) {
        assertEquals(1, partitions.size());
        List<WPair<Integer, Integer, Integer>> first = partitions.get(0);
        assertEquals(12, first.size());

        // map (window, key) -> list(data)
        Map<Pair<Integer, Integer>, List<WPair<Integer, Integer, Integer>>>
            windowKeyMap = first.stream()
            .collect(Collectors.groupingBy(p -> Pair.of(p.getWindow(), p.getKey())));

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

    };
  }

  // ---------------------------------------------------------------------------------

  private static class AccState<VALUE> extends State<VALUE, VALUE> {
    final ListStorage<VALUE> reducableValues;
    AccState(Context<VALUE> context,
             StorageProvider storageProvider)
    {
      super(context, storageProvider);
      reducableValues = storageProvider.getListStorage(
          ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
    }

    @Override
    public void flush() {
      for (VALUE value : reducableValues.get()) {
        getContext().collect(value);
      }
    }

    void add(AccState other) {
      this.reducableValues.addAll(other.reducableValues.get());
    }

    @Override
    public void close() {
      reducableValues.clear();
    }

    public static <VALUE> AccState<VALUE> combine(Iterable<AccState<VALUE>> xs) {
      Iterator<AccState<VALUE>> iter = xs.iterator();
      AccState<VALUE> first = iter.next();
      while (iter.hasNext()) {
        first.add(iter.next());
      }
      return first;
    }
  }

  TestCase<Triple<TimeInterval, Integer, String>> testSessionWindowing0() {
    return new AbstractTestCase<Pair<String, Integer>, Triple<TimeInterval, Integer, String>>() {
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
      protected Dataset<Triple<TimeInterval, Integer, String>>
      getOutput(Dataset<Pair<String, Integer>> input) {
        Dataset<Pair<Integer, String>> reduced =
            ReduceStateByKey.of(input)
                .keyBy(e -> e.getFirst().charAt(0) - '0')
                .valueBy(Pair::getFirst)
                .stateFactory((StateFactory<String, AccState<String>>) AccState::new)
                .combineStateBy(AccState::combine)
                .windowBy(Session.of(Duration.ofSeconds(5))
                    .using((Pair<String, Integer> e) -> e.getSecond() * 1_000L))
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
      public void validate(List<List<Triple<TimeInterval, Integer, String>>> partitions) {
        // ~ prepare the output for comparison
        List<String> flat = new ArrayList<>();
        for (Triple<TimeInterval, Integer, String> o : partitions.get(0)) {
          String buf = "(" +
              o.getFirst().getStartMillis() / 1_000L +
              "-" +
              o.getFirst().getEndMillis() / 1_000L +
              "): " +
              o.getSecond() + ": " + o.getThird();
          flat.add(buf);
        }
        flat.sort(Comparator.naturalOrder());
        assertEquals(
            sorted(asList(
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
            flat);
      }
    };
  }

}
