package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test operator {@code ReduceStateByKey}.
 */
public class ReduceStateByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testBatchSort(),
        testSortWindowed()
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

  TestCase testBatchSort() {
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

  TestCase testSortWindowed() {
    return new AbstractTestCase<Integer, Pair<Integer, Integer>>() {

      @Override
      protected Dataset<Pair<Integer, Integer>> getOutput(Dataset<Integer> input) {
        return ReduceStateByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> e)
            .stateFactory(SortState::new)
            .combineStateBy(SortState::combine)
            .setNumPartitions(1)
            .setPartitioner(e -> 0)
            .windowBy(new ReduceByKeyTest.TestWindowing())
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(9, 8, 10, 11 /* third window, key 0, 2, 1, 2 */,
                          5, 6, 7, 4 /* second window, key 2, 0, 1, 1 */),
            Arrays.asList(8, 9 /* third window, key 2, 0 */,
                          6, 5 /* second window, key 0, 2 */)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Pair<Integer, Integer>>> partitions) {
        assertEquals(1, partitions.size());
        List<Pair<Integer, Integer>> first = partitions.get(0);
        assertEquals(12, first.size());

        // map (window, key) -> list(data)
        @SuppressWarnings("unchecked")
        Map<Pair<Integer, Integer>, List<WindowedPair<Integer, Integer, Integer>>>
            windowKeyMap = first.stream()
            .map(p -> (WindowedPair<Integer, Integer, Integer>) p)
            .collect(Collectors.groupingBy(p -> Pair.of(p.getWindowLabel(), p.getKey())));

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

      List<Integer> flatten(List<WindowedPair<Integer, Integer, Integer>> l) {
        return l.stream().map(Pair::getSecond).collect(Collectors.toList());
      }

    };
  }


}
