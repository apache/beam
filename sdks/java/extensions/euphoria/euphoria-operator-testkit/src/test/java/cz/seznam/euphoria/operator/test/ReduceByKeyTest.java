
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.AlignedWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test operator {@code ReduceByKey}.
 */
public class ReduceByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testCount(true),
        testCount(false),
        testStreamReduceWithWindowing()
    );
  }

  TestCase testCount(boolean batch) {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        if (batch) {
          return ReduceByKey.of(input)
              .keyBy(e -> e)
              .valueBy(e -> 1L)
              .combineBy(Sums.ofLongs())
              .setPartitioner(e -> e % 2)
              .output();
        } else {
          return ReduceByKey.of(input)
              .keyBy(e -> e)
              .valueBy(e -> 1L)
              .combineBy(Sums.ofLongs())
              .setPartitioner(e -> e % 2)
              .windowBy(Windowing.Count.of(30))
              .output();
        }
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        if (batch) {
          return ListDataSource.bounded(
              Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
              Arrays.asList(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
        } else {
          return ListDataSource.unbounded(
              Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
              Arrays.asList(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
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
        assertUnorderedEquals(Arrays.asList(Pair.of(2, 1L), Pair.of(4, 1L),
            Pair.of(6, 2L), Pair.of(8, 2L), Pair.of(10, 2L), Pair.of(12, 1L),
            Pair.of(14, 1L)), first);
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(Pair.of(1, 1L), Pair.of(3, 1L),
            Pair.of(5, 2L), Pair.of(7, 2L), Pair.of(9, 2L), Pair.of(11, 1L),
            Pair.of(13, 1L), Pair.of(15, 1L)), second);
      }

    };
  }
  
  static class TestWindowContext extends WindowContext<Void, Integer> {
   
    public TestWindowContext(int label) {
      super(WindowID.aligned(label));
    }
    
  }

  static class TestWindowing
      implements AlignedWindowing<Integer, Integer, TestWindowContext> {

    @Override
    public Set<WindowID<Void, Integer>> assignWindows(Integer input) {
      // we group items into windows by 4 elements
      return Collections.singleton(WindowID.aligned(input / 4));
    }

    @Override
    public TestWindowContext createWindowContext(WindowID<Void, Integer> wid) {
      return new TestWindowContext(wid.getLabel());
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

}
