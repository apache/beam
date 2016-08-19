
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test operator {@code CountByKey}.
 */
public class CountByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
      testOnStream(),
      testOnBatch(),
      testWithCountWindow()
      // FIXME
      // testWithCountWindowAggregating()
    );
  }

  TestCase testOnStream() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(
          Dataset<Integer> input) {
        return CountByKey.of(input)
            .keyBy(e -> e)
            .setPartitioner(e -> e)
            .windowBy(Count.of(7))
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4, 5, 6, 7),
            Arrays.asList(10, 9, 8, 7, 6, 5, 4)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(2, partitions.size());
        // even elements are in first partition
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, 1L),
            Pair.of(4, 2L),
            Pair.of(6, 2L),
            Pair.of(8, 1L),
            Pair.of(10, 1L)
        ), first);
        // odd elements are in second partition
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, 1L),
            Pair.of(3, 1L),
            Pair.of(5, 2L),
            Pair.of(7, 2L),
            Pair.of(9, 1L)
        ), second);
      }

    };
  }

  TestCase testOnBatch() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(
          Dataset<Integer> input) {
        return CountByKey.of(input)
            .keyBy(e -> e)
            .setPartitioner(e -> e)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.bounded(
            Arrays.asList(1, 2, 3, 4, 5, 6, 7),
            Arrays.asList(10, 9, 8, 7, 6, 5, 4)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(2, partitions.size());
        // even elements are in first partition
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, 1L),
            Pair.of(4, 2L),
            Pair.of(6, 2L),
            Pair.of(8, 1L),
            Pair.of(10, 1L)
        ), first);
        // odd elements are in second partition
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, 1L),
            Pair.of(3, 1L),
            Pair.of(5, 2L),
            Pair.of(7, 2L),
            Pair.of(9, 1L)
        ), second);
      }

    };
  }

  TestCase testWithCountWindow() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(
          Dataset<Integer> input) {
        return CountByKey.of(input)
            .keyBy(e -> e)
            .windowBy(Count.of(3))
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(
                1, 2, 1,
                3, 3, 4,
                5, 5, 5,
                5, 5, 6,
                7, 7, 10,
                10, 9, 9,
                9, 9)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(1, partitions.size());
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, 2L),
            Pair.of(2, 1L),
            Pair.of(3, 2L),
            Pair.of(4, 1L),
            Pair.of(5, 3L),
            Pair.of(5, 2L),
            Pair.of(6, 1L),
            Pair.of(7, 2L),
            Pair.of(10, 1L),
            Pair.of(10, 1L),
            Pair.of(9, 2L),
            Pair.of(9, 2L)
        ), first);
      }

    };
  }
}
