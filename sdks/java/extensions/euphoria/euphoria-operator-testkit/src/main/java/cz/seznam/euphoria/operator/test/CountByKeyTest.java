
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import java.time.Duration;
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
      testWithEventTimeWindow()
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
            .setPartitioner(i -> i)
            .windowBy(Time.of(Duration.ofSeconds(1)))
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

  TestCase testWithEventTimeWindow() {
    return new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(
          Dataset<Pair<Integer, Long>> input) {
        return CountByKey.of(input)
            .keyBy(Pair::getFirst)
            .windowBy(Time.of(Duration.ofSeconds(1)), Pair::getSecond)
            .output();
      }

      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(
                Pair.of(1, 200L), Pair.of(2, 500L), Pair.of(1, 800L),
                Pair.of(3, 1400L), Pair.of(3, 1200L), Pair.of(4, 1800L),
                Pair.of(5, 2100L), Pair.of(5, 2300L), Pair.of(5, 2700L),
                Pair.of(5, 3500L), Pair.of(5, 3300L), Pair.of(6, 3800L),
                Pair.of(7, 4400L), Pair.of(7, 4500L), Pair.of(10, 4600L),
                Pair.of(10, 5100L), Pair.of(9, 5200L), Pair.of(9, 5500L),
                Pair.of(9, 6300L), Pair.of(9, 6700L))
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
