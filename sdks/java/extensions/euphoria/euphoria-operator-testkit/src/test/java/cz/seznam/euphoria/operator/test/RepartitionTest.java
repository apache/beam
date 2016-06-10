
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Repartition;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for operator {@code Repartition}.
 */
public class RepartitionTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testTwoToOne(),
        testOneToTwo(),
        testThreeToTwo()
    );
  }

  TestCase testTwoToOne() {
    return new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(1)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4),
            Arrays.asList(5, 6, 7)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(1, partitions.size());
        assertUnorderedEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), partitions.get(0));
      }

    };
  }

  TestCase testOneToTwo() {
    return new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4, 5, 6, 7)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        assertEquals(Arrays.asList(2, 4, 6), partitions.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7), partitions.get(1));
      }

    };

  }

  TestCase testThreeToTwo() {
    return new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4),
            Arrays.asList(5, 6, 7),
            Arrays.asList(8, 9, 10, 11, 12)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        assertUnorderedEquals(Arrays.asList(2, 4, 6, 8, 10, 12), partitions.get(0));
        assertUnorderedEquals(Arrays.asList(1, 3, 5, 7, 9, 11), partitions.get(1));
      }

    };

  }

}
