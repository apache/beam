
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test operator {@code Distinct}.
 */
public class DistinctTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testSimpleDuplicatesWithSinglePartitionBatch(),
        testSimpleDuplicatesWithSinglePartitionStream(),
        testSimpleDuplicatesWithSinglePartitionStreamTwoPartitions()
    );
  }


  /**
   * Test simple duplicates with single output partition.
   */
  TestCase testSimpleDuplicatesWithSinglePartitionBatch() {

    return new AbstractTestCase<Integer, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(1, 2, 3), first);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Distinct.of(input).output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.bounded(
            Arrays.asList(1, 2, 3, 3, 2, 1));
      }

    };
    
  }


  /**
   * Test simple duplicates with single output partition and unbounded input
   * with count window.
   */
  TestCase testSimpleDuplicatesWithSinglePartitionStream() {

    return new AbstractTestCase<Integer, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<Integer> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(1, 2, 3, 2, 1), first);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Distinct.of(input)
            .windowBy(Windowing.Count.of(2))
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(Arrays.asList(1, 2, 3, 3, 2, 1));
      }

    };

  }


  /**
   * Test duplicates with two output partitions and unbounded input
   * with two partitions with count window.
   */
  TestCase testSimpleDuplicatesWithSinglePartitionStreamTwoPartitions() {

    return new AbstractTestCase<Integer, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(2), first);
        List<Integer> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(1, 3), second);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Distinct.of(input)
            .setNumPartitions(2)
            .setPartitioner(e -> e)
            .windowBy(Windowing.Count.of(8))
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 3, 2, 1),
            Arrays.asList(1, 2, 3, 3, 2, 1));
      }

    };

  }
}
