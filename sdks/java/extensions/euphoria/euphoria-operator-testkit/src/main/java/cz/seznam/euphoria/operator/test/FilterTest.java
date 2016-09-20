
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Filter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test operator {@code Filter}.
 */
public class FilterTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testTwoPartitionStream()
    );
  }

  TestCase testTwoPartitionStream() {
    return new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Filter.of(input)
            .by(e -> e % 2 == 0)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        // two input partitions
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4, 5 ,6),
            Arrays.asList(7, 8, 9, 10, 11, 12, 13, 14)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(2, 4, 6), first);
        List<Integer> second = partitions.get(1);
        assertEquals(Arrays.asList(8, 10, 12, 14), second);
      }

    };
  }

}
