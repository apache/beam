
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.SumByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test operator {@code SumByKey}.
 */
public class SumByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testTwoPartitions()
    );
  }

  TestCase testTwoPartitions() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        return SumByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> (long) e)
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 4, 5),
            Arrays.asList(6, 7, 8, 9)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(2, partitions.size());
        assertEquals(1, partitions.get(0).size());
        assertEquals(Pair.of(0, 20L), partitions.get(0).get(0));
        assertEquals(1, partitions.get(1).size());
        assertEquals(Pair.of(1, 25L), partitions.get(1).get(0));
      }

    };
  }

}
