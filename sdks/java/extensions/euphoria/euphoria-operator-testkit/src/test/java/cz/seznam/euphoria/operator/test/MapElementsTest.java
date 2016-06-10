
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for operator {@code MapElements}.
 */
public class MapElementsTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testOnTwoPartitions()
    );
  }

  TestCase testOnTwoPartitions() {
    return new AbstractTestCase<Integer, Long>() {

      @Override
      protected Dataset<Long> getOutput(Dataset<Integer> input) {
        return MapElements.of(input)
            .using(e -> (long) e)
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6, 7)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Long>> partitions) {
        assertEquals(2, partitions.size());
        List<Long> first = partitions.get(0);
        assertEquals(Arrays.asList(1L, 2L, 3L), first);
        List<Long> second = partitions.get(1);
        assertEquals(Arrays.asList(4L, 5L, 6L, 7L), second);
      }

    };
  }

}
