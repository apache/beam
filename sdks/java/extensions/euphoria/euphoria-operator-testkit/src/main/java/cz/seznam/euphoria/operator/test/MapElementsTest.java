
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
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
    return new AbstractTestCase<Integer, String>() {

      @Override
      protected Dataset<String> getOutput(Dataset<Integer> input) {
        return MapElements.of(input)
            .using(e -> String.valueOf(e))
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.bounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6, 7)
        );
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<String>> partitions) {
        assertEquals(2, partitions.size());
        // the ordering of partitions is undefined here, because
        // the mapping from input to output partition numbers might
        // be random
        assertEquals(Sets.newHashSet(
            Arrays.asList("1", "2", "3"),
            Arrays.asList("4", "5", "6", "7")),
            Sets.newHashSet(partitions));
      }

    };
  }

}
