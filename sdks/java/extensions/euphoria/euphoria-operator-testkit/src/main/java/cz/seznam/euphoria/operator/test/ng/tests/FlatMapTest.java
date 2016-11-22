
package cz.seznam.euphoria.operator.test.ng.tests;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.operator.test.ng.junit.AbstractOperatorTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code FlatMap}.
 */
public class FlatMapTest extends AbstractOperatorTest {

  @Test
  public void testExplodeOnTwoPartitions() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return FlatMap.of(input)
            .using((Integer e, Context<Integer> c) -> {
              for (int i = 1; i <= e; i++) {
                c.collect(i);
              }
            })
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 3, 2, 1)
        );
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(1, 1, 2, 1, 2, 3), first);
        List<Integer> second = partitions.get(1);
        assertEquals(Arrays.asList(1, 2, 3, 4, 1, 2, 3, 1, 2, 1), second);
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

    });
  }

}
