package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test for operator {@code Union}
 */
@Processing(Processing.Type.ALL)
public class UnionTest extends AbstractOperatorTest {

  @Test
  public void testUnionAndMap() throws Exception {
    execute(new TestCase<Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
        Dataset<Integer> first = flow.createInput(ListDataSource.of(bounded,
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6)));

        Dataset<Integer> second = flow.createInput(ListDataSource.of(bounded,
            Arrays.asList(7, 8, 9),
            Arrays.asList(10, 11, 12)));

        Dataset<Integer> union = Union.of(first, second).output();
        return Repartition.of(union).setNumPartitions(1).output();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(1, partitions.size());
        assertUnorderedEquals(
            Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
            partitions.get(0));
      }
    });
  }

}
