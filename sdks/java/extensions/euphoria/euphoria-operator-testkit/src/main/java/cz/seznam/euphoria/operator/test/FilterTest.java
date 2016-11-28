
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code Filter}.
 */
@Processing(Processing.Type.ALL)
public class FilterTest extends AbstractOperatorTest {

  @Test
  public void testTwoPartitions() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Filter.of(input)
            .by(e -> e % 2 == 0)
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        // two input partitions
        return Partitions
            .add(1, 2, 3, 4, 5 ,6)
            .add(7, 8, 9, 10, 11, 12, 13, 14)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(2, 4, 6), first);
        List<Integer> second = partitions.get(1);
        assertEquals(Arrays.asList(8, 10, 12, 14), second);
      }
    });
  }
}
