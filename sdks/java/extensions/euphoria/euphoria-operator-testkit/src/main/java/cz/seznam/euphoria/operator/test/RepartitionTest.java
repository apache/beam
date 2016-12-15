
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test for operator {@code Repartition}.
 */
@Processing(Processing.Type.ALL)
public class RepartitionTest extends AbstractOperatorTest {

  @Test
  public void testTwoToOne() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(1)
            .output();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(1, partitions.size());
        assertUnorderedEquals(Arrays.asList(-1, 2, -3, 4, 5, 6, 7), partitions.get(0));
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(-1, 2, -3, 4)
            .add(5, 6, 7)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }
    });
  }

  @Test
  public void testOneToTwo() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(2, partitions.size());
        assertEquals(Arrays.asList(2, 4, 6), partitions.get(0));
        assertEquals(Arrays.asList(-1, -3, 5, 7), partitions.get(1));
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(-1, 2, -3, 4, 5, 6, 7).build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }
    });
  }

  @Test
  public void testThreeToTwo() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {
      
      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Repartition.of(input)
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(2, partitions.size());
        assertUnorderedEquals(Arrays.asList(2, 4, 6, 8, 10, 12), partitions.get(0));
        assertUnorderedEquals(Arrays.asList(1, 3, 5, 7, 9, 11), partitions.get(1));
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3, 4)
            .add(5, 6, 7)
            .add(8, 9, 10, 11, 12)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }
    });
  }
}
