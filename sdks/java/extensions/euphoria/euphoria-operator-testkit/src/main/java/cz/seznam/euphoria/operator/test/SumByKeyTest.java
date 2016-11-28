package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.operator.SumByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code SumByKey}.
 */
@Processing(Processing.Type.ALL)
public class SumByKeyTest extends AbstractOperatorTest {

  @Test
  public void testTwoPartitions() throws Exception {
    execute(new AbstractTestCase<Integer, Pair<Integer, Long>>() {
      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        return SumByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> (long) e)
            .setPartitioner(e -> e % 2)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3, 4, 5)
            .add(6, 7, 8, 9)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, Long>> partitions) {
        assertEquals(2, partitions.size());
        assertEquals(1, partitions.get(0).size());
        assertEquals(Pair.of(0, 20L), partitions.get(0).get(0));
        assertEquals(1, partitions.get(1).size());
        assertEquals(Pair.of(1, 25L), partitions.get(1).get(0));
      }
    });
  }

}
