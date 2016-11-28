package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code FlatMap}.
 */
@Processing(Processing.Type.ALL)
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
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3)
            .add(4, 3, 2, 1)
            .build();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
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
