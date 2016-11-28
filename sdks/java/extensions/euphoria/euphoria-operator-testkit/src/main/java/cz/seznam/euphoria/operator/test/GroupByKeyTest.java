package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.GroupByKey;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for operator {@code GroupByKey}.
 */
@Processing(Processing.Type.UNBOUNDED) // FIXME make Type.ALL #16916 euphoria-flink: GroupByKey failing on Repartition
public class GroupByKeyTest extends AbstractOperatorTest {

  @Test
  public void testGroupByMap() throws Exception {
    execute(new AbstractTestCase<Integer, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(Dataset<Integer> input) {
        GroupedDataset<Integer, Integer> grouped = GroupByKey.of(input)
            .keyBy(e -> e % 3)
            .setPartitioner(k -> k % 2)
            .output();

        return MapElements.of(grouped)
            .using(p -> Pair.of(p.getKey(), String.valueOf(p.getValue())))
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3, 4, 5, 6, 7, 8)
            .add(8, 7, 6, 5, 4, 3, 2, 1)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, String>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(2, "2"), Pair.of(2, "2"), Pair.of(0, "3"),
            Pair.of(0, "3"), Pair.of(2, "5"), Pair.of(2, "5"), Pair.of(0, "6"), Pair.of(0, "6"),
            Pair.of(2, "8"), Pair.of(2, "8")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(Pair.of(1, "1"), Pair.of(1, "1"), Pair.of(1, "4"),
            Pair.of(1, "4"), Pair.of(1, "7"), Pair.of(1, "7")), second);
      }
    });
  }

  @Test
  public void testGroupByReduce() throws Exception {
    execute(new AbstractTestCase<Integer, Pair<Integer, Integer>>() {

      @Override
      protected Dataset<Pair<Integer, Integer>> getOutput(Dataset<Integer> input) {
        GroupedDataset<Integer, Integer> grouped = GroupByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> e % 2)
            .output();
        return Distinct.of(grouped)
            .setPartitioner(p -> p.getKey() % 2)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3, 4)
            .add(1, 2, 3, 4, 6)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, Integer>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, Integer>> first = partitions.get(0);
        assertUnorderedEquals(
            Arrays.asList(Pair.of(0, 1), Pair.of(0, 0), Pair.of(2, 0)), first);
        List<Pair<Integer, Integer>> second = partitions.get(1);
        assertUnorderedEquals(
            Arrays.asList(Pair.of(1, 1), Pair.of(1, 0)), second);
      }
    });
  }
}
