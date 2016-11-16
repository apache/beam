
package cz.seznam.euphoria.operator.test.ng.tests;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.ng.junit.AbstractOperatorTest;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code Distinct}.
 */
public class DistinctTest extends AbstractOperatorTest {

  /**
   * Test simple duplicates with single output partition.
   */
  @Test
  public void testSimpleDuplicatesWithSinglePartitionBatch() throws Exception {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(1, 2, 3), first);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return Distinct.of(input).output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.bounded(
            Arrays.asList(1, 2, 3, 3, 2, 1));
      }
    });
  }


  /**
   * Test simple duplicates with single output partition and unbounded input
   * with count window.
   */
  @Test
  public void testSimpleDuplicatesWithSinglePartitionStream() throws Exception {
    execute(new AbstractTestCase<Pair<Integer, Long>, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(1, partitions.size());
        List<Integer> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(1, 2, 3, 2, 1), first);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        return Distinct.of(input)
            .mapped(Pair::getFirst)
            .windowBy(Time.of(Duration.ofSeconds(1))
                .using(Pair::getSecond))
            .output();
      }

      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(Pair.of(1, 100L), Pair.of(2, 300L), // first window
                Pair.of(3, 1200L), Pair.of(3, 1500L), // second window
                Pair.of(2, 2200L), Pair.of(1, 2700L)));
      }
    });
  }


  /**
   * Test duplicates with two output partitions and unbounded input
   * with two partitions with count window.
   */
  @Test
  public void testSimpleDuplicatesWithSinglePartitionStreamTwoPartitions() throws Exception {
    execute(new AbstractTestCase<Pair<Integer, Long>, Integer>() {

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Integer>> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertUnorderedEquals("Array should be equals to [2], got " + first,
            Arrays.asList(2), first);
        List<Integer> second = partitions.get(1);
        assertUnorderedEquals("Array should be equals to [1, 3], got " + second,
            Arrays.asList(1, 3), second);
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
        return Distinct.of(input)
            .mapped(Pair::getFirst)
            .setNumPartitions(2)
            .setPartitioner(e -> e)
            .windowBy(Time.of(Duration.ofSeconds(1)).using(Pair::getSecond))
            .output();
      }

      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        return ListDataSource.unbounded(
            asTimedList(100, 1, 2, 3, 3, 2, 1),
            asTimedList(100, 1, 2, 3, 3, 2, 1));
      }
    });
  }

  private List<Pair<Integer, Long>> asTimedList(long step, Integer ... values) {
    List<Pair<Integer, Long>> ret = new ArrayList<>(values.length);
    long i = step;
    for (Integer v : values) {
      ret.add(Pair.of(v, i));
      i += step;
    }
    return ret;
  }
}
