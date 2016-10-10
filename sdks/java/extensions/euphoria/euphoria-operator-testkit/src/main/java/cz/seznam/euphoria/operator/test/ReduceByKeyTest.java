
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code ReduceByKey}.
 */
public class ReduceByKeyTest extends OperatorTest {

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        testEventTime(true),
        testEventTime(false),
        testStreamReduceWithWindowing(),
        testReduceWithoutWindowing(true),
        testReduceWithoutWindowing(false)
    );
  }

  TestCase testEventTime(boolean batch) {
    return new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Pair<Integer, Long>> input) {
        return ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .setPartitioner(e -> e % 2)
            .windowBy(Time.of(Duration.ofSeconds(1))
                .using(Pair::getSecond))
            .output();
      }

      @Override
      protected DataSource<Pair<Integer, Long>> getDataSource() {
        if (batch) {
          return ListDataSource.bounded(
              Arrays.asList(Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
                  Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
                  Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
                  Pair.of(2, 3300L)),
              Arrays.asList(Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
                  Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
                  Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
                  Pair.of(4, 3300L), Pair.of(3, 3600L)));
        } else {
          return ListDataSource.unbounded(
              Arrays.asList(Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
                  Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
                  Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
                  Pair.of(2, 3300L)),
              Arrays.asList(Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
                  Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
                  Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
                  Pair.of(4, 3300L), Pair.of(3, 3600L)));
        }
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, 2L), Pair.of(4, 1L),  // first window
            Pair.of(2, 2L), Pair.of(4, 1L),  // second window
            Pair.of(2, 1L), Pair.of(4, 1L),  // third window
            Pair.of(2, 1L), Pair.of(4, 1L)), // fourth window
            first);
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, 1L), Pair.of(3, 2L),  // first window
            Pair.of(1, 1L), Pair.of(3, 2L),  // second window
            Pair.of(1, 2L), Pair.of(3, 2L),  // third window
            Pair.of(3, 1L)),                 // fourth window
            second);
      }

    };
  }
  
  static class TestWindowContext extends WindowContext<Integer> {

    public TestWindowContext(int label) {
      super(new WindowID<>(label));
    }
  }

  static class TestWindowing
      implements Windowing<Integer, Integer, TestWindowContext> {

    @Override
    public Set<WindowID<Integer>> assignWindowsToElement(
        WindowedElement<?, Integer> input) {
      return Collections.singleton(new WindowID<>(input.get() / 4));
    }

    @Override
    public TestWindowContext createWindowContext(WindowID<Integer> wid) {
      return new TestWindowContext(wid.getLabel());
    }
  }

  TestCase testStreamReduceWithWindowing() {
    return new AbstractTestCase<Integer, Pair<Integer, Long>>() {

      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e % 3)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .setPartitioner(e -> e % 2)
            .windowBy(new TestWindowing())
            .output();
      }

      @Override
      protected DataSource<Integer> getDataSource() {
        return ListDataSource.unbounded(
            Arrays.asList(1, 2, 3 /* first window, keys 1, 2, 0 */,
                          4, 5, 6, 7 /* second window, keys 1, 2, 0, 1 */,
                          8, 9, 10 /* third window, keys 2, 0, 1 */),
            Arrays.asList(5, 6, 7 /* second window, keys 2, 0, 1 */,
                          8, 9, 10, 11 /* third window, keys 2, 0, 1, 2 */,
                          12, 13, 14, 15 /* fourth window, keys 0, 1, 2, 0 */));
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, Long>>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, Long>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, 1L), Pair.of(2, 1L) /* first window */,
            Pair.of(0, 2L), Pair.of(2, 2L) /* second window */,
            Pair.of(0, 2L), Pair.of(2, 3L) /* third window */,
            Pair.of(0, 2L), Pair.of(2, 1L) /* fourth window */),
            first);
        List<Pair<Integer, Long>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(Pair.of(1, 1L) /* first window*/,
            Pair.of(1, 3L) /* second window */,
            Pair.of(1, 2L) /* third window */,
            Pair.of(1, 1L) /* fourth window */),
            second);
      }


    };
  }

  TestCase testReduceWithoutWindowing(boolean batch) {
    return new AbstractTestCase<String, Pair<String, Long>>() {
      @Override
      protected DataSource<String> getDataSource() {
        String[] words =
            "one two three four one two three four one two three one two one".split(" ");
        return batch
            ? ListDataSource.bounded(Arrays.asList(words))
            : ListDataSource.unbounded(Arrays.asList(words));
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Pair<String, Long>>> partitions) {
        assertEquals(1, partitions.size());
        HashMap<String, Long> actual = new HashMap<>();
        for (Pair<String, Long> p : partitions.get(0)) {
          actual.put(p.getFirst(), p.getSecond());
        }

        HashMap<String, Long> expected = new HashMap<>();
        expected.put("one", 5L);
        expected.put("two", 4L);
        expected.put("three", 3L);
        expected.put("four", 2L);
        assertEquals(expected, actual);
      }

      @Override
      protected Dataset<Pair<String, Long>> getOutput(Dataset<String> input) {
        return ReduceByKey.of(input)
            .keyBy(e -> e)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .output();
      }
    };
  }
}
