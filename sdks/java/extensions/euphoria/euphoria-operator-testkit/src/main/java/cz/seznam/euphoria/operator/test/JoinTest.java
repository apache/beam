package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code Join}.
 */
public class JoinTest extends OperatorTest {

  static abstract class JoinTestCase<LEFT, RIGHT, OUT> implements TestCase<OUT> {
    
    @Override
    public Dataset<OUT> getOutput(Flow flow) {
      Dataset<LEFT> left = getLeftInput(flow);
      Dataset<RIGHT> right = getRightInput(flow);
      return getOutput(left, right);
    }

    protected abstract Dataset<OUT> getOutput(
        Dataset<LEFT> left, Dataset<RIGHT> right);

    protected abstract Dataset<LEFT> getLeftInput(Flow flow);
    protected abstract Dataset<RIGHT> getRightInput(Flow flow);
  }

  @Override
  protected List<TestCase> getTestCases() {
    return Arrays.asList(
        batchJoinOuter(),
        batchJoinInner(),
        windowJoin()
    );
  }

  TestCase batchJoinOuter() {
    return new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> {
              c.collect(l + "+" + r);
            })
            .setPartitioner(e -> e % 2)
            .outer()
            .output();
      }

      @Override
      protected Dataset<Integer> getLeftInput(Flow flow) {
        return flow.createInput(ListDataSource.bounded(
            Arrays.asList(1, 2, 3, 0),
            Arrays.asList(4, 3, 2, 1)));
      }

      @Override
      protected Dataset<Long> getRightInput(Flow flow) {
        return flow.createInput(ListDataSource.bounded(
            Arrays.asList(11L, 12L),
            Arrays.asList(13L, 14L, 15L)
        ));
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, String>>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"), Pair.of(5, "null+15")),
            second);
      }


    };
  }

  TestCase batchJoinInner() {
    return new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> {
              c.collect(l + "+" + r);
            })
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      protected Dataset<Integer> getLeftInput(Flow flow) {
        return flow.createInput(ListDataSource.bounded(
            Arrays.asList(1, 2, 3, 0),
            Arrays.asList(4, 3, 2, 1)));
      }

      @Override
      protected Dataset<Long> getRightInput(Flow flow) {
        return flow.createInput(ListDataSource.bounded(
            Arrays.asList(11L, 12L),
            Arrays.asList(13L, 14L, 15L)
        ));
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, String>>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13")),
            second);
      }


    };
  }

  /**
   * Stable windowing for test purposes.
   */
  static class EvenOddWindowing
      implements Windowing<Either<Integer, Long>, IntWindow> {

    @Override
    public Set<IntWindow> assignWindowsToElement(
        WindowedElement<?, Either<Integer, Long>> input) {
      int element;
      Either<Integer, Long> unwrapped = input.get();
      if (unwrapped.isLeft()) {
        element = unwrapped.left();
      } else {
        element = (int) (long) unwrapped.right();
      }
      final int label = element % 2 == 0 ? 0 : element;
      return Collections.singleton(new IntWindow(label));
    }

    @Override
    public Trigger<IntWindow> getTrigger() {
      return NoopTrigger.get();
    }
  }

  TestCase windowJoin() {
    return new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> {
              c.collect(l + "+" + r);
            })
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .outer()
            .windowBy(new EvenOddWindowing())
            .output();
      }

      @Override
      protected Dataset<Integer> getLeftInput(Flow flow) {
        return flow.createInput(ListDataSource.unbounded(
            Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6)));
      }

      @Override
      protected Dataset<Long> getRightInput(Flow flow) {
        return flow.createInput(ListDataSource.unbounded(
            Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
        ));
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(List<List<Pair<Integer, String>>> partitions) {
        assertEquals(2, partitions.size());
        // all even elements for one window
        // all odd elements are each element in separate window
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(6, "6+16"), Pair.of(8, "null+18")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+null"), Pair.of(1, "1+null"), Pair.of(1, "null+11"),
            Pair.of(3, "3+null"), Pair.of(3, "3+null"), Pair.of(3, "null+13"),
            Pair.of(5, "5+null"), Pair.of(5, "null+15"), Pair.of(7, "null+17")),
            second);
      }

    };
  }


}
