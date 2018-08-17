package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.LeftJoin;
import cz.seznam.euphoria.core.client.operator.RightJoin;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.JoinTest;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.ExecutorProviderRunner;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
import cz.seznam.euphoria.spark.testkit.SparkExecutorProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@RunWith(ExecutorProviderRunner.class)
public class BroadcastHashJoinTest extends AbstractOperatorTest implements SparkExecutorProvider {

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void leftBroadcastHashJoin() {
    execute(new JoinTest.JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return LeftJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Optional<Long> r, Collector<String> c) ->
                c.collect(l + "+" + r.orElse(null)))
            .withHints(Sets.newHashSet(JoinHints.broadcastHashJoin()))
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L, 11L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, "0+null"), Pair.of(2, "2+12"), Pair.of(2, "2+12"),
            Pair.of(4, "4+14"), Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"), Pair.of(1, "1+11"),
            Pair.of(1, "1+11"));
      }
    });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void rightBroadcastHashJoin() {
    execute(new JoinTest.JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return RightJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Optional<Integer> l, Long r, Collector<String> c) ->
                c.collect(l.orElse(null) + "+" + r))
            .withHints(Sets.newHashSet(JoinHints.broadcastHashJoin()))
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(1, "1+11"), Pair.of(1, "1+11"), Pair.of(3, "3+13"),
            Pair.of(3, "3+13"), Pair.of(5, "null+15"));
      }
    });
  }
}
