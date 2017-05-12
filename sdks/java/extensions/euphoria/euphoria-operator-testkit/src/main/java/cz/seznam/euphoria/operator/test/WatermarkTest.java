package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

public class WatermarkTest extends AbstractOperatorTest {

  // ~ see https://github.com/seznam/euphoria/issues/119
  @Processing(Processing.Type.UNBOUNDED)
  @Test
  public void innerJoinOnFastAndSlowInputs() {
    execute(new JoinTest.JoinTestCase<
        Pair<String, Long>,
        Pair<String, Long>,
        Triple<TimeInterval, String, String>>() {

      // ~ a very fast source
      @Override
      protected Partitions<Pair<String, Long>> getLeftInput() {
        return Partitions.add(Pair.of("fi", 1L), Pair.of("fa", 2L)).build();
      }

      // ~ a very slow source
      @Override
      protected Partitions<Pair<String, Long>> getRightInput() {
        return Partitions.add(Pair.of("ha", 1L), Pair.of("ho", 4L))
            .build(Duration.ofMillis(2000), Duration.ofMillis(100));
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      protected Dataset<Triple<TimeInterval, String, String>>
      getOutput(Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {
        left = AssignEventTime.of(left).using(Pair::getSecond).output();
        right = AssignEventTime.of(right).using(Pair::getSecond).output();
        Dataset<Pair<String, Triple<TimeInterval, String, String>>> joined =
            Join.of(left, right)
                .by(p -> "", p -> "")
                .using((Pair<String, Long> l, Pair<String, Long> r, Context<Triple<TimeInterval, String, String>> c) ->
                    c.collect(Triple.of((TimeInterval) c.getWindow(), l.getFirst(), r.getFirst())))
                .windowBy(Time.of(Duration.ofMillis(10)))
                .setNumPartitions(1)
                .output();
        return MapElements.of(joined).using(Pair::getSecond).output();
      }

      @Override
      public void validate(Partitions<Triple<TimeInterval, String, String>> partitions) {
        TimeInterval expectedWindow = new TimeInterval(0, 10);
        assertUnorderedEquals(
            Arrays.asList(
                Triple.of(expectedWindow, "fi", "ha"),
                Triple.of(expectedWindow, "fi", "ho"),
                Triple.of(expectedWindow, "fa", "ha"),
                Triple.of(expectedWindow, "fa", "ho")),
            partitions.get(0));
      }
    });
  }
}
