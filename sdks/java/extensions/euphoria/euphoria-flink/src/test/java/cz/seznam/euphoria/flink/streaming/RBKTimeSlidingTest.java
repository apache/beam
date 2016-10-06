package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RBKTimeSlidingTest {
  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get(2);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",   1),
                Pair.of("aaa",   1), // ~ this one goes to a different partition than the rest
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("two",   8),
                Pair.of("three", 8),
                Pair.of("one",   8)))
            .withReadDelay(Duration.ofMillis(500))
            .withFinalDelay(Duration.ofMillis(1000));

    Flow f = Flow.create("test-windowing");
    Dataset<Pair<String, Long>> reduced =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5))
            // ~ event time
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(2)
        .setPartitioner(element -> element.equals("aaa") ? 1 : 0)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

//    new InMemExecutor()
//        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0))
//        .waitForCompletion(f);
    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .waitForCompletion(f);

    assertEquals(
        "[-5,5): one=2, [-5,5): two=1," +
        " [0,10): one=3, [0,10): three=1, [0,10): two=4," +
        " [5,15): one=1, [5,15): three=1, [5,15): two=3",
        flatten(output.getOutput(0)));
    assertEquals(
        "[-5,5): aaa=1," +
        " [0,10): aaa=1",
        flatten(output.getOutput(1)));
  }

  @Test
  public void testEventWindowing_NonCombining() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get(2);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",   1),
                Pair.of("aaa",   1), // ~ this one goes to a different partition than the rest
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("two",   8),
                Pair.of("three", 8),
                Pair.of("one",   8)))
            .withReadDelay(Duration.ofMillis(500))
            .withFinalDelay(Duration.ofMillis(1000));

    Flow f = Flow.create("test-attached-windowing");
    Dataset<Pair<String, Long>> reduced =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .reduceBy(xs -> {
          long sum = 0;
          for (long x : xs) {
            sum += x;
          }
          return sum;
        })
        .windowBy(TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5))
            // ~ event time
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(2)
        .setPartitioner(element -> element.equals("aaa") ? 1 : 0)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .waitForCompletion(f);

    assertEquals(
        "[-5,5): one=2, [-5,5): two=1," +
            " [0,10): one=3, [0,10): three=1, [0,10): two=4," +
            " [5,15): one=1, [5,15): three=1, [5,15): two=3",
        flatten(output.getOutput(0)));
    assertEquals(
        "[-5,5): aaa=1," +
            " [0,10): aaa=1",
        flatten(output.getOutput(1)));
  }

  private <K extends Comparable<K>, V>
  String flatten(List<Triple<TimeInterval, K, V>> xs) {
    return Joiner.on(", ").join(
        xs.stream().sorted((a, b) -> {
          int cmp = Long.compare(
              a.getFirst().getStartMillis(),
              b.getFirst().getStartMillis());
          if (cmp == 0) {
            cmp = a.getSecond().compareTo(b.getSecond());
          }
          return cmp;
        })
        .map(p -> "[" + p.getFirst().getStartMillis()
                      + "," + p.getFirst().getEndMillis()
                      + "): " + p.getSecond() + "=" + p.getThird())
        .collect(Collectors.toList()));
  }
}
