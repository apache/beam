package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TimeSlidingTest {
  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<WindowedPair<Long, String, Long>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one", 1),
                Pair.of("one", 2),
                Pair.of("two", 3),
                Pair.of("two", 6),
                Pair.of("two", 7),
                Pair.of("two", 8),
                Pair.of("three", 8),
                Pair.of("one", 8)))
            .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5))
            // ~ event time
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(output);

    new TestFlinkExecutor().waitForCompletion(f);

    assertEquals(
        Joiner.on(", ").join(
            output.getOutput(0)
                .stream()
                .sorted((a, b) -> {
                  int cmp = a.getWindowLabel().compareTo(b.getWindowLabel());
                  if (cmp == 0) {
                    cmp = a.getFirst().compareTo(b.getFirst());
                  }
                  return cmp;
                })
                .map(p -> p.getWindowLabel() + ": " + p.getFirst() + "=" + p
                    .getSecond())
                .collect(Collectors.toList())),
        "-5: one=2, -5: two=1," +
        " 0: one=3, 0: three=1, 0: two=4," +
        " 5: one=1, 5: three=1, 5: two=3");
  }
}
