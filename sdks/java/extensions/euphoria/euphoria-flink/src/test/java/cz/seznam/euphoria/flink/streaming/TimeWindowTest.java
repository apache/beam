package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TimeWindowTest {
  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<WindowedPair<TimeInterval, String, Long>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("two",   8),
                Pair.of("three", 8)))
            .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofMillis(5))
            // ~ event time
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(output);

    new TestFlinkExecutor()
        .setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoint"))
        .waitForCompletion(f);

    assertEquals(
        Arrays.asList("0:one-2", "0:two-1", "5:three-1", "5:two-3"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getWindowLabel().getStartMillis() + ":" + p.getFirst() + "-" + p.getSecond())
            .sorted()
            .collect(Collectors.toList()));

  }
}
