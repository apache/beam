package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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

  @Test
  public void testEventWindowingEarlyTriggered() throws Exception {
    ListDataSink<WindowedPair<TimeInterval, String, HashSet<String>>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",     1),
                Pair.of("two",     2),
                Pair.of("three",   3),
                Pair.of("four",    4),
                Pair.of("five",    5),
                Pair.of("six",     6),
                Pair.of("seven",   7),
                Pair.of("eight",   8),
                Pair.of("nine",    9),
                Pair.of("ten",    10),
                Pair.of("eleven", 11)))
            .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    ReduceByKey.of(f.createInput(source))
        .keyBy(e -> "")
        .valueBy((UnaryFunction<Pair<String, Integer>, HashSet<String>>) what ->
            Sets.newHashSet(what.getFirst()))
        .combineBy((CombinableReduceFunction<HashSet<String>>) what -> {
          Iterator<HashSet<String>> iter = what.iterator();
          HashSet<String> s = iter.next();
          while (iter.hasNext()) {
            s.addAll(iter.next());
          }
          return s;
        })
        .windowBy(Time.of(Duration.ofMillis(6))
            .earlyTriggering(Duration.ofMillis(2))
            // ~ event time
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .waitForCompletion(f);

    assertEquals(
        Arrays.asList("00: 02 => one, two",
                      "00: 04 => four, one, three, two",
                      "00: 05 => five, four, one, three, two",
                      "06: 03 => eight, seven, six",
                      "06: 05 => eight, nine, seven, six, ten",
                      "06: 06 => eight, eleven, nine, seven, six, ten"),
        output.getOutput(0)
            .stream()
            .map(p -> {
              StringBuilder sb = new StringBuilder();
              sb.append(String.format("%02d: %02d => ",
                  p.getWindowLabel().getStartMillis(),
                  p.getSecond().size()));
              ArrayList<String> xs = Lists.newArrayList(p.getSecond());
              xs.sort(String::compareTo);
              Joiner.on(", ").appendTo(sb, xs);
              return sb.toString();
            })
            .sorted()
            .collect(Collectors.toList()));
  }
}
