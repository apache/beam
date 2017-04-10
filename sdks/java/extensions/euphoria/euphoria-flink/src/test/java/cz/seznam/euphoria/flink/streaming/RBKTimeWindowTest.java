/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RBKTimeWindowTest {
  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("two",   8),
                Pair.of("three", 8)))
            .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    Dataset<Pair<String, Long>> reduced =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofMillis(5)),
            // ~ event time
            e -> (long) e.getSecond())
        .setNumPartitions(1)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoint"))
        .submit(f)
        .get();

    assertEquals(
        asList("0:one-2", "0:two-1", "5:three-1", "5:two-3"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getFirst().getStartMillis() + ":" + p.getSecond() + "-" + p.getThird())
            .sorted()
            .collect(Collectors.toList()));

  }

  @Test
  public void testEventWindowingEarlyTriggered() throws Exception {
    ListDataSink<Triple<TimeInterval, String, HashSet<String>>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            asList(
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
    Dataset<Pair<String, HashSet<String>>> reduced =
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
            .earlyTriggering(Duration.ofMillis(2)),
            // ~ event time
            e -> (long) e.getSecond())
        .setNumPartitions(1)
        .output();

    Dataset<Triple<TimeInterval, String, HashSet<String>>> extracted;
    extracted = Util.extractWindows(reduced, TimeInterval.class);
    extracted.persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .submit(f)
        .get();

    assertEquals(
        asList(
            "00: 02 => one, two",
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
                  p.getFirst().getStartMillis(),
                  p.getThird().size()));
              ArrayList<String> xs = Lists.newArrayList(p.getThird());
              xs.sort(String::compareTo);
              Joiner.on(", ").appendTo(sb, xs);
              return sb.toString();
            })
            .sorted()
            .collect(Collectors.toList()));
  }

  @Test
  public void testEventWindowingNonCombining() throws Exception {
    ListDataSink<Triple<TimeInterval, String, String>> output = ListDataSink.get(2);

    ListDataSource<Triple<String, String, Integer>> source =
        ListDataSource.unbounded(
            asList(
                Triple.of("one",   "a",   1),
                Triple.of("aaa",   "A",   1), // ~ this one goes to a different partition than the rest
                Triple.of("one",   "b",   2),
                Triple.of("aaa",   "B",   2), // ~ this one goes to a different partition than the rest
                Triple.of("aaa",   "C",   2), // ~ this one goes to a different partition than the rest
                Triple.of("two",   "X",   3),
                Triple.of("two",   "Q",   6),
                Triple.of("three", "F",   6),
                Triple.of("two",   "W",   7),
                Triple.of("two",   "E",   8),
                Triple.of("three", "G",   8),
                Triple.of("one",   "c",   8)))
            .withReadDelay(Duration.ofMillis(500))
            .withFinalDelay(Duration.ofMillis(1000));

    Flow f = Flow.create("test-attached-windowing");
    Dataset<Pair<String, String>> reduced =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Triple::getFirst)
        .valueBy(Triple::getSecond)
        .combineBy(xs -> {
          StringBuilder buf = new StringBuilder();
          xs.forEach(buf::append);
          return buf.toString();
        })
        .windowBy(Time.of(Duration.ofMillis(5)),
            // ~ event time
            e -> (long) e.getThird())
        .setNumPartitions(2)
        .setPartitioner(element -> element.equals("aaa") ? 1 : 0)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    assertEquals(
        asList("0: one=ab", "0: two=X", "5: one=c", "5: three=FG", "5: two=QWE"),
        fmt(output.getOutput(0)));
    assertEquals(
        asList("0: aaa=ABC"),
        fmt(output.getOutput(1)));
  }

  private List<String> fmt(List<Triple<TimeInterval, String, String>> xs) {
    return xs.stream()
        .map(p -> p.getFirst().getStartMillis() + ": " + p.getSecond() + "=" + p.getThird())
        .sorted()
        .collect(Collectors.toList());
  }

  @Test
  public void testEventWindowingWithAllowedLateness() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
            ListDataSource.unbounded(
                    asList(
                            Pair.of("one", 1),
                            Pair.of("one", 2),
                            Pair.of("two", 7),
                            Pair.of("two", 3), // latecomer, but in limit of allowed lateness
                            Pair.of("two", 1), // latecomer, will be dropped
                            Pair.of("two", 8),
                            Pair.of("three", 8)))
                    .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    Dataset<Pair<String, Long>> reduced =
            ReduceByKey.of(f.createInput(source))
                    .keyBy(Pair::getFirst)
                    .valueBy(e -> 1L)
                    .combineBy(Sums.ofLongs())
                    .windowBy(Time.of(Duration.ofMillis(5)),
                            // ~ event time
                            e -> (long) e.getSecond())
                    .setNumPartitions(1)
                    .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
            .setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoint"))
            .setAllowedLateness(Duration.ofMillis(4))
            .submit(f)
            .get();

    assertEquals(
            asList("0:one-2", "0:two-1", "5:three-1", "5:two-2"),
            output.getOutput(0)
                    .stream()
                    .map(p -> p.getFirst().getStartMillis() + ":" + p.getSecond() + "-" + p.getThird())
                    .sorted()
                    .collect(Collectors.toList()));

  }

}
