/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;

import static java.util.Arrays.asList;

public class RBKTimeWindowTest {
  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get();

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
    Dataset<Pair<String, Integer>> input =
        AssignEventTime.of(f.createInput(source))
            .using(Pair::getSecond)
            .output();
    Dataset<Pair<String, Long>> reduced =
        ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofMillis(5)))
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoint"))
        .submit(f)
        .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 5), "one", 2L),
        Triple.of(new TimeInterval(0, 5), "two", 1L),
        Triple.of(new TimeInterval(5, 10), "three", 1L),
        Triple.of(new TimeInterval(5, 10), "two", 3L));
  }

  @Test
  public void testEventWindowingEarlyTriggered() throws Exception {
    ListDataSink<Triple<TimeInterval, String, HashSet<String>>> output = ListDataSink.get();

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
        ReduceByKey.of(f.createInput(source, Pair::getSecond))
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
            .earlyTriggering(Duration.ofMillis(2)))
        .output();

    Dataset<Triple<TimeInterval, String, HashSet<String>>> extracted;
    extracted = Util.extractWindows(reduced, TimeInterval.class);
    extracted.persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .submit(f)
        .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 6), "", Sets.newHashSet("one", "two")),
        Triple.of(new TimeInterval(0, 6), "", Sets.newHashSet("four", "one", "three", "two")),
        Triple.of(new TimeInterval(0, 6), "", Sets.newHashSet("five", "four", "one", "three", "two")),
        Triple.of(new TimeInterval(6, 12), "", Sets.newHashSet("eight", "seven", "six")),
        Triple.of(new TimeInterval(6, 12), "", Sets.newHashSet("eight", "nine", "seven", "six", "ten")),
        Triple.of(new TimeInterval(6, 12), "", Sets.newHashSet("eight", "eleven", "nine", "seven", "six", "ten")));
  }

  @Test
  public void testEventWindowingNonCombining() throws Exception {
    ListDataSink<Triple<TimeInterval, String, String>> output = ListDataSink.get();

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
        ReduceByKey.of(f.createInput(source, Triple::getThird))
        .keyBy(Triple::getFirst)
        .valueBy(Triple::getSecond)
        .combineBy(xs -> {
          StringBuilder buf = new StringBuilder();
          xs.forEach(buf::append);
          return buf.toString();
        })
        .windowBy(Time.of(Duration.ofMillis(5)))
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 5), "one", "ab"),
        Triple.of(new TimeInterval(0, 5), "two", "X"),
        Triple.of(new TimeInterval(5, 10), "one", "c"),
        Triple.of(new TimeInterval(5, 10), "three", "FG"),
        Triple.of(new TimeInterval(5, 10), "two", "QWE"),
        Triple.of(new TimeInterval(0, 5), "aaa", "ABC"));
  }

  @Test
  public void testEventWindowingWithAllowedLateness() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Long>> output = ListDataSink.get();

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
            ReduceByKey.of(f.createInput(source, Pair::getSecond))
                    .keyBy(Pair::getFirst)
                    .valueBy(e -> 1L)
                    .combineBy(Sums.ofLongs())
                    .windowBy(Time.of(Duration.ofMillis(5)))
                    .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
            .setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoint"))
            .setAllowedLateness(Duration.ofMillis(4))
            .submit(f)
            .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 5), "one", 2L),
        Triple.of(new TimeInterval(0, 5), "two", 1L),
        Triple.of(new TimeInterval(5, 10), "three", 1L),
        Triple.of(new TimeInterval(5, 10), "two", 2L));

  }

}
