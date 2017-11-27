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
package cz.seznam.euphoria.executor.local;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

/**
 * Test basic operator functionality and ability to compile.
 */
public class BasicOperatorTest {

  private LocalExecutor executor = new LocalExecutor();

  private static UnaryFunctor<String, Pair<String, Long>> toWordCountPair() {
    return toWords(w -> Pair.of(w, 1L));
  }

  private static <O> UnaryFunctor<String, O> toWords(UnaryFunction<String, O> f) {
    return (String s, Collector<O> c) -> {
      for (String part : s.split(" ")) {
        c.collect(f.apply(part));
      }
    };
  }

  @Test
  public void testWordCountStream() throws Exception {
    ListDataSource<String> input =
        ListDataSource.unbounded(asList(
            "one two three four four two two",
            "one one one two two three"))
            .withReadDelay(Duration.ofSeconds(2));

    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(input);

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .using(toWordCountPair())
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    Dataset<Pair<String, Long>> streamOutput =
        ReduceByKey.of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    ListDataSink<Pair<String, Long>> out = ListDataSink.get();
    streamOutput.persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        Pair.of("four", 2L),
        Pair.of("one", 1L),
        Pair.of("three", 1L),
        Pair.of("two", 3L),
        Pair.of("one", 3L),
        Pair.of("three", 1L),
        Pair.of("two", 2L));
  }

  @Test
  public void testWordCountStreamWithWindowLabel() throws Exception {
    ListDataSource<String> input =
        ListDataSource.unbounded(asList(
            "one two three four four two two",
            "one one one two two three"))
        .withReadDelay(Duration.ofSeconds(2));

    Flow flow = Flow.create("Test");
    // FIXME: this is basically wrong and cannot function this way outside of local executor
    // the correct way of doing this is #192
    // fix this after implementation of that issue
    AtomicLong time = new AtomicLong(1000);
    Dataset<String> lines = flow.createInput(input, e -> time.addAndGet(2000L));

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .using(toWordCountPair())
        .output();

    Dataset<Pair<String, Long>> streamOutput =
        ReduceByKey.of(words)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();

    ListDataSink<Pair<String, Long>> out = ListDataSink.get();

    Dataset<Triple<TimeInterval, String, Long>> withWindow = FlatMap.of(streamOutput)
        .using((Pair<String, Long> p, Collector<Triple<TimeInterval, String, Long>> c) -> {
          // ~ just access the windows testifying their accessibility
          c.collect(Triple.of((TimeInterval) c.getWindow(), p.getFirst(), p.getSecond()));
        })
        .output();

    // strip the window again after
    MapElements.of(withWindow)
        .using(e -> Pair.of(e.getSecond(), e.getThird()))
        .output()
        .persist(out);

    executor
        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler<>(0))
        .submit(flow).get();

    List<Pair<String, Long>> fs = out.getOutputs();

    DatasetAssert.unorderedEquals(
        fs,
        Pair.of("four", 2L),
        Pair.of("one", 1L),
        Pair.of("three", 1L),
        Pair.of("two", 3L),
        Pair.of("one", 3L),
        Pair.of("three", 1L),
        Pair.of("two", 2L));
  }

  @Test
  public void testMapWithOutputGroupping() throws InterruptedException, ExecutionException {
    ListDataSource<String> input =
        ListDataSource.unbounded(asList(
            "one two three four four two two",
            "one one one two two three"));

    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(input);

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .using(toWordCountPair())
        .output();

    ListDataSink<Pair<String, Long>> sink = ListDataSink.get();
    // apply wordcount transform in output sink
    words.persist(
        sink.withPrepareDataset(d ->
          ReduceByKey.of(d)
              .keyBy(Pair::getFirst)
              .valueBy(Pair::getSecond)
              .combineBy(Sums.ofLongs())
              .output()
              .persist(sink)));

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        Pair.of("one", 4L),
        Pair.of("two", 5L),
        Pair.of("three", 2L),
        Pair.of("four", 2L));
  }

  @Test
  public void testWordCountStreamEarlyTriggered() throws Exception {
    ListDataSource<Pair<String, Integer>> input =
        ListDataSource.unbounded(asList(
            Pair.of("one two three four four two two", 1_000),
            Pair.of("one one one two two three", 4_000)));

    Flow flow = Flow.create("Test");
    Dataset<Pair<String, Integer>> lines = flow.createInput(input);

    // expand it to words
    Dataset<Triple<String, Long, Integer>> words = FlatMap.of(lines)
        .using((Pair<String, Integer> p, Collector<Triple<String, Long, Integer>> out) -> {
          for (String word : p.getFirst().split(" ")) {
            out.collect(Triple.of(word, 1L, p.getSecond()));
          }
        })
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    words = AssignEventTime.of(words).using(Triple::getThird).output();
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Triple::getFirst)
        .valueBy(Triple::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10))
            .earlyTriggering(Duration.ofMillis(1_003)))
        .output();


    ListDataSink<Pair<String, Long>> s = ListDataSink.get();
    streamOutput.persist(s);

    executor.setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0));
    executor.submit(flow).get();

    List<Pair<String, Long>> outputs = s.getOutputs();

    DatasetAssert.unorderedEquals(
        outputs,
        // first emission, early at watermark 1003
        Pair.of("one", 1L),
        Pair.of("two", 3L),
        Pair.of("three", 1L),
        Pair.of("four", 2L),

        // second emission, early at watermark 4012
        Pair.of("two", 5L),
        Pair.of("one", 4L),
        Pair.of("three", 2L),
        Pair.of("four", 2L),

        // final emission
        Pair.of("two", 5L),
        Pair.of("one", 4L),
        Pair.of("three", 2L),
        Pair.of("four", 2L));
  }

  @Test
  public void testWordCountStreamEarlyTriggeredInSession() throws Exception {
    ListDataSource<Pair<String, Integer>> input =
        ListDataSource.unbounded(asList(
            Pair.of("one", 150),    // early trigger at 1153
            Pair.of("two", 450),    // early trigger at 1453
            Pair.of("three", 999),  // early trigger at 2002
            Pair.of("four", 1111),  // early trigger at 2114
            Pair.of("four", 1500),  // 1st triggers `one` (next 2156) and `two` (2456)
            Pair.of("two", 1777),
            Pair.of("two", 1999),
            Pair.of("one", 4003),   // 2nd triggers `one` (4162), `two` (4462), `three` (4008)  and `four` (4120)
            Pair.of("one", 4158),   // 3rd triggers `three` (5011) and `four` (5123)
            Pair.of("one", 4899),   // 4th triggers `one` (5165) and `two` (5465)
            Pair.of("two", 5001),
            Pair.of("two", 5123),   // 5th triggers `three` (6014) and `four` (5126)
            Pair.of("three", 5921))); // 6th triggers `one` and `two`

    Flow flow = Flow.create("Test");
    Dataset<Pair<String, Integer>> lines = flow.createInput(input);

    // expand it to words
    Dataset<Triple<String, Long, Integer>> words = FlatMap.of(lines)
        .using((Pair<String, Integer> p, Collector<Triple<String, Long, Integer>> out) -> {
          for (String word : p.getFirst().split(" ")) {
            out.collect(Triple.of(word, 1L, p.getSecond()));
          }
        })
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    words = AssignEventTime.of(words).using(Triple::getThird).output();
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Triple::getFirst)
        .valueBy(Triple::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Session.of(Duration.ofSeconds(10))
            .earlyTriggering(Duration.ofMillis(1_003)))
        .output();


    ListDataSink<Pair<String, Long>> s = ListDataSink.get();
    streamOutput.persist(s);

    executor.setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0));
    executor.submit(flow).get();

    List<Pair<String, Long>> outputs = s.getOutputs();

    // ~ assert the total amount of data produced
    assertTrue(outputs.size() >= 8); // at least one early triggered result + end of window

    // ~ take only unique results (window can be triggered arbitrary times)
    Set<String> results = new HashSet<>(sublist(outputs, 0, -1, false));

    // ~ first window - early triggered
    results.containsAll(asList("four-2", "one-1", "three-1", "two-3"));

    //  ~ second (final) window
    results.containsAll(asList("one-4", "three-2", "two-5"));

  }

  private List<String> sublist(
      List<? extends Pair<String, Long>> xs, int start, int len, boolean sort)
  {
    Stream<String> s = xs.subList(start, len < 0 ? xs.size() : start + len)
        .stream()
        .map(p -> p.getFirst() + "-" + p.getSecond());
    if (sort) {
      s = s.sorted();
    }
    return s.collect(toList());
  }

  @Test
  public void testWordCountBatch() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(ListDataSource.bounded(
        asList("one two three four",
               "one two three",
               "one two",
               "one")));

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .using(toWordCountPair())
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .output();

    ListDataSink<Pair<String, Long>> out = ListDataSink.get();
    streamOutput.persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        Pair.of("one", 4L),
        Pair.of("two", 3L),
        Pair.of("three", 2L),
        Pair.of("four", 1L));
  }

  @Test
  public void testDistinctOnBatchWithoutWindowingLabels() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(ListDataSource.bounded(
        asList("one two three four", "one two three", "one two", "one")));

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    Dataset<String> output = Distinct.of(words).output();
    ListDataSink<String> out = ListDataSink.get();
    output.persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        "four", "one", "three", "two");
  }

  @Test
  public void testDistinctOnStreamWithoutWindowingLabels() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(asList(
            "one two three four one one two",
            "one two three three three"))
            .withReadDelay(Duration.ofSeconds(2)));

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    Dataset<String> output = Distinct.of(words)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    ListDataSink<String> out = ListDataSink.get();
    output.persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        "four", "one", "three", "two",
        "one", "three", "two");
  }

  @Test
  public void testDistinctOnStreamUsingWindowingLabels() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(asList(
            "one two three four one one two",
            "one two three three three"))
            .withReadDelay(Duration.ofSeconds(2)));

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    Dataset<Pair<TimeInterval, String>> output =
        FlatMap.of(Distinct.of(words).windowBy(Time.of(Duration.ofSeconds(1))).output())
            .using((UnaryFunctor<String, Pair<TimeInterval, String>>) (elem, context) ->
                context.collect(Pair.of((TimeInterval) context.getWindow(), elem)))
        .output();

    ListDataSink<String> out = ListDataSink.get();
    // strip the labels again because we cannot test them
    MapElements.of(output)
        .using(Pair::getSecond)
        .output()
        .persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        "four", "one", "three", "two",
        "one", "three", "two");
  }

  @Test
  public void testReduceWindow() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(
            asList("1-one 1-two 1-three 2-three 2-four 2-one 2-one 2-two".split(" ")),
            asList("1-one 1-two 1-four 2-three 2-three 2-three".split(" "))));

    ListDataSink<Set<String>> out = ListDataSink.get();

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    // window it, use the first character as time
    words = AssignEventTime.of(words)
        .using(s -> (int) s.charAt(0) * 3_600_000L)
        .output();
    ReduceWindow.of(words)
        .reduceBy(s -> s.collect(Collectors.toSet()))
        .windowBy(Time.of(Duration.ofMinutes(1)))
        .output()
        .persist(out);

    executor
        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0))
        .allowWindowBasedShuffling()
        .submit(flow)
        .get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        Sets.newHashSet("2-three", "2-one", "2-two", "2-four"),
        Sets.newHashSet("1-one", "1-two", "1-three", "1-four"));
  }
}
