
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.ImmutableMap;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Maps;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

/**
 * Test basic operator functionality and ability to compile.
 */
public class BasicOperatorTest {

  InMemExecutor executor = new InMemExecutor();

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

    ListDataSink<Pair<String, Long>> out = ListDataSink.get(1);
    streamOutput.persist(out);

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<Pair<String, Long>> f = new ArrayList<>(out.getOutput(0));

    // ~ assert the total amount of data produced
    assertEquals(7, f.size());

    // ~ first window
    assertEquals(asList("four-2", "one-1", "three-1", "two-3"), sublist(f, 0, 4));

    //  ~ second window
    assertEquals(asList("one-3", "three-1", "two-2"), sublist(f, 4, -1));
  }

  @Test
  public void testWordCountStreamWithWindowLabel() throws Exception {
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

    Dataset<WindowedPair<TimeInterval, String, Long>> streamOutput =
        ReduceByKey.of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .outputWindowed();

    ListDataSink<WindowedPair<TimeInterval, String, Long>> out =
        ListDataSink.get(1);

    MapElements.of(streamOutput)
        .using(p -> {
          // ~ just access the windowed pairs testifying their accessibility
          return WindowedPair.of(p.getWindowLabel(), p.getFirst(), p.getSecond());
        })
        .output()
        .persist(out);

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<WindowedPair<TimeInterval, String, Long>> fs =
        new ArrayList<>(out.getOutput(0));
//    System.out.println(fs);

    // ~ assert the total amount of data produced
    assertEquals(7, fs.size());

    long firstWindowStart = assertWindowedPairOutput(fs.subList(0, 4), 1000L,
        "four-2", "one-1", "three-1", "two-3");
    long secondWindowStart = assertWindowedPairOutput(fs.subList(4, fs.size()), 1000L,
        "one-3", "three-1", "two-2");
    assertTrue(firstWindowStart < secondWindowStart);
  }

  private <F, S> long assertWindowedPairOutput(
      List<WindowedPair<TimeInterval, F, S>> window,
      long expectedIntervalMillis, String ... expectedFirstAndSecond)
  {
    assertEquals(
        asList(expectedFirstAndSecond),
        window.stream()
            .map(p -> p.getFirst() + "-" + p.getSecond())
            .sorted()
            .collect(toList()));
    // ~ assert the windowing label (all elements of the window are expected to have
    // the same window label)
    long[] starts = window.stream().mapToLong(p -> {
      assertEquals(expectedIntervalMillis, p.getWindowLabel().getIntervalMillis());
      return p.getWindowLabel().getStartMillis();
    }).distinct().toArray();
    assertEquals(1, starts.length);
    return starts[0];
  }

  /** FIXME: this test fails nondeterministically! */
  @Test
  public void testWordCountStreamEarlyTriggered() throws Exception {
    ListDataSource<String> input =
        ListDataSource.unbounded(asList(
            "one two three four four two two",
            "one one one two two three"))
        .withReadDelay(Duration.ofSeconds(3));

    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(input);

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
        .windowBy(Time.of(Duration.ofSeconds(10))
            .earlyTriggering(Duration.ofSeconds(1)))
        .output();


    ListDataSink<Pair<String, Long>> s = ListDataSink.get(1);
    streamOutput.persist(s);

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<Pair<String, Long>> f = s.getOutput(0);

    // ~ assert the total amount of data produced
    assertTrue(f.size() >= 8); // at least one early triggered result + end of window

    // ~ take only unique results (window can be triggered arbitrary times)
    Set<String> results = new HashSet<>(sublist(f, 0, -1, false));

    // ~ first window - early triggered
    results.containsAll(asList("four-2", "one-1", "three-1", "two-3"));

    //  ~ second (final) window
    results.containsAll(asList("one-4", "three-2", "two-5"));
  }

  private List<String> sublist(
      List<? extends Pair<String, Long>> xs, int start, int len) {
    return sublist(xs, start, len, true);
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
  public void testWordCountByCount() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> words = flow.createInput(ListDataSource.unbounded(
        asList("one",   "two",  "three", "four", "four", "two",
               "two",   "one",  "one",   "one",  "two",  "two",
               "three", "three", "four", "four",  "four", "four")));

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    final ListDataSink<Pair<String, Long>> output = ListDataSink.get(1);
    ReduceByKey
        .of(words)
        .keyBy(w -> w)
        .valueBy(w -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Count.of(6))
        .output()
        .persist(output);

    executor.waitForCompletion(flow);

    assertNotNull(output.getOutput(0));

    assertEquals(8, output.getOutput(0).size());
    // ~ first 6 input elements processed
    assertEquals(
        asList("four-2", "one-1", "three-1", "two-2"),
        sublist(output.getOutput(0), 0, 4));
    // ~ seconds 6 input elems
    assertEquals(
        asList("one-3", "two-3"),
        sublist(output.getOutput(0), 4, 2));
    // ~ third 6 input elems
    assertEquals(
        asList("four-4", "three-2"),
        sublist(output.getOutput(0), 6, -1));
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

    ListDataSink<Pair<String, Long>> f = ListDataSink.get(1);
    streamOutput.persist(f);

    executor.waitForCompletion(flow);

    assertNotNull(f.getOutput(0));
    ImmutableMap<String, Pair<String, Long>> idx =
        Maps.uniqueIndex(f.getOutput(0), Pair::getFirst);
    assertEquals(4, idx.size());
    assertEquals((long) idx.get("one").getValue(), 4L);
    assertEquals((long) idx.get("two").getValue(), 3L);
    assertEquals((long) idx.get("three").getValue(), 2L);
    assertEquals((long) idx.get("four").getValue(), 1L);
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
    ListDataSink<String> f = ListDataSink.get(1);
    output.persist(f);

    executor.waitForCompletion(flow);

    assertNotNull(f.getOutput(0));
    assertEquals(
        asList("four", "one", "three", "two"),
        f.getOutput(0).stream().sorted().collect(toList()));
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

    ListDataSink<String> f = ListDataSink.get(1);
    output.persist(f);

    executor.waitForCompletion(flow);

    List<String> out = f.getOutput(0);
    assertNotNull(out);
    assertEquals(
        asList("four", "one", "three", "two"),
        out.subList(0, 4).stream().sorted().collect(toList()));
    assertEquals(
        asList("one", "three", "two"),
        out.subList(4, out.size()).stream().sorted().collect(toList()));
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
        Distinct.of(words)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .outputWindowed();

    ListDataSink<Pair<TimeInterval, String>> f = ListDataSink.get(1);
    output.persist(f);

    executor.waitForCompletion(flow);

    List<Pair<TimeInterval, String>> out = f.getOutput(0);
    assertNotNull(out);

    long firstWindowStart =
        assertWindowedOutput(out.subList(0, 4), 1000L, "four", "one", "three", "two");
    long secondWindowStart =
        assertWindowedOutput(out.subList(4, out.size()), 1000L, "one", "three", "two");
    assertTrue(firstWindowStart < secondWindowStart);
  }

  @Test
  public void testReduceWindow() throws Exception {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(
            asList("1-one 1-two 1-three 2-three 2-four 2-one 2-one 2-two".split(" ")),
            asList("1-one 1-two 1-four 2-three 2-three 2-three".split(" "))));

    ListDataSink<HashSet<String>> f = ListDataSink.get(4);

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    // window it, use the first character as time
    ReduceWindow.of(words)
        .reduceBy(Sets::newHashSet)
        .windowBy(Time.of(Duration.ofMinutes(1))
            .using(s -> (int) s.charAt(0) * 3_600_000L))
        .setNumPartitions(4)
        .output()
        .persist(f);

    executor
        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0))
        .allowWindowBasedShuffling()
        .waitForCompletion(flow);

    List<HashSet<String>> output = f.getOutput(1);
    assertEquals(1, output.size());
    assertEquals(Sets.newHashSet(
        Arrays.asList("1-one", "1-two", "1-three", "1-four")), output.get(0));
    output = f.getOutput(3);
    assertEquals(1, output.size());
    assertEquals(Sets.newHashSet(
        Arrays.asList("2-three", "2-one", "2-two", "2-four")), output.get(0));
  }

  private <S> long assertWindowedOutput(
      List<Pair<TimeInterval, S>> window,
      long expectedIntervalMillis, String ... expectedFirstAndSecond)
  {
    assertEquals(
        asList(expectedFirstAndSecond),
        window.stream().map(Pair::getSecond).sorted().collect(toList()));
    // ~ assert the windowing label (all elements of the window are expected to have
    // the same window label)
    long[] starts = window.stream().mapToLong(p -> {
      assertEquals(expectedIntervalMillis, p.getFirst().getIntervalMillis());
      return p.getFirst().getStartMillis();
    }).distinct().toArray();
    assertEquals(1, starts.length);
    return starts[0];
  }
}
