package cz.seznam.euphoria.core.client.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.dataset.Windowing.Time.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.io.TCPLineStreamSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
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

  Executor executor = new InMemExecutor();

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
    final InMemFileSystem inmemfs = InMemFileSystem.get();

    inmemfs.reset()
        .setFile("/tmp/foo.txt", Duration.ofSeconds(2), asList(
            "one two three four four two two",
            "one one one two two three"));

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.SourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.inmem",
        InMemFileSystem.SinkFactory.class);

    Flow flow = Flow.create("Test", settings);
    Dataset<String> lines = flow.createInput(URI.create("inmem:///tmp/foo.txt"));

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
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
        .output();

    streamOutput.persist(URI.create("inmem:///tmp/output"));

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<Pair<String, Long>> f = new ArrayList<>(inmemfs.getFile("/tmp/output/0"));

    // ~ assert the total amount of data produced
    assertEquals(7, f.size());

    // ~ first window
    assertEquals(asList("four-2", "one-1", "three-1", "two-3"), sublist(f, 0, 4));

    //  ~ second window
    assertEquals(asList("one-3", "three-1", "two-2"), sublist(f, 4, -1));
  }

  @Test
  public void testWordCountStreamWithWindowLabel() throws Exception {
    final InMemFileSystem inmemfs = InMemFileSystem.get();

    inmemfs.reset()
        .setFile("/tmp/foo.txt", Duration.ofSeconds(2), asList(
            "one two three four four two two",
            "one one one two two three"));

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.SourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.inmem",
        InMemFileSystem.SinkFactory.class);

    Flow flow = Flow.create("Test", settings);
    Dataset<String> lines = flow.createInput(URI.create("inmem:///tmp/foo.txt"));

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .using(toWordCountPair())
        .output();

    Dataset<WindowedPair<TimeInterval, String, Long>> streamOutput =
        ReduceByKey.of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
        .outputWindowed();

    MapElements.of(streamOutput)
        .using(p -> {
          // ~ just access the windowed pairs testifying their accessibility
          return WindowedPair.of(p.getWindowLabel(), p.getFirst(), p.getSecond());
        })
        .output()
        .persist(URI.create("inmem:///tmp/output"));

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<WindowedPair<TimeInterval, String, Long>> fs =
        new ArrayList<>(inmemfs.getFile("/tmp/output/0"));
    System.out.println(fs);

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

  @Test
  public void testWordCountStreamEarlyTriggered() throws Exception {
    final InMemFileSystem inmemfs = InMemFileSystem.get();

    inmemfs.reset()
        .setFile("/tmp/foo.txt", Duration.ofSeconds(3), asList(
            "one two three four four two two",
            "one one one two two three"));

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.SourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.inmem",
        InMemFileSystem.SinkFactory.class);

    Flow flow = Flow.create("Test", settings);
    Dataset<String> lines = flow.createInput(URI.create("inmem:///tmp/foo.txt"));

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
        .windowBy(Windowing.Time.of(Duration.ofSeconds(10))
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

  private List<String> sublist(List<? extends Pair<String, Long>> xs, int start, int len) {
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
        .windowBy(Windowing.Count.of(6))
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
  public void testDistinctOnBatchWithoutWindowingLabels() {
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
  public void testDistinctOnStreamWithoutWindowingLabels() {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(asList(
            "one two three four one one two",
            "one two three three three"))
            .setSleepTime(2000));

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    Dataset<String> output = Distinct.of(words)
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
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
  public void testDistinctOnStreamUsingWindowingLabels() {
    Flow flow = Flow.create("Test");
    Dataset<String> lines = flow.createInput(
        ListDataSource.unbounded(asList(
            "one two three four one one two",
            "one two three three three"))
            .setSleepTime(2000));

    // expand it to words
    Dataset<String> words = FlatMap.of(lines)
        .using(toWords(w -> w))
        .output();

    Dataset<Pair<TimeInterval, String>> output =
        Distinct.of(words)
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
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

  @Test
  @Ignore
  public void testDistinctGroupByStream() throws Exception {

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.tcp",
        TCPLineStreamSource.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);

    Dataset<String> lines = flow.createInput(new URI("tcp://localhost:8080"));

    // get rid of empty lines
    Dataset<String> nonempty = Filter.of(lines)
        .by(line -> !line.isEmpty() && line.contains("\t"))
        .output();

    // split lines to pairs
    Dataset<Pair<String, String>> pairs = MapElements.of(nonempty)
        .using(s -> {
          String[] parts = s.split("\t", 2);
          return Pair.of(parts[0], parts[1]);
        })
        .output();

    // group all lines by the first element
    GroupedDataset<String, String> grouped = GroupByKey.of(pairs)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
        .output();

    // calculate all distinct values within each group
    Dataset<Pair<CompositeKey<String, String>, Void>>
        reduced = ReduceByKey.of(grouped)
        .keyBy(e -> e)
        .valueBy(e -> (Void) null)
        .combineBy(values -> null)
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
        .output();

    // take distinct tuples
    Dataset<CompositeKey<String, String>> distinct =
        MapElements.of(reduced)
        .using(Pair::getFirst)
        .output();

    // calculate the final distinct values per key
    Dataset<Pair<String, Long>> output =
        CountByKey.of(distinct)
        .keyBy(Pair::getFirst)
        .windowBy(Windowing.Time.of(Duration.ofSeconds(1)))
        .output();

    output.persist(URI.create("stdout:///?dump-partition-id=false"));

    executor.waitForCompletion(flow);

  }

}
