package cz.seznam.euphoria.core.client.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.io.TCPLineStreamSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.executor.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// TODO: Will be moved to euphoria-examples
/**
 * Test basic operator functionality and ability to compile.
 */
public class BasicOperatorTest {

  Executor executor = new InMemExecutor();

  @Test
  public void testWordCountStreamNonAggregating() throws Exception {
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
        .by((String s, Collector<Pair<String, Long>> c) -> {
          for (String part : s.split(" ")) {
            c.collect(Pair.of(part, 1L));
          }})
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Windowing.Time.seconds(1))
        .output();

    streamOutput.persist(URI.create("inmem:///tmp/output"));

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<Pair<String, Long>> f = new ArrayList<>(inmemfs.getFile("/tmp/output/0"));
//    System.out.println(f);

    // ~ assert the total amount of data produced
    assertEquals(7, f.size());

    // ~ first window
    assertEquals(asList("four-2", "one-1", "three-1", "two-3"), sublist(f, 0, 4));

    //  ~ second window
    assertEquals(asList("one-3", "three-1", "two-2"), sublist(f, 4, -1));
  }

  @Test
  public void testWordCountStreamAggregating() throws Exception {
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
        .by((String s, Collector<Pair<String, Long>> c) -> {
          for (String part : s.split(" ")) {
            c.collect(Pair.of(part, 1L));
          }})
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Windowing.Time.seconds(1).aggregating())
        .output();

    streamOutput.persist(URI.create("inmem:///tmp/output"));

    executor.waitForCompletion(flow);

    @SuppressWarnings("unchecked")
    List<Pair<String, Long>> f = new ArrayList<>(inmemfs.getFile("/tmp/output/0"));
    System.out.println(f);

    // ~ assert the total amount of data produced
    assertEquals(7, f.size());

    // ~ first window
    assertEquals(asList("four-2", "one-1", "three-1", "two-3"), sublist(f, 0, 4));

    //  ~ second window
    assertEquals(asList("one-4", "three-2", "two-5"), sublist(f, 4, -1));
  }

  private List<String> sublist(List<Pair<String, Long>> xs, int start, int len) {
    return sublist(xs, start, len, true);
  }

  private List<String> sublist(
      List<Pair<String, Long>> xs, int start, int len, boolean sort)
  {
    Stream<String> s = xs.subList(start, len < 0 ? xs.size() : start + len)
        .stream()
        .map(p -> p.getFirst() + "-" + p.getSecond());
    if (sort) {
      s = s.sorted();
    }
    return s.collect(Collectors.toList());
  }

  @Test
  public void testWordCountByCountNonAggregating() throws Exception {
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
//    System.out.println(output.getOutput(0));

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
  public void testWordCountByCountAggregating() throws Exception {
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
        .windowBy(Windowing.Count.of(6).aggregating())
        .output()
        .persist(output);

    executor.waitForCompletion(flow);

    assertNotNull(output.getOutput(0));
//    System.out.println(output.getOutput(0));

    assertEquals(8, output.getOutput(0).size());
    // ~ first 6 input elements processed
    assertEquals(
        asList("four-2", "one-1", "three-1", "two-2"),
        sublist(output.getOutput(0), 0, 4));
    // ~ seconds 6 input elems
    assertEquals(
        asList("one-4", "two-5"),
        sublist(output.getOutput(0), 4, 2));
    // ~ third 6 input elems
    assertEquals(
        asList("four-6", "three-3"),
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
        .by((String s, Collector<Pair<String, Long>> c) -> {
          for (String part : s.split(" ")) {
            c.collect(Pair.of(part, 1L));
          }})
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
    Dataset<Pair<String, String>> pairs = Map.of(nonempty)
        .by(s -> {
          String[] parts = s.split("\t", 2);
          return Pair.of(parts[0], parts[1]);
        })
        .output();

    // group all lines by the first element
    GroupedDataset<String, String> grouped = GroupByKey.of(pairs)
        .valueBy(Pair::getSecond)
        .keyBy(Pair::getFirst)
        .output();

    // calculate all distinct values within each group
    Dataset<Pair<CompositeKey<String, String>, Void>> reduced = ReduceByKey.of(grouped)
        .valueBy(e -> (Void) null)
        .combineBy(values -> null)
        .windowBy(Windowing.Time.seconds(1).aggregating())
        .output();

    // take distinct tuples
    Dataset<CompositeKey<String, String>> distinct = Map.of(reduced)
        .by(Pair::getFirst)
        .output();

    // calculate the final distinct values per key
    Dataset<Pair<String, Long>> output = CountByKey.of(distinct)
        .by(Pair::getFirst)
        .windowBy(Windowing.Time.seconds(1))
        .output();

    output.persist(URI.create("stdout:///?dump-partition-id=false"));

    executor.waitForCompletion(flow);

  }

  @Test
  @Ignore
  public void testJoinOnStreams() throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.tcp",
        TCPLineStreamSource.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);

    Dataset<String> first = flow.createInput(new URI("tcp://localhost:8080"));
    Dataset<String> second = flow.createInput(new URI("tcp://localhost:8081"));

    UnaryFunctor<String, Pair<String, Integer>> tokv = (s, c) -> {
      String[] parts = s.split("\t", 2);
      if (parts.length == 2) {
        c.collect(Pair.of(parts[0], Integer.valueOf(parts[1])));
      }
    };

    Dataset<Pair<String, Integer>> firstkv = FlatMap.of(first)
        .by(tokv)
        .output();
    Dataset<Pair<String, Integer>> secondkv = FlatMap.of(second)
        .by(tokv)
        .output();

    Dataset<Pair<String, Object>> output = Join.of(firstkv, secondkv)
        .by(Pair::getFirst, Pair::getFirst)
        .using((l, r, c) -> {
          c.collect((l == null ? 0 : l.getSecond()) + (r == null ? 0 : r.getSecond()));
        })
        .windowBy(Windowing.Time.seconds(10))
        .outer()
        .output();

    Map.of(output).by(p -> p.getFirst() + ", " + p.getSecond())
        .output().persist(URI.create("stdout:///?dump-partition-id=false"));

    executor.waitForCompletion(flow);

  }

}
