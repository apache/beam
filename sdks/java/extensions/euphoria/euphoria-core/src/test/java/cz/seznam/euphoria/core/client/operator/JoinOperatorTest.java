package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static cz.seznam.euphoria.core.util.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JoinOperatorTest {

  private Executor executor;
  private Settings settings;
  private InMemFileSystem inmemfs;

  @Before
  public void setUp() {
    inmemfs = InMemFileSystem.get().reset();

    settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.SourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.inmem",
        InMemFileSystem.SinkFactory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    executor = new InMemExecutor();
  }

  @SuppressWarnings("unchecked")
  private void testJoin(boolean outer,
                        Windowing windowing,
                        Duration readDelay,
                        List<String> leftInput,
                        List<String> rightInput,
                        List<String> expectedOutput)
      throws Exception
  {
    inmemfs
        .setFile("/tmp/foo.txt", readDelay, leftInput)
        .setFile("/tmp/bar.txt", readDelay, rightInput);

    Flow flow = Flow.create("Test", settings);

    Dataset<String> first = flow.createInput(new URI("inmem:///tmp/foo.txt"));
    Dataset<String> second = flow.createInput(new URI("inmem:///tmp/bar.txt"));

    UnaryFunctor<String, Pair<String, Integer>> tokv = (s, c) -> {
      String[] parts = s.split("[\t ]+", 2);
      if (parts.length == 2) {
        c.collect(Pair.of(parts[0], Integer.valueOf(parts[1])));
      }
    };

    Dataset<Pair<String, Integer>> firstkv = FlatMap.of(first)
        .using(tokv)
        .output();
    Dataset<Pair<String, Integer>> secondkv = FlatMap.of(second)
        .using(tokv)
        .output();

    Join.WindowingBuilder joinBuilder = Join.of(firstkv, secondkv)
        .by(Pair::getFirst, Pair::getFirst)
        .using((l, r, c) ->
            c.collect((l == null ? 0 : l.getSecond()) + (r == null ? 0 : r.getSecond())));
    if (outer) {
      joinBuilder = joinBuilder.outer();
    }
    Dataset<Pair<String, Object>> output = joinBuilder
        .windowBy(windowing)
        .output();

    MapElements.of(output).using(p -> p.getFirst() + ", " + p.getSecond())
        .output().persist(URI.create("inmem:///tmp/output"));

    executor.waitForCompletion(flow);

    List<String> f = new ArrayList<>(inmemfs.getFile("/tmp/output/0"));
    assertEquals(sorted(expectedOutput), sorted(f));
  }

  @Test
  public void testInnerJoinOnStreamsAggregating() throws Exception {
    testJoin(false,
        Windowing.Time.seconds(1).aggregating(),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "one, 34", "one, 67",
               "one, 32", "one, 55", "one, 88", "one, 54",
               "one, 77", "one, 110", "two, 21"));
  }

  // ~ the behaviour of this setup is actually supposed to produce the same output
  // as an outer join over and aggregating windowing
  @Test
  public void testInnerJoinOnBatch() throws Exception {
    testJoin(false,
        BatchWindowing.get(),
        null,
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21"));
  }

  @Test
  public void testInnerJoinOnStreamsNonAggregating() throws Exception {
    testJoin(false,
        Windowing.Time.seconds(1),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "two, 21", "one, 55"));
  }

  @Test
  public void testOuterJoinOnStreamAggregating() throws Exception {
    testJoin(true,
        Windowing.Time.seconds(1).aggregating(),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList(
            "one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21", "three, 55"));
  }

  @Test
  public void testOuterJoinOnStreamAggregating2() throws Exception {
    testJoin(true,
        Windowing.Time.seconds(1).aggregating(),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("quux 99", "one 10", "two 20", "one 33", "three 55", "one 66"),
        asList(
            "one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21", "three, 55", "quux, 99"));
  }

  // ~ the behaviour of this setup is actually supposed to produce the same output
  // as an outer join over and aggregating windowing
  @Test
  public void testOuterJoinOnBatch() throws Exception {
    testJoin(true,
        BatchWindowing.get(),
        null,
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList(
            "one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21", "three, 55"));
  }

  @Test
  public void testOuterJoinOnStreamNonAggregating() throws Exception {
    testJoin(true,
        Windowing.Time.seconds(1),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "two, 21", "one, 55", "one, 44", "three, 55", "one, 66"));
  }

}
