package cz.seznam.euphoria.core.client.dataset;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.executor.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cz.seznam.euphoria.core.util.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class WindowingTest {

  private static final Logger LOG = LoggerFactory.getLogger(WindowingTest.class);

  private InMemExecutor executor;
  private Settings settings;
  private InMemFileSystem inmemfs;

  @Before
  public void setUp() {
    executor = new InMemExecutor();
    inmemfs = InMemFileSystem.get().reset();

    settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.SourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.inmem",
        InMemFileSystem.SinkFactory.class);
  }

  static final class Item {
    final String word;
    final long evtTs;

    Item(String word, long evtTs) {
      this.word = word;
      this.evtTs = evtTs;
    }
  }

  @Test
  public void testWindowingByEventTime() throws Exception {
    // we have to make sure the timestamp is in the future
    inmemfs.setFile("/tmp/foo.txt", asList(
        new Item("one", 3001),
        new Item("one", 3500),
        new Item("one", 1000),
        new Item("one", 2100),
        new Item("one", 2050),
        new Item("one", 2099),
        new Item("two", 1100),
        new Item("two", 1500),
        new Item("two", 1900),
        new Item("two", 2199),
        new Item("two", 2355),
        new Item("four", 1500),
        new Item("four", 1200),
        new Item("three", 1000),
        new Item("three", 2831),
        new Item("three", 2123)));

    Flow flow = Flow.create("Test", settings);
    Dataset<Item> lines = flow.createInput(new URI("inmem:///tmp/foo.txt"));

    Dataset<Pair<Item, Long>> words = MapElements.of(lines)
        .using((UnaryFunction<Item, Pair<Item, Long>>) item -> Pair.of(item, 1L))
        .output();

    Dataset<Pair<String, Long>> reduced = ReduceByKey
        .of(words)
        .keyBy(e -> e.getFirst().word)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        // ~ windowing by one second using a user supplied event-time-fn
        .windowBy(Windowing.Time.seconds(1)
            .using((Pair<Item, Long> x) -> x.getFirst().evtTs))
        .output();

    Dataset<String> mapped = MapElements.of(reduced)
        .using(p -> p.getFirst() + "-" + p.getSecond())
        .output();

    ListDataSink<String> output = ListDataSink.get(1);
    mapped.persist(output);

    executor.waitForCompletion(flow);

    assertEquals(1, output.getOutputs().size());

    assertEquals(
        sorted(asList(
        /* 1st second window */ "one-1", "two-3", "three-1", "four-2",
        /* 2nd second window */ "one-3", "two-2", "three-2",
        /* 3rd second window */ "one-2")),
        sorted(output.getOutput(0)));
  }

  @Test
  public void testTimeBuilders() {
    assertTimeWindowing(
        Windowing.Time.seconds(100),
        false,
        100 * 1000,
        Windowing.Time.ProcessingTime.<String>get());

    assertTimeWindowing(
        Windowing.Time.seconds(20).aggregating(),
        true,
        20 * 1000,
        Windowing.Time.ProcessingTime.<Pair<Long, String>>get());

    UnaryFunction<Pair<Long, Long>, Long> evtf = event-> 0L;

    assertTimeWindowing(
        Windowing.Time.seconds(4).aggregating().using(evtf),
        true,
        4 * 1000,
        evtf);

    assertTimeWindowing(
        Windowing.Time.seconds(3).using(evtf).aggregating(),
        true,
        3 * 1000,
        evtf);

    assertTimeWindowing(
        Windowing.Time.seconds(8).using(evtf),
        false,
        8 * 1000,
        evtf);
  }

  private <T> void assertTimeWindowing(Windowing.Time<T> w,
                                       boolean expectAggregating,
                                       long expectDurationMillis,
                                       UnaryFunction<T, Long> expectedFn)
  {
    assertNotNull(w);
    assertEquals(expectAggregating, w.aggregating);
    assertEquals(expectDurationMillis, w.durationMillis);
    assertSame(expectedFn, w.eventTimeFn);
  }

  @Test
  public void testAttachedWindowing0() {
    final long READ_DELAY_MS = 100L;
    Flow flow = Flow.create("Test", settings);

    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList(("t1-r-one t1-r-two t1-r-three t1-s-one t1-s-two t1-s-three t1-t-one")
            .split(" "))
    ).setSleepTime(READ_DELAY_MS));

    // ~ emits after 3 input elements received
    Dataset<Pair<String, Set<String>>> first =
        ReduceByKey.of(input)
        .keyBy(e -> e.substring(0, 2))
        .valueBy(e -> e)
        .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
        .windowBy(Windowing.Count.of(3))
        .output();

    Dataset<Pair<String, Set<String>>> mediator =
        MapElements.of(first)
        .using(e -> e)
        .output();

    // ~ should emit as soon as it receives input from the previous operator
    Dataset<Pair<String, Set<String>>> second = ReduceByKey.of(mediator)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .reduceBy((CombinableReduceFunction<Set<String>>) what -> {
          Set<String> s = new HashSet<>();
          s.add("!");
          for (Set<String> x : what) {
            s.addAll(x);
          }
          return s;
        })
        .output();

    // ~ emits as soon as it receives input from the previous operator
    Dataset<Pair<Long, Pair<String, Set<String>>>> third =
        MapElements.of(second)
        .using(what -> Pair.of(System.currentTimeMillis(), what))
        .output();

    ListDataSink<Pair<Long, Pair<String, Set<String>>>> output = ListDataSink.get(1);
    third.persist(output);

    executor.waitForCompletion(flow);
    LOG.debug("output.getOutput(0): {}", output.getOutput(0));

    assertNotNull(output.getOutput(0));
    assertEquals(3, output.getOutput(0).size());
    List<Pair<Long, Pair<String, Set<String>>>> ordered =
        output.getOutput(0)
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList());
    assertTrue(ordered.get(0).getFirst() + READ_DELAY_MS - 1 < ordered.get(1).getFirst());
    assertTrue(ordered.get(1).getFirst() + READ_DELAY_MS - 1 < ordered.get(2).getFirst());
    assertEquals(Sets.newHashSet("!", "t1-r-one", "t1-r-two", "t1-r-three"),
                 ordered.get(0).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "t1-s-one", "t1-s-two", "t1-s-three"),
                 ordered.get(1).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "t1-t-one"),
                 ordered.get(2).getSecond().getSecond());
  }
}