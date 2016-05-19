package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.Map;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.executor.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static cz.seznam.euphoria.core.util.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class WindowingTest {

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

    Dataset<Pair<Item, Long>> words = Map.of(lines)
        .by((UnaryFunction<Item, Pair<Item, Long>>) item -> Pair.of(item, 1L))
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

    Dataset<String> mapped = Map.of(reduced)
        .by(p -> p.getFirst() + "-" + p.getSecond())
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
}
