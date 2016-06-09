package cz.seznam.euphoria.core.client.dataset;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.dataset.Windowing.Time.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Fluent;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
  public void testAttachedWindowing_ContinuousOutput() {
    final long READ_DELAY_MS = 100L;
    Flow flow = Flow.create("Test", settings);

    // ~ one partition; supplying every READ_DELAYS_MS a new element
    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList(("r-one r-two r-three s-one s-two s-three t-one")
            .split(" "))
    ).setSleepTime(READ_DELAY_MS));

    // ~ emits after 3 input elements received due to "count windowing"
    Dataset<Pair<String, Set<String>>> first =
        ReduceByKey.of(input)
        .keyBy(e -> "")
        .valueBy(e -> e)
        .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
        .windowBy(Windowing.Count.of(3))
        .output();

    // ~ consume the output of the previous operator and forward to the next
    // serving merely as another operator in the pipeline; must emit its
    // inputs elements as soon they arrive (note: this is a
    // non-window-wise operator)
    Dataset<Pair<String, Set<String>>> mediator =
        MapElements.of(first)
        .using(e -> e)
        .output();

    // ~ a window-wise operator with the default "attached windowing". it's
    // attaching itself to the windowing of the preceding window-wise operator
    // in the pipeline and is supposed to emit equivalent windows as that one.
    // further, the operator is supposed to emit the windows as soon as possible,
    // i.e. once the last item for a window from the preceding window-wise operator
    // is received.
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

    // ~ consume the output and put a timestamp on each element. emits output
    // itself as soon as it receives input, due to the operator's streaming nature.
    Dataset<Pair<Long, Pair<String, Set<String>>>> third =
        MapElements.of(second)
        .using(what -> Pair.of(System.currentTimeMillis(), what))
        .output();

    ListDataSink<Pair<Long, Pair<String, Set<String>>>> output = ListDataSink.get(1);
    third.persist(output);

    executor.waitForCompletion(flow);

    assertNotNull(output.getOutput(0));
    assertEquals(3, output.getOutput(0).size());
    output.getOutput(0).forEach(x -> {
      assertNotNull(x);
      assertNotNull(x.getFirst());
      assertNotNull(x.getSecond());
      assertNotNull(x.getSecond().getFirst());
      assertNotNull(x.getSecond().getSecond());
    });
    List<Pair<Long, Pair<String, Set<String>>>> ordered =
        output.getOutput(0)
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList());
    assertEquals(3, ordered.size());
    // ~ if no threads got blocked (e.g. system overload) we shall receive at
    // output element approx. every 3*READ_DELAY_MS (READ_DELAY_MS for every read
    // element from the input source times window-of-3-items) except for the very
    // last item which is triggered earlier due to "end-of-stream"
    assertTrue(ordered.get(0).getFirst() + READ_DELAY_MS < ordered.get(1).getFirst());
    assertTrue(ordered.get(1).getFirst() + READ_DELAY_MS < ordered.get(2).getFirst());
    assertEquals(Sets.newHashSet("!", "r-one", "r-two", "r-three"),
                 ordered.get(0).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "s-one", "s-two", "s-three"),
                 ordered.get(1).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "t-one"),
                 ordered.get(2).getSecond().getSecond());
  }

  @Test
  public void testAttachedWindowing_InhertitedLabel() throws Exception {
    Flow flow = Flow.create("Test", settings);

    InMemFileSystem.get().setFile("/tmp/foo.txt", Duration.ofMillis(100L),
        asList(
            Pair.of("one", 2000000000000L),
            Pair.of("one", 2000000000001L),
            Pair.of("two", 2000000000002L),
            Pair.of("two", 2000000000003L),
            Pair.of("three", 2000000000004L),
            Pair.of("four", 2000000000005L),
            Pair.of("five", 2000000000006L),
            Pair.of("one", 2000000001001L),
            Pair.of("two", 2000000001002L),
            Pair.of("three", 2000000001003L),
            Pair.of("four", 2000000001004L)));

    Dataset<Pair<String, Long>> input =
        flow.createInput(URI.create("inmem:///tmp/foo.txt"));

    Dataset<WindowedPair<TimeInterval, String, Void>> distinctRBK =
        ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> (Void) null)
        .combineBy(e -> null)
        .windowBy(Windowing.Time.seconds(1).using(Pair::getSecond))
        .outputWindowed();

    Dataset<Pair<TimeInterval, Long>> counts = ReduceByKey.of(distinctRBK)
        .keyBy(WindowedPair::getWindowLabel)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .output();

    ListDataSink<Pair<TimeInterval, Long>> output = ListDataSink.get(1);
    counts.persist(output);

    executor.waitForCompletion(flow);

    assertEquals(2, output.getOutput(0).size());
    List<Pair<Long, Long>> ordered = output.getOutput(0)
        .stream()
        .sorted(Comparator.comparing(e -> e.getFirst().getStartMillis()))
        .map(e -> Pair.of(e.getFirst().getStartMillis(), e.getSecond()))
        .collect(Collectors.toList());
    assertEquals(asList(
        Pair.of(2000000000000L, 5L), Pair.of(2000000001000L, 4L)),
        ordered);
  }

  @Test
  public void testWindowing_EndOfWindow_RBK_OnePartition() {
    testWindowing_EndOfWindowImpl(1);
    testWindowing_EndOfWindowImpl_Fluent(1);
  }

  @Test
  public void testWindowing_EndOfWindow_RBK_ManyPartitions() {
    // ~ this causes the initial reduce-by-key to output three
    // partitions of which only one receives data.
    testWindowing_EndOfWindowImpl(2);
    testWindowing_EndOfWindowImpl_Fluent(2);
  }

  private void testWindowing_EndOfWindowImpl_Fluent(int dataPartitions) {
    final long READ_DELAY_MS = 100L;
    ListDataSink<Set<String>> out = ListDataSink.get(1);
    Fluent.flow("Test", settings)
        .read(ListDataSource.unbounded(
            asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" ")))
            .setSleepTime(READ_DELAY_MS))
        // ~ create windows of size three
        .apply(input -> ReduceByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
            .setNumPartitions(dataPartitions)
            .windowBy(Windowing.Count.of(3)))
        // ~ strip the needless key and flatten out the elements thereby
        // creating multiple elements in the output belonging to the same window
        .flatMap((Pair<String, Set<String>> e, Collector<String> c) ->
            e.getSecond().stream().forEachOrdered(c::collect))
        // ~ now spread the elements (belonging to the same window) over
        // multiple partitions
        .repartition(2, e -> '0' - e.charAt(0))
        // ~ now reduce all of the partitions to one
        .repartition(1)
        // ~ now process the single partition
        // ~ we now expect to reconstruct the same windowing
        // as the very initial step
        .apply(input -> ReduceByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet))
        // ~ strip the needless key
        .mapElements(Pair::getSecond)
        .persist(out)
        .execute(executor);
  }

  private void testWindowing_EndOfWindowImpl(int dataPartitions) {
    final long READ_DELAY_MS = 100L;
    Flow flow = Flow.create("Test", settings);

    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" "))
    ).setSleepTime(READ_DELAY_MS));

    // ~ create windows of size three
    Dataset<Pair<String, Set<String>>> first =
        ReduceByKey.of(input)
        .keyBy(e -> "")
        .valueBy(e -> e)
        .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
        .setNumPartitions(dataPartitions)
        .windowBy(Windowing.Count.of(3))
        .output();
    // ~ strip the needless key and flatten out the elements thereby
    // creating multiple elements in the output belonging to the same window
    Dataset<String> second = FlatMap.of(first)
        .using((UnaryFunctor<Pair<String, Set<String>>, String>) (e, c) -> {
          e.getSecond().stream().forEachOrdered(c::collect);
        })
        .output();

    // ~ now spread the elements (belonging to the same window) over
    // multiple partitions
    Dataset<String> third = Repartition.of(second)
        .setNumPartitions(2)
        .setPartitioner((Partitioner<String>) element -> '0' - element.charAt(0))
        .output();

    // ~ now reduce all of the partitions to one
    Dataset<String> fourth = Repartition.of(third)
        .setNumPartitions(1)
        .output();

    // ~ now process the single partition
    // ~ we now expect to reconstruct the same windowing
    // as the very initial step
    Dataset<Pair<String, Set<String>>> fifth =
        ReduceByKey.of(fourth)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
            .output();

    // ~ strip the needless key
    Dataset<Set<String>> sixth =
        MapElements.of(fifth)
            .using(Pair::getSecond)
            .output();

    ListDataSink<Set<String>> out = ListDataSink.get(1);
    sixth.persist(out);

    executor.waitForCompletion(flow);

    assertEquals(3, out.getOutput(0).size());
    assertEquals(
        Sets.newHashSet("0-one", "1-two", "0-three"),
        out.getOutput(0).get(0));
    assertEquals(
        Sets.newHashSet("1-four", "0-five", "1-six"),
        out.getOutput(0).get(1));
    assertEquals(
        Sets.newHashSet("0-seven"),
        out.getOutput(0).get(2));
  }
}