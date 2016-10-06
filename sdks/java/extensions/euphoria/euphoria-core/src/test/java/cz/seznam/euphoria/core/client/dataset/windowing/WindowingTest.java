
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.Session.SessionInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cz.seznam.euphoria.core.util.Util.sorted;
import static java.util.Arrays.asList;

import java.util.Map;
import static org.junit.Assert.*;

public class WindowingTest {

  private InMemExecutor executor;

  @Before
  public void setUp() {
    executor = new InMemExecutor();
  }

  static final class Item {
    final String word;
    final long evtTs;

    Item(String word, long evtTs) {
      this.word = word;
      this.evtTs = evtTs;
    }

    @Override
    public String toString() {
      return "Item{" +
          "word='" + word + '\'' +
          ", evtTs=" + evtTs +
          '}';
    }
  }

  @Test
  public void testWindowingByEventTime() throws Exception {
    // we have to make sure the timestamp is in the future
    ListDataSource<Item> input = ListDataSource.bounded(asList(
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

    Flow flow = Flow.create("Test");
    Dataset<Item> lines = flow.createInput(input);

    Dataset<Pair<Item, Long>> words = MapElements.of(lines)
        .using((UnaryFunction<Item, Pair<Item, Long>>) item -> Pair.of(item, 1L))
        .output();

    Dataset<Pair<String, Long>> reduced = ReduceByKey
        .of(words)
        .keyBy(e -> e.getFirst().word)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        // ~ windowing by one second using a user supplied event-time-fn
        .windowBy(Time.of(Duration.ofSeconds(1))
            .using((Pair<Item, Long> x) -> x.getFirst().evtTs))
        .output();

    Dataset<String> mapped = MapElements.of(reduced)
        .using(p -> p.getFirst() + "-" + p.getSecond())
        .output();

    mapped = FlatMap.of(mapped)
        .using((UnaryFunctor<String, String>) (elem, c) -> {
          TimeInterval w = (TimeInterval) c.getWindow();
          c.collect(w.getStartMillis() / 1000L + ": " + elem);
        })
        .output();

    ListDataSink<String> output = ListDataSink.get(1);
    mapped.persist(output);

    executor.waitForCompletion(flow);

    assertEquals(1, output.getOutputs().size());

    assertEquals(
        sorted(asList(
        /* 1st second window */ "1: one-1", "1: two-3", "1: three-1", "1: four-2",
        /* 2nd second window */ "2: one-3", "2: two-2", "2: three-2",
        /* 3rd second window */ "3: one-2")),
        sorted(output.getOutput(0)));
  }

  @Test
  public void testTimeBuilders() {
    assertTimeWindowing(
        Time.of(Duration.ofSeconds(100)),
        100 * 1000,
        null,
        Time.ProcessingTime.<String>get());

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(20)).earlyTriggering(Duration.ofSeconds(10)),
        20 * 1000,
        Duration.ofSeconds(10),
        Time.ProcessingTime.<Pair<Long, String>>get());

    UnaryFunction<Pair<Long, Long>, Long> evtf = event-> 0L;

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(4))
            .earlyTriggering(Duration.ofSeconds(10))
            .using(evtf),
        4 * 1000,
        Duration.ofSeconds(10),
        evtf);

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(3))
            .earlyTriggering(Duration.ofHours(1))
            .using(evtf),
        3 * 1000,
        Duration.ofHours(1),
        evtf);

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(8)).using(evtf),
        8 * 1000,
        null,
        evtf);
  }

  private <T> void assertTimeWindowing(Time<T> w,
                                       long expectDurationMillis,
                                       Duration expectEarlyTriggeringPeriod,
                                       UnaryFunction<T, Long> expectedFn) {
    
    assertNotNull(w);
    assertEquals(expectEarlyTriggeringPeriod, w.earlyTriggeringPeriod);
    assertEquals(expectDurationMillis, w.durationMillis);
    assertSame(expectedFn, w.eventTimeFn);
  }

  @Test
  public void testAttachedWindowing_ContinuousOutput() {
    final Duration READ_DELAY = Duration.ofMillis(37L);
    Flow flow = Flow.create("Test");

    // ~ one partition; supplying every READ_DELAYS a new element
    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList(("r-one r-two r-three s-one s-two s-three t-one")
            .split(" "))
    ).withReadDelay(READ_DELAY));

    // ~ emits after 3 input elements received due to "count windowing"
    Dataset<Pair<String, HashSet<String>>> first = ReduceByKey.of(input)
        .keyBy(e -> "")
        .valueBy(e -> e)
        .reduceBy(Sets::newHashSet)
        .windowBy(Count.of(3))
        .output();

    // ~ consume the output of the previous operator and forward to the next
    // serving merely as another operator in the pipeline; must emit its
    // inputs elements as soon they arrive (note: this is a
    // non-window-wise operator)
    Dataset<Pair<String, HashSet<String>>> mediator = MapElements.of(first)
        .using(e -> e)
        .output();

    // ~ a window-wise operator with the default "attached windowing". it's
    // attaching itself to the windowing of the preceding window-wise operator
    // in the pipeline and is supposed to emit equivalent windows as that one.
    // further, the operator is supposed to emit the windows as soon as possible,
    // i.e. once the last item for a window from the preceding window-wise operator
    // is received.
    Dataset<Pair<String, HashSet<String>>> second = ReduceByKey.of(mediator)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .reduceBy(what -> {
          HashSet<String> s = new HashSet<>();
          s.add("!");
          for (Set<String> x : what) {
            s.addAll(x);
          }
          return s;
        })
        .output();

    // ~ consume the output and put a timestamp on each element. emits output
    // itself as soon as it receives input, due to the operator's streaming nature.
    Dataset<Pair<Long, Pair<String, HashSet<String>>>> third =
        MapElements.of(second)
        .using(what -> Pair.of(System.currentTimeMillis(), what))
        .output();

    ListDataSink<Pair<Long, Pair<String, HashSet<String>>>> output = ListDataSink.get(1);
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
    List<Pair<Long, Pair<String, HashSet<String>>>> ordered =
        output.getOutput(0)
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList());
    assertEquals(3, ordered.size());
    // ~ if no threads got blocked (e.g. system overload) we shall receive an
    // output element approx. every 3*READ_DELAY (READ_DELAY for every read
    // element from the input source times window-of-3-items) except for the very
    // last item which is triggered earlier due to "end-of-stream"
    assertSmaller(
        ordered.get(0).getFirst() + 2 * READ_DELAY.toMillis(), ordered.get(1).getFirst());
    assertEquals(Sets.newHashSet("!", "r-one", "r-two", "r-three"),
                 ordered.get(0).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "s-one", "s-two", "s-three"),
                 ordered.get(1).getSecond().getSecond());
    assertEquals(Sets.newHashSet("!", "t-one"),
                 ordered.get(2).getSecond().getSecond());
  }

  private void assertSmaller(long x, long y) {
    assertTrue("Expected x < y but it's not with x=" + x + " and y=" + y, x < y);
  }

  @Test
  public void testAttachedWindowing_InhertitedLabel() throws Exception {
    Flow flow = Flow.create("Test");

    Dataset<Pair<String, Long>> input =
        flow.createInput(ListDataSource.unbounded(asList(
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
            Pair.of("four", 2000000001004L)))
            .withReadDelay(Duration.ofMillis(100L)));

    Dataset<WindowedPair<TimeInterval, String, Void>> distinctRBK =
        ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> (Void) null)
        .combineBy(e -> null)
        .windowBy(Time.of(Duration.ofSeconds(1)).using(Pair::getSecond))
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
  }

  @Test
  public void testWindowing_EndOfWindow_RBK_ManyPartitions() {
    // ~ this causes the initial reduce-by-key to output three
    // partitions of which only one receives data.
    testWindowing_EndOfWindowImpl(2);
  }

  private void testWindowing_EndOfWindowImpl(int dataPartitions) {
    final Duration READ_DELAY = Duration.ofMillis(50L);
    Flow flow = Flow.create("Test");

    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" "))
    ).withReadDelay(READ_DELAY));

    // ~ create windows of size three
    Dataset<Pair<String, Set<String>>> first =
        ReduceByKey.of(input)
        .keyBy(e -> "")
        .valueBy(e -> e)
        .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
        .setNumPartitions(dataPartitions)
        .windowBy(Count.of(3))
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
    Dataset<Set<String>> fifth =
        ReduceWindow.of(fourth)
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
            .output();

    ListDataSink<Set<String>> out = ListDataSink.get(1);
    fifth.persist(out);

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

  @SuppressWarnings("unchecked")
  <LABEL, T> Set<WindowID<LABEL>> assignWindows(
      Windowing<T, LABEL, ?> windowing, T elem) {
    return windowing.assignWindowsToElement(
        new WindowedElement(new WindowID(null), elem));
  }

  @Test
  public void testWindowing_SessionMergeWindows() {
    Session<Long> windowing =
        Session.of(Duration.ofSeconds(10))
            .using(e -> e);

    WindowID<SessionInterval> w1 = assertSessionWindow(
        assignWindows(windowing, 1_000L), 1_000L, 11_000L);
    WindowID<SessionInterval> w2 = assertSessionWindow(
        assignWindows(windowing, 10_000L), 10_000L, 20_000L);
    WindowID<SessionInterval> w3 = assertSessionWindow(
        assignWindows(windowing, 21_000L), 21_000L, 31_000L);
    // ~ a small window which is fully contained in w1
    WindowID<SessionInterval> w4 = new WindowID<>(
        new SessionInterval(3_000L, 8_000L));


    
    Map<WindowID<SessionInterval>, WindowID<SessionInterval>> merges;
    merges = windowing.mergeWindows(
        Arrays
            .asList(w4, w3, w2, w1)
            .stream()
            .map(windowing::createWindowContext)
            .collect(Collectors.toList()))
        .stream()
        .flatMap(p -> {
          return p.getFirst().stream()
              .map(w -> Pair.of(w.getWindowID(), p.getSecond().getWindowID()));
        })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    assertEquals(3L, merges.size());
    assertNotNull(merges.get(w1));
    assertNotNull(merges.get(w4));
    assertNotNull(merges.get(w2));

    Set<WindowID<SessionInterval>> target = merges.values().stream().
        collect(Collectors.toSet());
    assertEquals(1, target.size());
    WindowID<SessionInterval> targetWindow = target.iterator().next();
    assertSessionWindow(targetWindow, 1_000L, 20_000L);
  }

  private WindowID<Session.SessionInterval> assertSessionWindow(
      Set<WindowID<Session.SessionInterval>> window,
      long expectedStartMillis, long expectedEndMillis)
  {
    WindowID<SessionInterval> w = Iterables.getOnlyElement(window);
    assertSessionWindow(w, expectedStartMillis, expectedEndMillis);
    return w;
  }

  private void assertSessionWindow(
      WindowID<Session.SessionInterval> window,
      long expectedStartMillis, long expectedEndMillis) {

    assertNotNull(window);
    SessionInterval label = window.getLabel();
    assertNotNull(label);
    assertEquals(expectedStartMillis, label.getStartMillis());
    assertEquals(expectedEndMillis, label.getEndMillis());
  }

  @Test
  public void testWindowing_SessionWindowing0() throws Exception {
    final long NOW = System.currentTimeMillis() + 15_000L;

    Flow flow = Flow.create("Test");

    Dataset<Item> input = flow.createInput(ListDataSource.unbounded(asList(
        new Item("1-one",   1),
        new Item("2-one",   2),
        new Item("1-two",   4),
        new Item("1-three", 8),
        new Item("1-four",  10),
        new Item("2-two",   10),
        new Item("1-five",  18),
        new Item("2-three", 20),
        new Item("1-six",   22)))
        .withReadDelay(Duration.ofMillis(900)));

    ListDataSink<WindowedPair<SessionInterval, Integer, HashSet<String>>> out
        = ListDataSink.get(1);

    ReduceByKey.of(input)
        .keyBy(e -> e.word.charAt(0) - '0')
        .valueBy(e -> e.word)
        .reduceBy(Sets::newHashSet)
        .windowBy(Session.of(Duration.ofSeconds(5))
            .using(e -> NOW + e.evtTs * 1_000L))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(out);

    executor.waitForCompletion(flow);

    // ~ prepare the output for comparison
    List<String> flat = new ArrayList<>();
    for (WindowedPair<SessionInterval, Integer, HashSet<String>> o : out.getOutput(0)) {
      StringBuilder buf = new StringBuilder();
      buf.append("(")
          .append((o.getWindowLabel().getStartMillis() - NOW) / 1_000L)
          .append("-")
          .append((o.getWindowLabel().getEndMillis() - NOW) / 1_000L)
          .append("): ");
      buf.append(o.getFirst()).append(": ");
      ArrayList<String> xs = new ArrayList<>(o.getSecond());
      xs.sort(Comparator.naturalOrder());
      Joiner.on(", ").appendTo(buf, xs);
      flat.add(buf.toString());
    }
    flat.sort(Comparator.naturalOrder());
    assertEquals(
        sorted(asList(
            "(1-15): 1: 1-four, 1-one, 1-three, 1-two",
            "(10-15): 2: 2-two",
            "(18-27): 1: 1-five, 1-six",
            "(2-7): 2: 2-one",
            "(20-25): 2: 2-three")),
        flat);
  }
}
