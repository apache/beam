
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.GroupByKey;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * {@code InMemExecutor} test suite.
 * The {@code InMemExecutor} stands on the basic operators, so we just
 * need to test it correctly implements all of them. Next we need to test
 * that it can process complex flows with many partitions.
 */
public class InMemExecutorTest {

  InMemExecutor executor;
  Flow flow;
  
  @Before
  public void setup() {
    executor = new InMemExecutor();
    flow = Flow.create("Test");
  }
  
  @After
  public void teardown() {
    executor.abort();
  }

  // Repartition operator

  @Test
  public void simpleRepartitionTest() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6)));
    // repartition even and odd elements to different partitions
    Dataset<Integer> repartitioned = Repartition.of(ints)
        .setPartitioner(e -> e % 2)
        .setNumPartitions(2)
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(2);

    repartitioned.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    // first partition contains even numbers
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    // second partition contains odd numbers
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  @Test
  // test that repartition works from 2 to 3 partitions
  public void upRepartitionTest() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6)));
    // repartition even and odd elements to different partitions
    Dataset<Integer> repartitioned = Repartition.of(ints)
        .setPartitioner(e -> e % 3)
        .setNumPartitions(3)
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(3);

    repartitioned.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(3, outputs.size());
    assertUnorderedEquals(Arrays.asList(3, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(4, 1), outputs.get(1));
    assertUnorderedEquals(Arrays.asList(5, 2), outputs.get(2));
  }

  @Test
  // test that repartition works from 3 to 2 partitions
  public void downRepartitionTest() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(1, 2),
            Arrays.asList(3, 4),
            Arrays.asList(5, 6)));
    // repartition even and odd elements to different partitions
    Dataset<Integer> repartitioned = Repartition.of(ints)
        .setPartitioner(e -> e % 2)
        .setNumPartitions(2)
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(2);

    repartitioned.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  @Test
  // test that repartition works from 3 to 2 partitions
  public void downRepartitionTestWithHashPartitioner() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6)));
    // repartition even and odd elements to different partitions
    Dataset<Integer> repartitioned = Repartition.of(ints)
        .setNumPartitions(2)
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(2);

    repartitioned.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  // Union operator
  
  @Test
  public void simpleUnionTest() {
    Dataset<Integer> first = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(1),
            Arrays.asList(2, 3, 4, 5, 6)));
    Dataset<Integer> second = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(7, 8, 9)));

    Dataset<Integer> union = Union.of(first, second)
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(1);

    Repartition.of(union)
        .setNumPartitions(1)
        .output()
        .persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(1, outputs.size());
    assertUnorderedEquals(
        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), outputs.get(0));
  }

  // FlatMap operator

  @Test
  public void simpleFlatMapTest() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(0, 1, 2, 3),
            Arrays.asList(4, 5, 6)));

    // repeat each element N N count
    Dataset<Integer> output = FlatMap.of(ints)
        .using((Integer e, Collector<Integer> c) -> {
          for (int i = 0; i < e; i++) {
            c.collect(e);
          }
        })
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    // this must be equal including ordering and partitioning
    assertEquals(Arrays.asList(1, 2, 2, 3, 3, 3), outputs.get(0));
    assertEquals(Arrays.asList(4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6),
        outputs.get(1));
  }

  // ReduceStateByKey operator

  /**
   * Simple sort state for tests.
   * This state takes comparable elements and produces sorted sequence.
   */
  public static class SortState extends State<Integer, Integer> {

    List<Integer> data = new ArrayList<>();

    SortState(Collector<Integer> c) {
      super(c);
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      data.stream().sorted().forEachOrdered(
          c -> this.getCollector().collect(c));
    }

    static SortState combine(Iterable<SortState> others) {
      SortState ret = null;
      for (SortState s : others) {
        if (ret == null) {
          ret = new SortState(s.getCollector());
        }
        ret.data.addAll(s.data);
      }
      return ret;
    }

  }

  static class CountLabel {
    final int count;
    int get() { return count; }
    // on purpose no hashcode or equals
    CountLabel(int count) { this.count = count; }
    @Override
    public String toString() { return String.valueOf(count); }
  }



  private static class CountWindowContext<GROUP>
      extends WindowContext<GROUP, CountLabel> {

    final int maxSize;
    int size = 1;

    public CountWindowContext(WindowID<GROUP, CountLabel> wid, int maxSize) {
      super(wid);
      this.maxSize = maxSize;
    }

    public CountWindowContext(GROUP group, int maxSize) {
      this(WindowID.unaligned(group, new CountLabel(maxSize)), maxSize);
    }

    public CountWindowContext(GROUP group, CountLabel label) {
      this(WindowID.unaligned(group, label), label.get());
    }


    @Override
    public String toString() {
      return "CountWindowContext(" + getWindowID() + ", " + size + ")";
    }


  }


  static class UnalignedCountWindowing<T, GROUP> implements
      MergingWindowing<T, GROUP, CountLabel, CountWindowContext<GROUP>> {

    final UnaryFunction<T, GROUP> groupExtractor;
    final UnaryFunction<GROUP, Integer> size;

    UnalignedCountWindowing(
        UnaryFunction<T, GROUP> groupExtractor,
        UnaryFunction<GROUP, Integer> size) {
      this.groupExtractor = groupExtractor;
      this.size = size;
    }

    @Override
    public Collection<Pair<Collection<CountWindowContext<GROUP>>, CountWindowContext<GROUP>>> mergeWindows(
        Collection<CountWindowContext<GROUP>> actives) {

      // we will merge together only windows with the same window size

      List<Pair<Collection<CountWindowContext<GROUP>>, CountWindowContext<GROUP>>> ret
          = new ArrayList<>();

      Map<Integer, List<CountWindowContext<GROUP>>> toMergeMap = new HashMap<>();
      Map<Integer, AtomicInteger> currentSizeMap = new HashMap<>();

      for (CountWindowContext<GROUP> w : actives) {
        final int wSize = w.maxSize;
        AtomicInteger currentSize = currentSizeMap.get(wSize);
        if (currentSize == null) {
          currentSize = new AtomicInteger(0);
          currentSizeMap.put(wSize, currentSize);
          toMergeMap.put(wSize, new ArrayList<>());
        }
        if (currentSize.get() + w.size <= wSize) {
          currentSize.addAndGet(w.size);
          toMergeMap.get(wSize).add(w);
        } else {
          List<CountWindowContext<GROUP>> toMerge = toMergeMap.get(wSize);
          if (!toMerge.isEmpty()) {
            CountWindowContext<GROUP> res = new CountWindowContext<>(
                w.getWindowID().getGroup(), currentSize.get());
            res.size = currentSize.get();
            ret.add(Pair.of(new ArrayList<>(toMerge), res));
            toMerge.clear();
          }
          toMerge.add(w);
          currentSize.set(w.size);
        }
      }

      for (List<CountWindowContext<GROUP>> toMerge : toMergeMap.values()) {
        if (!toMerge.isEmpty()) {
          CountWindowContext<GROUP> first = toMerge.get(0);
          CountWindowContext<GROUP> res = new CountWindowContext<>(
              first.getWindowID().getGroup(), first.maxSize);
          res.size = currentSizeMap.get(first.maxSize).get();
          ret.add(Pair.of(toMerge, res));
        }
      }
      return ret;
    }

    @Override
    public Set<WindowID<GROUP, CountLabel>> assignWindowsToElement(
        WindowedElement<?, ?, T> input) {
      GROUP g = groupExtractor.apply(input.get());
      int sizeForGroup = size.apply(g);
      return new HashSet<>(Arrays.asList(
          WindowID.unaligned(g, new CountLabel(sizeForGroup)),
          WindowID.unaligned(g, new CountLabel(2 * sizeForGroup))));
    }

    
    @Override
    public boolean isComplete(CountWindowContext<GROUP> window) {
      return window.size == window.maxSize;
    }

    @Override
    public CountWindowContext<GROUP> createWindowContext(
        WindowID<GROUP, CountLabel> wid) {
      return new CountWindowContext<>(wid, wid.getLabel().get());
    }

  }


  @Test
  public void testReduceByKeyWithSortStateAndUnalignedWindow() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            reversed(sequenceInts(0, 100)),
            reversed(sequenceInts(100, 1100))));

    UnalignedCountWindowing<Integer, Integer> windowing =
        new UnalignedCountWindowing<>(i -> i % 10, i -> i + 1);

    // the key for sort will be the last digit
    Dataset<WindowedPair<CountLabel, Integer, Integer>> output = ReduceStateByKey.of(ints)
        .keyBy(i -> i % 10)
        .valueBy(e -> e)
        .stateFactory(SortState::new)
        .combineStateBy(SortState::combine)
        .windowBy(windowing)
        .outputWindowed();

    // collector of outputs
    ListDataSink<WindowedPair<CountLabel, Integer, Integer>> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<WindowedPair<CountLabel, Integer, Integer>>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());

    // each partition should have 550 items in each window set
    assertEquals(2 * 550, outputs.get(0).size());
    assertEquals(2 * 550, outputs.get(1).size());

    Set<Integer> firstKeys = outputs.get(0).stream()
        .map(Pair::getFirst).distinct()
        .collect(Collectors.toSet());

    // validate that the two partitions contain different keys
    outputs.get(1).forEach(p -> assertFalse(firstKeys.contains(p.getFirst())));

    outputs.forEach(this::checkKeyAlignedSortedList);

  }


  private void checkKeyAlignedSortedList(
      List<WindowedPair<CountLabel, Integer, Integer>> list) {

    Map<CountLabel, Map<Integer, List<Integer>>> sortedSequencesInWindow = new HashMap<>();

    for (WindowedPair<CountLabel, Integer, Integer> p : list) {
      Map<Integer, List<Integer>> sortedSequences = sortedSequencesInWindow.get(
          p.getWindowLabel());
      if (sortedSequences == null) {
        sortedSequencesInWindow.put(p.getWindowLabel(),
            sortedSequences = new HashMap<>());
      }
      List<Integer> sorted = sortedSequences.get(p.getKey());
      if (sorted == null) {
        sortedSequences.put(p.getKey(), sorted = new ArrayList<>());
      }
      sorted.add(p.getValue());
    }

    assertFalse(sortedSequencesInWindow.isEmpty());
    int totalCount = 0;
    for (Map.Entry<CountLabel, Map<Integer, List<Integer>>> we : sortedSequencesInWindow.entrySet()) {
      assertFalse(we.getValue().isEmpty());
      for (Map.Entry<Integer, List<Integer>> e : we.getValue().entrySet()) {
        // now, each list must be sorted
        int last = -1;
        for (int i : e.getValue()) {
          assertTrue("Sequence " + e.getValue() + " is not sorted", last < i);
          last = i;
          totalCount++;
        }
      }
    }
    assertEquals(1100, totalCount);
  }

  // reverse given list
  private static <T> List<T> reversed(List<T> what) {
    Collections.reverse(what);
    return what;
  }


  // produce random N random ints as list
  private static List<Integer> sequenceInts(int from, int to) {
    List<Integer> ret = new ArrayList<>();
    for (int i = from; i < to; i++) {
      ret.add(i);
    }
    return ret;
  }


  // check that given lists are equal irrespecitve of order
  public static <T extends Comparable<T>> void assertUnorderedEquals(
      List<T> first, List<T> second) {
    List<T> firstCopy = new ArrayList<>(first);
    List<T> secondCopy = new ArrayList<>(second);
    Collections.sort(firstCopy);
    Collections.sort(secondCopy);
    assertEquals(firstCopy, secondCopy);
  }

  @Test(timeout = 5000L)
  public void testInputMultiConsumption() {
    final int N = 1000;
    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(sequenceInts(0, N)));

    // ~ consume the input another time
    Dataset<Integer> map = MapElements
        .of(input)
        .using(e -> e)
        .output();
    ListDataSink<Integer> mapOut = ListDataSink.get(1);
    map.persist(mapOut);

    Dataset<Pair<Integer, Integer>> sum = ReduceByKey
        .of(input)
        .keyBy(e -> 0)
        .valueBy(e -> e)
        .reduceBy(Sums.ofInts())
        .output();
    ListDataSink<Pair<Integer, Integer>> sumOut = ListDataSink.get(1);
    sum.persist(sumOut);

    executor.waitForCompletion(flow);

    assertNotNull(sumOut.getOutput(0));
    assertEquals(1, sumOut.getOutput(0).size());
    assertEquals(Integer.valueOf((N-1) * N / 2),
                 sumOut.getOutput(0).get(0).getSecond());

    assertNotNull(mapOut.getOutput(0));
    assertEquals(N, mapOut.getOutput(0).size());
    assertEquals(Integer.valueOf((N-1) * N / 2),
                 mapOut.getOutput(0).stream().reduce((x, y) -> x + y).get());
  }


  @Test
  public void testWithWatermarkAndEventTime() throws Exception {
    
    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(sequenceInts(0, N))
            .withReadDelay(Duration.ofMillis(2)));

    ListDataSink<Pair<String, Long>> outputs = ListDataSink.get(2);

    ReduceByKey.of(input)
        .keyBy(e -> "") // reduce all
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)).using(e -> e * 1000L))
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));
    
    // run the executor in separate thread in order to be able to watch
    // the partial results
    Thread exec = new Thread(() ->  executor.waitForCompletion(flow));
    exec.start();

    // sleep for one second
    Thread.sleep(1000L);

    // the data in first unfinished partition
    List<Pair<String, Long>> output = new ArrayList<>(
        outputs.getUncommittedOutputs().get(0));


    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w.getSecond() != 10).count() <= 1);

    exec.join();

    output = outputs.getOutputs().get(0);

    output.forEach(w -> assertEquals("Each window should have 10 elements, got "
        + w.getSecond(), 10L, (long) w.getSecond()));

    // we have 2000 elements split into 200 windows
    assertEquals(200, output.size());
    
  }


  @Test
  public void testWithWatermarkAndEventTimeAndDiscarding() throws Exception {

    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(reversed(sequenceInts(0, N))));

    ListDataSink<Pair<String, Long>> outputs = ListDataSink.get(2);

    ReduceByKey.of(input)
        .keyBy(e -> "") // reduce all
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)).using(e -> e * 1000L))
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));

    executor.waitForCompletion(flow);

    // there should be only one element on output - the first element
    // all other windows are discarded
    List<Pair<String, Long>> output = outputs.getOutputs().get(0);
    assertEquals(1, output.size());
  }

  @Test
  public void testWithWatermarkAndEventTimeMixed() throws Exception {

    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(sequenceInts(0, N))
            .withReadDelay(Duration.ofMillis(2)));

    // first add some fake operator operating on processing time
    // doing virtually nothing
    Dataset<Pair<String, Set<Integer>>> reduced = ReduceByKey.of(input)
        .keyBy(e -> "")
        .reduceBy((Iterable<Integer> values) -> {
          Set<Integer> grp = new TreeSet<>();
          for (Integer i : values) {
            grp.add(i);
          }
          return grp;
        })
        .windowBy(Batch.get())
        .output();

    // explode it back to the original input (more maybe reordered)
    // and store it as the original input, process it further in
    // the same way as in `testWithWatermarkAndEventTime'
    input = FlatMap.of(reduced)
        .using((Pair<String, Set<Integer>> grp, Collector<Integer> c) -> {
          for (Integer i : grp.getSecond()) {
            c.collect(i);
          }
        }).output();

    ListDataSink<Pair<String, Long>> outputs = ListDataSink.get(2);

    ReduceByKey.of(input)
        .keyBy(e -> "") // reduce all
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)).using(e -> e * 1000L))
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));
    
    executor.waitForCompletion(flow);

    // the data in first unfinished partition
    List<Pair<String, Long>> output = new ArrayList<>(
        outputs.getUncommittedOutputs().get(0));


    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w.getSecond() != 10).count() <= 1);

    output = outputs.getOutputs().get(0);

    output.forEach(w -> assertEquals("Each window should have 10 elements, got "
        + w.getSecond(), 10L, (long) w.getSecond()));

    // we have 2000 elements split into 200 windows
    assertEquals(200, output.size());

  }




  @Test(timeout = 2000)
  public void testGroupedDatasetReduceByKey() throws Exception {
    Flow flow = Flow.create("Test");

    ListDataSource<Pair<Integer, String>> input =
        ListDataSource.bounded(Arrays.asList(
            Pair.of(1, "one"),
            Pair.of(1, "two"),
            Pair.of(1, "three"),
            Pair.of(1, "one"),
            Pair.of(2, "two"),
            Pair.of(1, "three"),
            Pair.of(1, "three")));

    Dataset<Pair<Integer, String>> pairs = flow.createInput(input);

    GroupedDataset<Integer, String> grouped = GroupByKey.of(pairs)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .output();

    Dataset<Pair<CompositeKey<Integer, String>, Long>> output = ReduceByKey.of(grouped)
        .keyBy(e -> e)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .output();

    ListDataSink<Pair<CompositeKey<Integer, String>, Long>> out = ListDataSink.get(1);
    output.persist(out);

    InMemExecutor executor = new InMemExecutor();
    executor.waitForCompletion(flow);

    assertUnorderedEquals(
        Arrays.asList("1-one:2", "1-two:1", "1-three:3", "2-two:1"),
        out.getOutput(0).stream().map(p -> {
          assertEquals(Integer.class, p.getFirst().getFirst().getClass());
          assertEquals(String.class, p.getFirst().getSecond().getClass());
          assertEquals(Long.class, p.getSecond().getClass());
          return p.getFirst().getFirst() + "-"
              + p.getFirst().getSecond() + ":"
              + p.getSecond();
        }).collect(Collectors.toList()));
  }

}
