package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.GroupByKey;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  public void simpleRepartitionTest() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    // first partition contains even numbers
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    // second partition contains odd numbers
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  @Test
  // test that repartition works from 2 to 3 partitions
  public void upRepartitionTest() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(3, outputs.size());
    assertUnorderedEquals(Arrays.asList(3, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(4, 1), outputs.get(1));
    assertUnorderedEquals(Arrays.asList(5, 2), outputs.get(2));
  }

  @Test
  // test that repartition works from 3 to 2 partitions
  public void downRepartitionTest() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  @Test
  // test that repartition works from 3 to 2 partitions
  public void downRepartitionTestWithHashPartitioner() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());
    assertUnorderedEquals(Arrays.asList(2, 4, 6), outputs.get(0));
    assertUnorderedEquals(Arrays.asList(1, 3, 5), outputs.get(1));
  }

  // Union operator
  
  @Test
  public void simpleUnionTest() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

    List<List<Integer>> outputs = outputSink.getOutputs();
    assertEquals(1, outputs.size());
    assertUnorderedEquals(
        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), outputs.get(0));
  }

  // FlatMap operator

  @Test
  public void simpleFlatMapTest() throws InterruptedException, ExecutionException {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            Arrays.asList(0, 1, 2, 3),
            Arrays.asList(4, 5, 6)));

    // repeat each element N N count
    Dataset<Integer> output = FlatMap.of(ints)
        .using((Integer e, Context<Integer> c) -> {
          for (int i = 0; i < e; i++) {
            c.collect(e);
          }
        })
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.submit(flow).get();

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

    final ListStorage<Integer> data;

    SortState(
        Context<Integer> c,
        StorageProvider storageProvider) {
      
      super(c, storageProvider);
      data = storageProvider.getListStorage(
          ListStorageDescriptor.of("data", Integer.class));
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      List<Integer> toSort = Lists.newArrayList(data.get());
      Collections.sort(toSort);
      for (Integer i : toSort) {
        getContext().collect(i);
      }
    }

    static SortState combine(Iterable<SortState> others) {
      SortState ret = null;
      for (SortState s : others) {
        if (ret == null) {
          ret = new SortState(
              s.getContext(),
              s.getStorageProvider());
        }
        ret.data.addAll(s.data.get());
      }
      return ret;
    }

    @Override
    public void close() {
      data.clear();
    }
  } // ~ end of SortState

  static class SizedCountWindow extends Window {
    final int size;

    int get() {
      return size;
    }

    SizedCountWindow(int size) {
      this.size = size;
    }

    @Override
    public String toString() {
      return String.valueOf(size);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SizedCountWindow) {
        SizedCountWindow that = (SizedCountWindow) o;
        return size == that.size;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return size;
    }
  } // ~ end of SizedCountWindow

  static class SizedCountWindowing<T>
      implements Windowing<T, SizedCountWindow> {

    final UnaryFunction<T, Integer> size;

    SizedCountWindowing(UnaryFunction<T, Integer> size) {
      this.size = size;
    }

    @Override
    public Set<SizedCountWindow> assignWindowsToElement(WindowedElement<?, T> input) {
      int sz = size.apply(input.getElement());
      return Sets.newHashSet(new SizedCountWindow(sz), new SizedCountWindow(2 * sz));
    }

    @Override
    public Trigger<SizedCountWindow> getTrigger() {
      return new SizedCountTrigger();
    }
  } // ~ end of SizedCountWindowing

  static class SizedCountTrigger implements Trigger<SizedCountWindow> {
    private final ValueStorageDescriptor<Long> countDesc =
        ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y );

    @Override
    public TriggerResult onElement(long time, SizedCountWindow window, TriggerContext ctx) {
      ValueStorage<Long> cnt = ctx.getValueStorage(countDesc);
      cnt.set(cnt.get() + 1L);
      if (cnt.get() >= window.get()) {
        return TriggerResult.FLUSH_AND_PURGE;
      }
      return TriggerResult.NOOP;
    }

    @Override
    public TriggerResult onTimer(long time, SizedCountWindow window,
                                 TriggerContext ctx) {
      return TriggerResult.NOOP;
    }

    @Override
    public void onClear(SizedCountWindow window, TriggerContext ctx) {
      ctx.getValueStorage(countDesc).clear();
    }

    @Override
    public TriggerResult onMerge(SizedCountWindow window, TriggerContext.TriggerMergeContext ctx) {
      ctx.mergeStoredState(countDesc);
      return TriggerResult.NOOP;
    }
  } // ~ end of SizedCountTrigger

  @Test
  public void testReduceByKeyWithSortStateAndCustomWindowing() throws InterruptedException, ExecutionException {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            reversed(sequenceInts(0, 100)),
            reversed(sequenceInts(100, 1100))));

    SizedCountWindowing<Integer> windowing =
        new SizedCountWindowing<>(i -> (i % 10) + 1);

    // the key for sort will be the last digit
    Dataset<Pair<Integer, Integer>> output =
        ReduceStateByKey.of(ints)
        .keyBy(i -> i % 10)
        .valueBy(e -> e)
        .stateFactory(SortState::new)
        .combineStateBy(SortState::combine)
        .windowBy(windowing)
        .output();

    // collector of outputs
    ListDataSink<Triple<SizedCountWindow, Integer, Integer>> outputSink = ListDataSink.get(2);

    FlatMap.of(output)
        .using((UnaryFunctor<Pair<Integer, Integer>, Triple<SizedCountWindow, Integer, Integer>>)
            (elem, context) -> context.collect(Triple.of((SizedCountWindow) context.getWindow(), elem.getFirst(), elem.getSecond())))
        .output()
        .persist(outputSink);

    executor.submit(flow).get();

    List<List<Triple<SizedCountWindow, Integer, Integer>>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());

    // each partition should have 550 items in each window set
    assertEquals(2 * 550, outputs.get(0).size());
    assertEquals(2 * 550, outputs.get(1).size());

    Set<Integer> firstKeys = outputs.get(0).stream()
        .map(Triple::getSecond).distinct()
        .collect(Collectors.toSet());

    // validate that the two partitions contain different keys
    outputs.get(1).forEach(p -> assertFalse(firstKeys.contains(p.getSecond())));

    checkKeyAlignedSortedList(outputs.get(0));
    checkKeyAlignedSortedList(outputs.get(1));
  }

  private void checkKeyAlignedSortedList(
      List<Triple<SizedCountWindow, Integer, Integer>> list) {

    Map<SizedCountWindow, Map<Integer, List<Integer>>> byWindow = new HashMap<>();

    for (Triple<SizedCountWindow, Integer, Integer> p : list) {
      Map<Integer, List<Integer>> byKey = byWindow.get(p.getFirst());
      if (byKey == null) {
        byWindow.put(p.getFirst(), byKey = new HashMap<>());
      }
      List<Integer> sorted = byKey.get(p.getSecond());
      if (sorted == null) {
        byKey.put(p.getSecond(), sorted = new ArrayList<>());
      }
      sorted.add(p.getThird());
    }

    assertFalse(byWindow.isEmpty());
    int totalCount = 0;
    List<SizedCountWindow> iterOrder =
        byWindow.keySet()
            .stream()
            .sorted(Comparator.comparing(SizedCountWindow::get))
            .collect(Collectors.toList());
    for (SizedCountWindow w : iterOrder) {
      Map<Integer, List<Integer>> wkeys = byWindow.get(w);
      assertNotNull(wkeys);
      assertFalse(wkeys.isEmpty());
      for (Map.Entry<Integer, List<Integer>> e : wkeys.entrySet()) {
        // now, each list must be sorted
        assertAscendingWindows(e.getValue(), w, e.getKey());
        totalCount += e.getValue().size();
      }
    }
    assertEquals(1100, totalCount);
  }

  private static void assertAscendingWindows(
      List<Integer> xs, SizedCountWindow window, Integer key) {
    List<List<Integer>> windows = Lists.partition(xs, window.get());
    assertFalse(windows.isEmpty());
    int totalSeen = 0;
    for (List<Integer> windowData : windows) {
      int last = -1;
      for (int x : windowData) {
        if (last > x) {
          fail(String.format("Sequence not ascending for (window: %s / key: %d): %s",
              window, key, xs));
        }
        last = x;
        totalSeen += 1;
      }
    }
    assertEquals(xs.size(), totalSeen);
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
  public void testInputMultiConsumption() throws InterruptedException, ExecutionException {
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

    executor.submit(flow).get();

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
        ListDataSource.unbounded(sequenceInts(0, N)));

    ListDataSink<Long> outputs = ListDataSink.get(2);

    ReduceWindow.of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)), e -> e * 1000L)
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler<>(100));
    
    // run the executor in separate thread in order to be able to watch
    // the partial results
    CompletableFuture<Executor.Result> future = executor.submit(flow);

    // sleep for one second
    Thread.sleep(1000L);

    // the data in first unfinished partition
    List<Long> output = new ArrayList<>(outputs.getUncommittedOutputs().get(0));


    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w != 10).count() <= 1);

    future.get();

    output = outputs.getOutputs().get(0);

    output.forEach(w -> assertEquals("Each window should have 10 elements, got "
        + w, 10L, (long) w));

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

    ListDataSink<Long> outputs = ListDataSink.get(2);

    ReduceWindow.of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)), e -> e * 1000L)
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));

    executor.submit(flow).get();

    // there should be only one element on output - the first element
    // all other windows are discarded
    List<Long> output = outputs.getOutputs().get(0);
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
    Dataset<Set<Integer>> reduced = ReduceWindow.of(input)
        .reduceBy((Iterable<Integer> values) -> {
          Set<Integer> grp = new TreeSet<>();
          for (Integer i : values) {
            grp.add(i);
          }
          return grp;
        })
        .windowBy(Batch.get())
        .output();

    // explode it back to the original input (maybe reordered)
    // and store it as the original input, process it further in
    // the same way as in `testWithWatermarkAndEventTime'
    input = FlatMap.of(reduced)
        .using((Set<Integer> grp, Context<Integer> c) -> {
          for (Integer i : grp) {
            c.collect(i);
          }
        }).output();

    ListDataSink<Long> outputs = ListDataSink.get(1);

    ReduceWindow.of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)), e -> e * 1000L)
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));
    
    executor.submit(flow).get();

    // the data in first unfinished partition
    List<Long> output = new ArrayList<>(outputs.getUncommittedOutputs().get(0));


    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w != 10).count() <= 1);

    output = outputs.getOutputs().get(0);

    output.forEach(w -> assertEquals("Each window should have 10 elements, got "
        + w, 10L, (long) w));

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
    executor.submit(flow).get();

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

  @Test
  public void testWatermarkSchedulerWithLatecomers() throws InterruptedException, ExecutionException {
    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(reversed(sequenceInts(0, N))));

    ListDataSink<Integer> outputs = ListDataSink.get(1);

    ReduceWindow.of(input)
        .valueBy(e -> e)
        .combineBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofSeconds(1)), e -> e * 1000L)
        .setNumPartitions(1)
        .output()
        .persist(outputs);

    // watermarking 4000 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(4000));

    executor.submit(flow).get();

    // there should be five windows at the output
    // (1999, 1998, 1997, 1996 and 1995)
    // all other windows are discarded
    List<Integer> output = outputs.getOutputs().get(0);
    assertUnorderedEquals(Arrays.asList(1999, 1998, 1997, 1996, 1995), output);

  }

}
