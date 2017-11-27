/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.executor.local;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * {@code LocalExecutor} test suite.
 * The {@code LocalExecutor} stands on the basic operators, so we just
 * need to test it correctly implements all of them. Next we need to test
 * that it can process complex flows with many partitions.
 */
public class LocalExecutorTest {

  LocalExecutor executor;
  Flow flow;

  @Before
  public void setup() {
    executor = new LocalExecutor();
    flow = Flow.create("Test");
  }

  @After
  public void teardown() {
    executor.abort();
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

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get();

    Union.of(first, second)
        .output()
        .persist(outputSink);

    executor.submit(flow).get();

    List<Integer> outputs = outputSink.getOutputs();
    DatasetAssert.unorderedEquals(
        outputs,
        1, 2, 3, 4, 5, 6, 7, 8, 9);
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
        .using((Integer e, Collector<Integer> c) -> {
          for (int i = 0; i < e; i++) {
            c.collect(e);
          }
        })
        .output();

    // collector of outputs
    ListDataSink<Integer> outputSink = ListDataSink.get();

    output.persist(outputSink);

    executor.submit(flow).get();

    List<Integer> outputs = outputSink.getOutputs();
    DatasetAssert.unorderedEquals(
        outputs,
        1, 2, 2, 3, 3, 3,
        4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6);
  }

  // ReduceStateByKey operator

  /**
   * Simple sort state for tests.
   * This state takes comparable elements and produces sorted sequence.
   */
  public static class SortState implements State<Integer, Integer> {

    final ListStorage<Integer> data;

    SortState(StateContext context, Collector<Integer> c) {
      data = context.getStorageProvider().getListStorage(
          ListStorageDescriptor.of("data", Integer.class));
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush(Collector<Integer> context) {
      StreamSupport.stream(data.get().spliterator(), false)
          .sorted()
          .forEach(context::collect);
    }

    static void combine(SortState target, Iterable<SortState> others) {
      for (SortState other : others) {
        target.data.addAll(other.data.get());
      }
    }

    @Override
    public void close() {
      data.clear();
    }
  } // ~ end of SortState

  static class SizedCountWindow extends Window<SizedCountWindow> {
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

    @Override
    public int compareTo(SizedCountWindow o) {
      return Integer.compare(size, o.size);
    }
  } // ~ end of SizedCountWindow

  // windowing that assigns elements to exactly two windows
  // with label N and 2*N, where N is defined by {@code sizeFn}
  // each window is triggered when reaches count of elements
  // equal to its label
  static class SizedCountWindowing<T>
      implements Windowing<T, SizedCountWindow> {

    final UnaryFunction<T, Integer> sizeFn;

    SizedCountWindowing(UnaryFunction<T, Integer> size) {
      this.sizeFn = size;
    }

    @Override
    public Iterable<SizedCountWindow> assignWindowsToElement(WindowedElement<?, T> input) {
      int sz = sizeFn.apply(input.getElement());
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
    public void onMerge(SizedCountWindow window, TriggerContext.TriggerMergeContext ctx) {
      ctx.mergeStoredState(countDesc);
    }
  } // ~ end of SizedCountTrigger

  @Test
  public void testReduceByKeyWithSortStateAndCustomWindowing()
      throws InterruptedException, ExecutionException {

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
        .mergeStatesBy(SortState::combine)
        .windowBy(windowing)
        .output();

    // collector of outputs
    ListDataSink<Triple<SizedCountWindow, Integer, Integer>> sink = ListDataSink.get();

    FlatMap.of(output)
        .using((UnaryFunctor<Pair<Integer, Integer>, Triple<SizedCountWindow, Integer, Integer>>)
            (elem, context) -> context.collect(Triple.of((SizedCountWindow) context.getWindow(), elem.getFirst(), elem.getSecond())))
        .output()
        .persist(sink);

    executor.submit(flow).get();

    List<Triple<SizedCountWindow, Integer, Integer>> outputs = sink.getOutputs();

    assertEquals(4 * 550, outputs.size());

    checkKeyAlignedSortedList(outputs);
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


  @Test(timeout = 5000L)
  public void testInputMultiConsumption() throws InterruptedException, ExecutionException {
    final int N = 1000;
    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(sequenceInts(0, N)));

    // there seems to be bug in LocalExecutor
    // that makes it impossible to consume the
    // same dataset twice by single union operator
    Dataset<Integer> first = MapElements.of(input)
        .using(e -> e)
        .output();

    Dataset<Integer> second = MapElements.of(input)
        .using(e -> e)
        .output();

    // ~ consume the input another time
    Dataset<Integer> union = Union.of(first, second)
        .output();

    Dataset<Pair<Integer, Integer>> sum = ReduceByKey
        .of(union)
        .keyBy(e -> 0)
        .valueBy(e -> e)
        .reduceBy(Sums.ofInts())
        .output();

    ListDataSink<Pair<Integer, Integer>> sumOut = ListDataSink.get();
    sum.persist(sumOut);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        sumOut.getOutputs(),
        Pair.of(0, 2 * (N - 1) * N / 2));
  }


  @Test
  public void testWithWatermarkAndEventTime() throws Exception {

    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(sequenceInts(0, N)));

    ListDataSink<Long> outputs = ListDataSink.get();

    input = AssignEventTime.of(input).using(e -> e * 1000L).output();
    ReduceWindow.of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)))
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
    List<Long> output = outputs.getUncommittedOutputs();

    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w != 10).count() <= 1);

    future.get();

    output = outputs.getOutputs();

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

    ListDataSink<Long> outputs = ListDataSink.get();

    input = AssignEventTime.of(input).using(e -> e * 1000L).output();
    ReduceWindow.of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)))
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));

    executor.submit(flow).get();

    // there should be only one element on output - the first element
    // all other windows are discarded
    List<Long> output = outputs.getOutputs();
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
        .reduceBy(s -> s.collect(Collectors.toSet()))
        .windowBy(GlobalWindowing.get())
        .output();

    // explode it back to the original input (maybe reordered)
    // and store it as the original input, process it further in
    // the same way as in `testWithWatermarkAndEventTime'
    input = FlatMap.of(reduced)
        .using((Set<Integer> grp, Collector<Integer> c) -> {
          for (Integer i : grp) {
            c.collect(i);
          }
        }).output();

    ListDataSink<Long> outputs = ListDataSink.get();

    input = AssignEventTime.of(input).using(e -> e * 1000L).output();
    ReduceWindow.named("foo").of(input)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)))
        .output()
        .persist(outputs);

    // watermarking 100 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(100));

    executor.submit(flow).get();

    // the data in first unfinished partition
    List<Long> output = outputs.getUncommittedOutputs();


    // after one second we should have something about 500 elements read,
    // this means we should have at least 40 complete windows
    assertTrue("Should have at least 40 windows, got "
        + output.size(), 40 <= output.size());
    assertTrue("All but (at most) one window should have size 10",
        output.stream().filter(w -> w != 10).count() <= 1);

    output = outputs.getOutputs();

    output.forEach(w -> assertEquals("Each window should have 10 elements, got "
        + w, 10L, (long) w));

    // we have 2000 elements split into 200 windows
    assertEquals(200, output.size());

  }

  @Test
  public void testWatermarkSchedulerWithLatecomers() throws InterruptedException, ExecutionException {
    int N = 2000;

    // generate some small ints, use them as event time and count them
    // in 10s windows

    Dataset<Integer> input = flow.createInput(
        ListDataSource.unbounded(reversed(sequenceInts(0, N))));

    ListDataSink<Integer> outputs = ListDataSink.get();

    input = AssignEventTime.of(input).using(e -> e * 1000L).output();
    ReduceWindow.of(input)
        .valueBy(e -> e)
        .combineBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output()
        .persist(outputs);

    // watermarking 4000 ms
    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(4000));

    executor.submit(flow).get();

    // there should be five windows at the output
    // (1999, 1998, 1997, 1996 and 1995)
    // all other windows are discarded
    List<Integer> output = outputs.getOutputs();
    DatasetAssert.unorderedEquals(
        output,
        1999, 1998, 1997, 1996, 1995);

  }
}
