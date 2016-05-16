
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Map;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import org.junit.Ignore;

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
        .partitionBy(e -> e % 2)
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
        .partitionBy(e -> e % 3)
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
        .partitionBy(e -> e % 2)
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
        .by((Integer e, Collector<Integer> c) -> {
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
  public static class SortState extends State<Integer, Pair<Integer, Integer>> {

    List<Integer> data = new ArrayList<>();

    final Integer key;

    SortState(Integer key, Collector<Pair<Integer, Integer>> c) {
      super(c);
      this.key = key;
    }

    @Override
    public void add(Integer element) {
      data.add(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      Collections.sort((List) data);
      data.stream().forEachOrdered(
          c -> this.collector.collect(Pair.of(key, c)));
    }

    static SortState combine(Iterable<SortState> others) {
      SortState ret = null;
      for (SortState s : others) {
        if (ret == null) {
          ret = new SortState(s.key, s.collector);
        }
        ret.data.addAll(s.data);
      }
      return ret;
    }

  }

  @Test
  public void testReduceByKeyWithSortStateAndAggregatingWindow() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            reversed(sequenceInts(0, 100)),
            reversed(sequenceInts(100, 1100))));

    // the key for sort will be the last digit
    Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(ints)
        .keyBy(i -> i % 10)
        .valueBy(e -> e)
        .stateFactory(SortState::new)
        .combineStateBy(SortState::combine)
        .windowBy(Windowing.Count.of(100).aggregating())
        .output();

    // collector of outputs
    ListDataSink<Pair<Integer, Integer>> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Pair<Integer, Integer>>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());

    // each partition should have (100 + 200 + 300 + 400 + 500 + 550) = 2050 items
    assertEquals(2050, outputs.get(0).size());
    assertEquals(2050, outputs.get(1).size());

    Set<Integer> firstKeys = outputs.get(0).stream()
        .map(Pair::getFirst).distinct()
        .collect(Collectors.toSet());

    outputs.get(1).forEach(p -> assertFalse(firstKeys.contains(p.getFirst())));

    outputs.stream().forEach(
        p -> checkSortedSublists(p, 100, 200, 300, 400, 500, 550));

  }


  @Test
  public void testReduceByKeyWithSortStateAndNonAggregatingWindow() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            reversed(sequenceInts(0, 100)),
            reversed(sequenceInts(100, 1100))));

    // the key for sort will be the last digit
    Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(ints)
        .keyBy(i -> i % 10)
        .valueBy(e -> e)
        .stateFactory(SortState::new)
        .combineStateBy(SortState::combine)
        .windowBy(Windowing.Count.of(100))
        .output();

    // collector of outputs
    ListDataSink<Pair<Integer, Integer>> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Pair<Integer, Integer>>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());

    // each partition should have (100 + 100 + 100 + 100 + 100 + 50) = 550 items
    assertEquals(550, outputs.get(0).size());
    assertEquals(550, outputs.get(1).size());

    Set<Integer> firstKeys = outputs.get(0).stream()
        .map(Pair::getFirst).distinct()
        .collect(Collectors.toSet());

    outputs.get(1).forEach(p -> assertFalse(firstKeys.contains(p.getFirst())));

    outputs.stream().forEach(
        p -> checkSortedSublists(p, 100, 100, 100, 100, 100, 50));


  }

  private void checkSortedSublists(
      List<Pair<Integer, Integer>> list, int... lengths) {

    int sublistIndex = 0;
    for (int sublistLength : lengths) {
      // sublists are of length 100, 100, 100, 100, 100, 50
      int start = sublistIndex;
      final List<List<Pair<Integer, Integer>>> sublists;
      checkSorted(list.subList(start, start + sublistLength));
      sublistIndex += sublistLength;
    }

  }


  private static class CountWindow<GROUP> implements Window<GROUP, Integer> {

    final GROUP group;
    final int maxSize;
    
    int size = 1;

    public CountWindow(GROUP group, int maxSize) {
      this.group = group;
      this.maxSize = maxSize;
    }

    @Override
    public GROUP getGroup() {
      return group;
    }

    @Override
    public Integer getLabel() {
      return System.identityHashCode(this);
    }

    @Override
    public void registerTrigger(
        Triggering triggering,
        UnaryFunction<Window<?, ?>, Void> evict) {
      // nop
    }

    @Override
    public String toString() {
      return "CountWindow(" + group + ", " + size + ", " + maxSize + ")";
    }


  }


  static class UnalignedCountWindowing<T, GROUP> implements
      MergingWindowing<T, GROUP, Integer, CountWindow<GROUP>> {

    final UnaryFunction<T, GROUP> groupExtractor;
    final UnaryFunction<GROUP, Integer> size;

    UnalignedCountWindowing(
        UnaryFunction<T, GROUP> groupExtractor,
        UnaryFunction<GROUP, Integer> size) {
      this.groupExtractor = groupExtractor;
      this.size = size;
    }

    @Override
    public Collection<Pair<Collection<CountWindow<GROUP>>, CountWindow<GROUP>>> mergeWindows(
        Collection<CountWindow<GROUP>> actives) {

      // we will merge together only windows with the same window size

      List<Pair<Collection<CountWindow<GROUP>>, CountWindow<GROUP>>> ret = new ArrayList<>();
      Map<Integer, List<CountWindow<GROUP>>> toMergeMap = new HashMap<>();
      Map<Integer, AtomicInteger> currentSizeMap = new HashMap<>();

      for (CountWindow<GROUP> w : actives) {
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
          List<CountWindow<GROUP>> toMerge = toMergeMap.get(wSize);
          if (!toMerge.isEmpty()) {
            CountWindow<GROUP> res = new CountWindow<>(w.group, currentSize.get());
            res.size = currentSize.get();            
            ret.add(Pair.of(new ArrayList<>(toMerge), res));
            toMerge.clear();
          }
          toMerge.add(w);
          currentSize.set(w.size);
        }
      }

      for (List<CountWindow<GROUP>> toMerge : toMergeMap.values()) {
        if (!toMerge.isEmpty()) {
          CountWindow<GROUP> first = toMerge.get(0);
          CountWindow<GROUP> res = new CountWindow<>(first.group, first.maxSize);
          res.size = currentSizeMap.get(first.maxSize).get();
          ret.add(Pair.of(toMerge, res));
        }
      }
      return ret;
    }

    @Override
    public Set<CountWindow<GROUP>> assignWindows(T input) {
      GROUP g = groupExtractor.apply(input);
      int sizeForGroup = size.apply(g);
      return new HashSet<>(Arrays.asList(
          new CountWindow<>(g, sizeForGroup),
          new CountWindow<>(g, 2 * sizeForGroup)));
    }

    
    @Override
    public boolean isComplete(CountWindow<GROUP> window) {
      return window.size == window.maxSize;
    }


  }


  @Test
  // FIXME: fix this test as soon as we have window label in output
  // from ReduceStateByKey!
  @Ignore
  public void testReduceByKeyWithSortStateAndUnalignedWindow() {
    Dataset<Integer> ints = flow.createInput(
        ListDataSource.unbounded(
            reversed(sequenceInts(0, 100)),
            reversed(sequenceInts(100, 1100))));

    UnalignedCountWindowing<Integer, Integer> windowing = new UnalignedCountWindowing<>(
        i -> i % 10, i -> i + 1);

    // the key for sort will be the last digit
    Dataset<Pair<Integer, Integer>> output = ReduceStateByKey.of(ints)
        .keyBy(i -> i % 10)
        .valueBy(e -> e)
        .stateFactory(SortState::new)
        .combineStateBy(SortState::combine)
        .windowBy(windowing)
        .output();

    // collector of outputs
    ListDataSink<Pair<Integer, Integer>> outputSink = ListDataSink.get(2);

    output.persist(outputSink);

    executor.waitForCompletion(flow);

    List<List<Pair<Integer, Integer>>> outputs = outputSink.getOutputs();
    assertEquals(2, outputs.size());

    // each partition should have 550 items in each window set
    assertEquals(2 * 550, outputs.get(0).size());
    assertEquals(2 * 550, outputs.get(1).size());

    Set<Integer> firstKeys = outputs.get(0).stream()
        .map(Pair::getFirst).distinct()
        .collect(Collectors.toSet());

    outputs.get(1).forEach(p -> assertFalse(firstKeys.contains(p.getFirst())));

    // each partition is now constructed so that there is exactly (N + 1)
    // sorted elements in a row, N is the element key
    // then key *might* get switched

    outputs.forEach(this::checkKeyAlignedSortedList);

  }


  private static void checkSorted(List<Pair<Integer, Integer>> collection) {
    Pair<Integer, Integer> last = null;
    Set<Integer> passedKeys = new HashSet<>();
    for (Pair<Integer, Integer> elem : collection) {
      if (last != null && (int) last.getFirst() != (int) elem.getFirst()) {
        assertTrue("Each key should be in the sequence in single continuous block. Key "
            + last.getFirst() + " was seen multiple times. The sequence is not properly sorted",
            passedKeys.add(last.getFirst()));
        last = null;
      }
      if (last != null) {
        assertTrue("Element " + last + " should be less than " + elem,
            last.getSecond() < elem.getSecond());
      }
      last = elem;
    }

  }


  private void checkKeyAlignedSortedList(List<Pair<Integer, Integer>> list) {

    Integer lastKey = null;
    int lastValue = -1;
    int sortedInRow = 0;
    Map<Integer, List<Integer>> sortedSequences = new HashMap<>();

    for (Pair<Integer, Integer> p : list) {
      if (lastKey != null) {
        if (lastKey != (int) p.getFirst() || lastValue >= p.getValue()) {
          List<Integer> sorted = sortedSequences.get(lastKey);
          if (sorted == null) {
            sortedSequences.put(lastKey, (sorted = new ArrayList<>()));
          }
          sorted.add(sortedInRow);
          sortedInRow = 0;
          lastValue = -1;
        }
      }
      lastKey = p.getFirst();
      lastValue = p.getSecond();
      sortedInRow++;
    }

    for (Map.Entry<Integer, List<Integer>> e : sortedSequences.entrySet()) {
      // now, in correctly constructed sequence the following holds:
      // a) all but the last two elements are either (N + 1) or 2 * (N + 1)
      // b) the last two elements are equal to
      // (110 - sum(all elements that are equal to (N + 1),
      //  110 - sum(all elements that are equal to 2 * (N + 1))
      // irrespective to ordering

      int listSize = e.getValue().size();
      assertTrue(listSize > 2);

      Set<Integer> rest = new HashSet<>(Arrays.asList(
          e.getValue().get(listSize - 1),
          e.getValue().get(listSize - 2)));
      
      List<Integer> core = e.getValue().subList(0, e.getValue().size() - 2);
      int key = e.getKey();
      int total = core.size();
      int doubles = 0;
      for (Integer i : core) {
        if (i == 2 * (key + 1)) {
          doubles ++;
        } else {
          assertEquals("The elements in core sequence must be either "
              + (key + 1) + " or " + (2 * (key  + 1)) + " got " + i,
              (int) i, (int) (key + 1));
        }
      }
      assertEquals("Key " + key + " should have " + 55 / (key + 1)
          + " sequences of double size",
          55 / (key + 1), doubles);
      assertEquals("Key " + key + " should have " + 110 / (key + 1)
          + " sequences of normal size",
          110 / (key + 1), total - doubles);
    }

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
    Dataset<Integer> map = Map
        .of(input)
        .by(e -> e)
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
}
