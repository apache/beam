
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        .keyBy(i -> (int) i % 10)
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

    int sublistIndex = 0;
    for (int i = 0; i < 6; i++) {
      // sublists are of length 100, 200, 300, 400, 500, 550
      int start = sublistIndex;
      int sublistLength = (i + 1) * 100 - (i == 5 ? 50 : 0);
      final List<List<Pair<Integer, Integer>>> sublists;
      sublists = outputs.stream()
          .map(l -> l.subList(start, start + sublistLength))
          .collect(Collectors.toList());
      sublists.forEach(InMemExecutorTest::checkSorted);
      sublistIndex += sublistLength;
    }

  }


  private static void checkSorted(List<Pair<Integer, Integer>> collection) {
    Pair<Integer, Integer> last = null;
    for (Pair<Integer, Integer> elem : collection) {
      if (last != null && (int) last.getFirst() != (int) elem.getFirst()) {
        last = null;
      }
      if (last != null) {
        assertTrue("Element " + last + " should be less than " + elem,
            last.getSecond() < elem.getSecond());
      }
      last = elem;
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

}
