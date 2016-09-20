package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.StateStorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStateStorage;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.Assert.*;

public class ReduceStateByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.named("ReduceStateByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceStateByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertNotNull(reduce.getStateCombiner());
    assertNotNull(reduce.getStateFactory());
    assertEquals(reduced, reduce.output());
    assertSame(windowing, reduce.getWindowing());

    // default partitioning used
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(2, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertEquals("ReduceStateByKey", reduce.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, reduce.getPartitioning().getNumPartitions());
  }

  // test ReduceStateByKey with GroupedDataset as input


  @Test
  public void testBuild_Grouped() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Count<Object> windowing = Count.of(10);
    Dataset<Pair<CompositeKey<String, String>, Long>> reduced = ReduceStateByKey.named("ReduceStateByKey1")
            .of(grouped)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(2, flow.size());

    ReduceStateByKey reduce = (ReduceStateByKey) ((List)flow.operators()).get(1);
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceStateByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertNotNull(reduce.getStateCombiner());
    assertNotNull(reduce.getStateFactory());
    assertEquals(reduced, reduce.output());
    assertSame(windowing, reduce.getWindowing());

    // default partitioning used
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(2, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Grouped_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced = ReduceStateByKey.of(grouped)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) ((List)flow.operators()).get(1);
    assertEquals("ReduceStateByKey", reduce.getName());
  }

  @Test
  public void testBuild_Grouped_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced = ReduceStateByKey.of(grouped)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) ((List)flow.operators()).get(1);
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Grouped_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced = ReduceStateByKey.of(grouped)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) ((List)flow.operators()).get(1);
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Grouped_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced = ReduceStateByKey.of(grouped)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) ((List)flow.operators()).get(1);
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, reduce.getPartitioning().getNumPartitions());
  }


  /**
   * Simple aggregating state.
   */
  private static class WordCountState extends State<Long, Long> {

    private final ValueStateStorage<Long> sum;

    protected WordCountState(
        Operator<?, ?> operator,
        Collector<Long> collector,
        StateStorageProvider storageProvider) {
      super(operator, collector, storageProvider);
      sum = storageProvider.getValueStorage(this, Long.class);
    }

    @Override
    public void add(Long element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush() {
      this.getCollector().collect(sum.get());
    }

    static WordCountState combine(Iterable<WordCountState> others) {
      WordCountState state = null;
      for (WordCountState s : others) {
        if (state == null) {
          state = new WordCountState(
              s.getAssociatedOperator(),
              s.getCollector(),
              s.getStorageProvider());
        }
        state.add(s.sum.get());
      }

      return state;
    }

    @Override
    public void close() {
      sum.clear();
    }


  }
}
