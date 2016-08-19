package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

public class ReduceByKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> reduced = ReduceByKey.named("ReduceByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.valueExtractor);
    assertNotNull(reduce.reducer);
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

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals("ReduceByKey", reduce.getName());
  }

  @Test
  public void testBuild_ReduceBy() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.reducer);
  }


  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .setPartitioning(new HashPartitioning<>(1))
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
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

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<CompositeKey<String, String>, Long>> reduced =
            ReduceByKey.named("ReduceByKey1")
                    .of(grouped)
                    .keyBy(s -> s)
                    .valueBy(s -> 1L)
                    .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
                    .windowBy(windowing)
                    .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(2, flow.size());

    ReduceByKey reduce = (ReduceByKey) ((List)flow.operators()).get(1);
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.valueExtractor);
    assertEquals(reduced, reduce.output());
    assertEquals(windowing, reduce.getWindowing());

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

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced =
            ReduceByKey.of(grouped)
                    .keyBy(s -> s)
                    .valueBy(s -> 1L)
                    .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
                    .output();

    ReduceByKey reduce = (ReduceByKey) ((List)flow.operators()).get(1);
    assertEquals("ReduceByKey", reduce.getName());
  }

  @Test
  public void testBuild_Grouped_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced =
            ReduceByKey.named("ReduceByKey1")
                    .of(grouped)
                    .keyBy(s -> s)
                    .valueBy(s -> 1L)
                    .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

    ReduceByKey reduce = (ReduceByKey) ((List)flow.operators()).get(1);
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Grouped_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    GroupedDataset<String, String> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced =
            ReduceByKey.named("ReduceByKey1")
                    .of(grouped)
                    .keyBy(s -> s)
                    .valueBy(s -> 1L)
                    .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
                    .setPartitioning(new HashPartitioning<>(1))
                    .output();

    ReduceByKey reduce = (ReduceByKey) ((List)flow.operators()).get(1);
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

    Dataset<Pair<CompositeKey<String, String>, Long>> reduced =
            ReduceByKey.named("ReduceByKey1")
                    .of(grouped)
                    .keyBy(s -> s)
                    .valueBy(s -> 1L)
                    .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
                    .setPartitioner(new HashPartitioner<>())
                    .setNumPartitions(5)
                    .output();

    ReduceByKey reduce = (ReduceByKey) ((List)flow.operators()).get(1);
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, reduce.getPartitioning().getNumPartitions());
  }
}