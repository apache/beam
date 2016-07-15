package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class GroupByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> grouped = GroupByKey.named("GroupByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .output();

    assertEquals(flow, grouped.getFlow());
    assertEquals(1, flow.size());

    GroupByKey group = (GroupByKey) flow.operators().iterator().next();
    assertEquals(flow, group.getFlow());
    assertEquals("GroupByKey1", group.getName());
    assertNotNull(group.keyExtractor);
    assertNotNull(group.valueExtractor);
    assertEquals(grouped, group.output());

    // default partitioning used
    assertTrue(group.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(3, group.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .output();

    GroupByKey group = (GroupByKey) flow.operators().iterator().next();
    assertEquals("GroupByKey", group.getName());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBuild_NoValueBy() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, String>> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    GroupByKey group = (GroupByKey) flow.operators().iterator().next();
    assertNotNull(group.keyExtractor);
    assertNotNull(group.valueExtractor);
    // value extractor should be identity by default
    assertEquals("xxx", group.valueExtractor.apply("xxx"));
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .setPartitioning(new HashPartitioning<>(5))
            .valueBy(s -> 1L)
            .output();

    GroupByKey group = (GroupByKey) flow.operators().iterator().next();
    assertTrue(group.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, group.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> grouped = GroupByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    GroupByKey group = (GroupByKey) flow.operators().iterator().next();
    assertTrue(group.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, group.getPartitioning().getNumPartitions());
  }
}