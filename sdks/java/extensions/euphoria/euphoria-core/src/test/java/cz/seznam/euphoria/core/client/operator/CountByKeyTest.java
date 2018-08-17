package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class CountByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .output();

    assertEquals(flow, counted.getFlow());
    assertEquals(1, flow.size());

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertEquals(flow, count.getFlow());
    assertEquals("CountByKey1", count.getName());
    assertNotNull(count.keyExtractor);
    assertEquals(counted, count.output());
    // batch windowing by default
    assertEquals(BatchWindowing.get(), count.getWindowing());

    // default partitioning used
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(3, count.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = CountByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertEquals("CountByKey", count.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(Windowing.Time.hours(1))
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getWindowing() instanceof Windowing.Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, count.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, count.getPartitioning().getNumPartitions());
  }
}