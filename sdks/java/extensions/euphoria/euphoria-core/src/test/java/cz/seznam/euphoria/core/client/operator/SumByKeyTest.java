
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class SumByKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = SumByKey.named("SumByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .output();

    assertEquals(flow, counted.getFlow());
    assertEquals(1, flow.size());

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertEquals(flow, sum.getFlow());
    assertEquals("SumByKey1", sum.getName());
    assertNotNull(sum.keyExtractor);
    assertEquals(counted, sum.output());
    // batch windowing by default
    assertEquals(Batch.get(), sum.getWindowing());

    // default partitioning used
    assertTrue(sum.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(3, sum.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = SumByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertEquals("SumByKey", sum.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = SumByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertTrue(sum.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = SumByKey.of(dataset)
            .keyBy(s -> s)
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertTrue(sum.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, sum.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted = SumByKey.of(dataset)
            .keyBy(s -> s)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    SumByKey count = (SumByKey) flow.operators().iterator().next();
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, count.getPartitioning().getNumPartitions());
  }
}