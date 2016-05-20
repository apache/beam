package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistinctTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq =
        Distinct.named("Distinct1")
            .of(dataset)
            .output();

    assertEquals(flow, uniq.getFlow());
    assertEquals(1, flow.size());

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertEquals(flow, distinct.getFlow());
    assertEquals("Distinct1", distinct.getName());
    assertEquals(uniq, distinct.output());
    // batch windowing by default
    assertEquals(BatchWindowing.get(), distinct.getWindowing());

    // default partitioning used
    assertTrue(distinct.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(3, distinct.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset).output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertEquals("Distinct", distinct.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset)
            .windowBy(Windowing.Time.hours(1))
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(distinct.getWindowing() instanceof Windowing.Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset)
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(distinct.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, distinct.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(distinct.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, distinct.getPartitioning().getNumPartitions());
  }
}