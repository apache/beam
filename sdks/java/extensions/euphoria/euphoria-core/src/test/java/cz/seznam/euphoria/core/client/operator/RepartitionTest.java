package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import static org.junit.Assert.*;

public class RepartitionTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> partitioned = Repartition.named("Repartition1")
            .of(dataset)
            .setNumPartitions(5)
            .setPartitioner(new HashPartitioner<>())
            .output();

    assertEquals(flow, partitioned.getFlow());
    assertEquals(1, flow.size());

    Repartition repartition = (Repartition) flow.operators().iterator().next();
    assertEquals(flow, repartition.getFlow());
    assertEquals("Repartition1", repartition.getName());
    assertEquals(partitioned, repartition.output());

    // default partitioning used
    assertTrue(!repartition.getPartitioning().hasDefaultPartitioner());
    assertTrue(repartition.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, repartition.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> partitioned = Repartition.of(dataset)
            .setNumPartitions(5)
            .output();

    Repartition repartition = (Repartition) flow.operators().iterator().next();
    assertEquals("Repartition", repartition.getName());
  }
}