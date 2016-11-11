
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class DistinctTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<String> uniq =
        Distinct.named("Distinct1")
            .of(dataset)
            .windowBy(windowing)
            .output();

    assertEquals(flow, uniq.getFlow());
    assertEquals(1, flow.size());

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertEquals(flow, distinct.getFlow());
    assertEquals("Distinct1", distinct.getName());
    assertEquals(uniq, distinct.output());
    assertSame(windowing, distinct.getWindowing());

    // default partitioning used
    assertTrue(distinct.getPartitioning().hasDefaultPartitioner());
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
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(distinct.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset)
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(!distinct.getPartitioning().hasDefaultPartitioner());
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
    assertTrue(!distinct.getPartitioning().hasDefaultPartitioner());
    assertTrue(distinct.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, distinct.getPartitioning().getNumPartitions());
  }
}