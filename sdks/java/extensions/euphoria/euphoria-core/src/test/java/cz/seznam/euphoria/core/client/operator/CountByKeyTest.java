
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class CountByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> counted = CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(windowing)
            .output();

    assertEquals(flow, counted.getFlow());
    assertEquals(1, flow.size());

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertEquals(flow, count.getFlow());
    assertEquals("CountByKey1", count.getName());
    assertNotNull(count.keyExtractor);
    assertEquals(counted, count.output());
    assertSame(windowing, count.getWindowing());
    assertNull(count.getEventTimeAssigner());

    // default partitioning used
    assertTrue(count.getPartitioning().hasDefaultPartitioner());
    assertEquals(3, count.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .setPartitioner((String element) -> element.hashCode() + 1)
            .windowBy(windowing)
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();

    int hash = "test".hashCode() + 1;
    assertEquals(hash, count.getPartitioning().getPartitioner().getPartition("test"));
    assertEquals(3, count.getPartitioning().getNumPartitions());
  }


  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    CountByKey.of(dataset)
            .keyBy(s -> s)
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertEquals("CountByKey", count.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(Time.of(Duration.ofHours(1)), (s -> 0L))
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getWindowing() instanceof Time);
    assertNotNull(count.getEventTimeAssigner());
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getWindowing() instanceof Time);
    assertTrue(!count.getPartitioning().hasDefaultPartitioner());
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, count.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner2() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    CountByKey count = (CountByKey) flow.operators().iterator().next();
    assertTrue(count.getWindowing() instanceof Time);
    assertTrue(!count.getPartitioning().hasDefaultPartitioner());
    assertTrue(count.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, count.getPartitioning().getNumPartitions());
  }
}