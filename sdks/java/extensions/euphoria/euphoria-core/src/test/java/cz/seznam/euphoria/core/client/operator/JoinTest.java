package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class JoinTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            //TODO It's sad the Collector type must be explicitly stated :-(
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .output();

    assertEquals(flow, joined.getFlow());
    assertEquals(1, flow.size());

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(flow, join.getFlow());
    assertEquals("Join1", join.getName());
    assertNotNull(join.leftKeyExtractor);
    assertNotNull(join.rightKeyExtractor);
    assertEquals(joined, join.output());
    // batch windowing by default
    assertEquals(BatchWindowing.get(), join.getWindowing());
    assertFalse(join.outer);

    // default partitioning used
    assertTrue(join.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(3, join.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Dataset<Pair<Integer, String>> joined = Join.of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals("Join", join.getName());
  }

  @Test
  public void testBuild_OuterJoin() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .outer()
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.outer);
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .windowBy(Windowing.Time.of(Duration.ofHours(1)))
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getWindowing() instanceof Windowing.Time);
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 3);
    Dataset<String> right = Util.createMockDataset(flow, 2);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, join.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 3);
    Dataset<String> right = Util.createMockDataset(flow, 2);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, join.getPartitioning().getNumPartitions());
  }
}