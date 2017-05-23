/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
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
    assertNull(join.getWindowing());
    assertFalse(join.outer);

    // default partitioning used
    assertTrue(join.getPartitioning().hasDefaultPartitioner());
    assertEquals(3, join.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined = Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> {
              c.getCounter("my-counter").increment();
              c.collect(l + r);
            })
            .output();

    assertEquals(flow, joined.getFlow());
    assertEquals(1, flow.size());

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(flow, join.getFlow());
    assertEquals("Join1", join.getName());
    assertNotNull(join.leftKeyExtractor);
    assertNotNull(join.rightKeyExtractor);
    assertEquals(joined, join.output());
    assertNull(join.getWindowing());
    assertFalse(join.outer);

    // default partitioning used
    assertTrue(join.getPartitioning().hasDefaultPartitioner());
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
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getWindowing() instanceof Time);
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
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(!join.getPartitioning().hasDefaultPartitioner());
    assertTrue(join.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, join.getPartitioning().getNumPartitions());
    assertTrue(join.getWindowing() instanceof Time);
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
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(!join.getPartitioning().hasDefaultPartitioner());
    assertTrue(join.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, join.getPartitioning().getNumPartitions());
    assertTrue(join.getWindowing() instanceof Time);
  }
}