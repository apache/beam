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
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class TopPerKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Triple<String, Long, Long>> result = TopPerKey.named("TopPerKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(windowing)
            .output();

    assertEquals(flow, result.getFlow());
    assertEquals(1, flow.size());

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    assertEquals(flow, tpk.getFlow());
    assertEquals("TopPerKey1", tpk.getName());
    assertNotNull(tpk.getKeyExtractor());
    assertNotNull(tpk.getValueExtractor());
    assertNotNull(tpk.getScoreExtractor());
    assertEquals(result, tpk.output());
    assertSame(windowing, tpk.getWindowing());

    // default partitioning used
    assertTrue(tpk.getPartitioning().hasDefaultPartitioner());
    assertEquals(2, tpk.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Triple<String, Long, Long>> result = TopPerKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    assertEquals("TopPerKey", tpk.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Triple<String, Long, Long>> result = TopPerKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Triple<String, Long, Long>> result = TopPerKey.named("TopPerKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .setPartitioning(new HashPartitioning<>(1))
            .windowBy(windowing)
            .output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    assertTrue(!tpk.getPartitioning().hasDefaultPartitioner());
    assertTrue(tpk.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, tpk.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Triple<String, Long, Long>> result = TopPerKey.named("TopPerKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(windowing)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    assertTrue(!tpk.getPartitioning().hasDefaultPartitioner());
    assertTrue(tpk.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, tpk.getPartitioning().getNumPartitions());
  }
}