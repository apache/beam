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
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SortTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<String> result = Sort.named("Sort1")
            .of(dataset)
            .by(s -> 1L)
            .windowBy(windowing)
            .setNumPartitions(1)
            .output();

    assertEquals(flow, result.getFlow());
    assertEquals(1, flow.size());

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertEquals(flow, tpk.getFlow());
    assertEquals("Sort1", tpk.getName());
    assertNotNull(tpk.getKeyExtractor());
    assertNotNull(tpk.getSortByExtractor());
    assertEquals(result, tpk.output());
    assertSame(windowing, tpk.getWindowing());
    assertNull(tpk.getEventTimeAssigner());

    assertTrue(!tpk.getPartitioning().hasDefaultPartitioner());
    assertTrue(tpk.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, tpk.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> result = Sort.of(dataset)
            .by(s -> 1L)
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertEquals("Sort", tpk.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> result = Sort.of(dataset)
            .by(s -> 1L)
            .windowBy(Time.of(Duration.ofHours(1)), (s -> 0L))
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertNotNull(tpk.getEventTimeAssigner());
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<String> result = Sort.named("Sort1")
            .of(dataset)
            .by(s -> 1L)
            .setPartitioning(new HashPartitioning<>(1))
            .windowBy(windowing)
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertTrue(!tpk.getPartitioning().hasDefaultPartitioner());
    assertTrue(tpk.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, tpk.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<String> result = Sort.named("Sort1")
            .of(dataset)
            .by(s -> 1L)
            .windowBy(windowing)
            .setPartitioner(e -> 0)
            .setNumPartitions(5)
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertTrue(!tpk.getPartitioning().hasDefaultPartitioner());
    assertTrue(tpk.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, tpk.getPartitioning().getNumPartitions());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testBuild_UnsupportedPartitioningImplicit() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<String> result = Sort.of(dataset)
            .by(s -> 1L)
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertEquals("Sort", tpk.getName());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testBuild_UnsupportedPartitioningExplicit() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> result = Sort.of(dataset)
            .by(s -> 1L)
            .setNumPartitions(2)
            .output();

    Sort tpk = (Sort) Iterables.getOnlyElement(flow.operators());
    assertEquals("Sort", tpk.getName());
  }
}