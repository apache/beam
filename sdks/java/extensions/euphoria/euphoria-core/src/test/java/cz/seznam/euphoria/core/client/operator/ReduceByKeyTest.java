/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import org.junit.Test;

import java.time.Duration;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

public class ReduceByKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> reduced = ReduceByKey.named("ReduceByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.valueExtractor);
    assertNotNull(reduce.reducer);
    assertEquals(reduced, reduce.output());
    assertSame(windowing, reduce.getWindowing());
    assertNull(reduce.getEventTimeAssigner());

    // default partitioning used
    assertTrue(reduce.getPartitioning().hasDefaultPartitioner());
    assertEquals(2, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals("ReduceByKey", reduce.getName());
  }

  @Test
  public void testBuild_ReduceBy() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.reducer);
  }


  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(Time.of(Duration.ofHours(1)), (s -> 0L))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
    assertNotNull(reduce.getEventTimeAssigner());
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .setPartitioning(new HashPartitioning<>(1))
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(!reduce.getPartitioning().hasDefaultPartitioner());
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, reduce.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(!reduce.getPartitioning().hasDefaultPartitioner());
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, reduce.getPartitioning().getNumPartitions());
  }

  /**
   * Verify that the number of partitions of the reduce-by-key
   * operator's input is preserved in the output since no partitioning
   * is explicitly specified.
   */
  @Test
  public void testOutputNumPartitionsIsPreserved() {
    final int N_PARTITIONS = 78;

    Flow f = Flow.create();
    Dataset<Pair<String, Long>> input = Util.createMockDataset(f, N_PARTITIONS);
    assertEquals(N_PARTITIONS, input.getNumPartitions());

    Dataset<Pair<String, Long>> output =
        ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .output();
    assertEquals(N_PARTITIONS, output.getNumPartitions());
  }

  @Test
  public void testOutputExplicitNumPartitionsIsRespected() {
    final int INPUT_PARTITIONS = 78;
    final int OUTPUT_PARTITIONS = 13;

    Flow f = Flow.create();
    Dataset<Pair<String, Long>> input = Util.createMockDataset(f, INPUT_PARTITIONS);
    assertEquals(INPUT_PARTITIONS, input.getNumPartitions());

    Dataset<Pair<String, Long>> output =
        ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .setNumPartitions(OUTPUT_PARTITIONS)
            .output();
    assertEquals(OUTPUT_PARTITIONS, output.getNumPartitions());
  }
}