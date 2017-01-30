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
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class ReduceStateByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.named("ReduceStateByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceStateByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertNotNull(reduce.getStateCombiner());
    assertNotNull(reduce.getStateFactory());
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

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertEquals("ReduceStateByKey", reduce.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)), (s -> 0L))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
    assertNotNull(reduce.getEventTimeAssigner());
  }

  @Test
  public void testBuild_Partitioning() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .setPartitioning(new HashPartitioning<>(1))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(!reduce.getPartitioning().hasDefaultPartitioner());
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(1, reduce.getPartitioning().getNumPartitions());
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Partitioner() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .combineStateBy(WordCountState::combine)
            .setPartitioner(new HashPartitioner<>())
            .setNumPartitions(5)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(!reduce.getPartitioning().hasDefaultPartitioner());
    assertTrue(reduce.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, reduce.getPartitioning().getNumPartitions());
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  /**
   * Simple aggregating state.
   */
  private static class WordCountState extends State<Long, Long> {

    private final ValueStorage<Long> sum;

    protected WordCountState(
        Context<Long> context,
        StorageProvider storageProvider) {
      super(context, storageProvider);
      sum = storageProvider.getValueStorage(ValueStorageDescriptor.of(
          "sum", Long.class, 0L));
    }

    @Override
    public void add(Long element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush() {
      this.getContext().collect(sum.get());
    }

    static WordCountState combine(Iterable<WordCountState> others) {
      WordCountState state = null;
      for (WordCountState s : others) {
        if (state == null) {
          state = new WordCountState(
              s.getContext(),
              s.getStorageProvider());
        }
        state.add(s.sum.get());
      }

      return state;
    }

    @Override
    public void close() {
      sum.clear();
    }
  }
}
