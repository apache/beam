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
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
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
        .mergeStatesBy(WordCountState::combine)
        .windowBy(windowing)
        .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceStateByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertNotNull(reduce.getStateMerger());
    assertNotNull(reduce.getStateFactory());
    assertEquals(reduced, reduce.output());
    assertSame(windowing, reduce.getWindowing());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceStateByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .stateFactory(WordCountState::new)
        .mergeStatesBy(WordCountState::combine)
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
            .mergeStatesBy(WordCountState::combine)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
  }

  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceStateByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .stateFactory(WordCountState::new)
        .mergeStatesBy(WordCountState::combine)
        .applyIf(true, b -> b.windowBy(Time.of(Duration.ofHours(1))))
        .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
  }


  /**
   * Simple aggregating state.
   */
  private static class WordCountState implements State<Long, Long> {
    private final ValueStorage<Long> sum;

    protected WordCountState(StateContext context, Collector<Long> collector) {
      sum = context.getStorageProvider().getValueStorage(
          ValueStorageDescriptor.of("sum", Long.class, 0L));
    }

    @Override
    public void add(Long element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush(Collector<Long> ctx) {
      ctx.collect(sum.get());
    }

    static void combine(WordCountState target, Iterable<WordCountState> others) {
      for (WordCountState other : others) {
        target.add(other.sum.get());
      }
    }

    @Override
    public void close() {
      sum.clear();
    }
  }

}
