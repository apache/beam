/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.junit.Test;

/** Test operator ReduceStateByKey. */
public class ReduceStateByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    DefaultTrigger trigger = DefaultTrigger.of();
    Dataset<KV<String, Long>> reduced =
        ReduceStateByKey.named("ReduceStateByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .mergeStatesBy(WordCountState::combine)
            .windowBy(windowing)
            .triggeredBy(trigger)
            .accumulationMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
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
    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertSame(windowing, windowingDesc.getWindowFn());
    assertSame(trigger, windowingDesc.getTrigger());
    assertSame(AccumulationMode.ACCUMULATING_FIRED_PANES, windowingDesc.getAccumulationMode());
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

    Dataset<KV<String, Long>> reduced =
        ReduceStateByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .stateFactory(WordCountState::new)
            .mergeStatesBy(WordCountState::combine)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertTrue(windowingDesc.getWindowFn() instanceof FixedWindows);
    assertTrue(windowingDesc.getTrigger() instanceof DefaultTrigger);
    assertNotNull(windowingDesc.getAccumulationMode());
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
        .applyIf(
            true,
            b ->
                b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
        .output();

    ReduceStateByKey reduce = (ReduceStateByKey) flow.operators().iterator().next();
    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertTrue(windowingDesc.getWindowFn() instanceof FixedWindows);
    assertTrue(windowingDesc.getTrigger() instanceof DefaultTrigger);
    assertNotNull(windowingDesc.getAccumulationMode());
  }

  /** Simple aggregating state. */
  private static class WordCountState implements State<Long, Long> {
    private final ValueStorage<Long> sum;

    protected WordCountState(StateContext context, Collector<Long> collector) {
      sum =
          context
              .getStorageProvider()
              .getValueStorage(ValueStorageDescriptor.of("sum", Long.class, 0L));
    }

    static void combine(WordCountState target, Iterable<WordCountState> others) {
      for (WordCountState other : others) {
        target.add(other.sum.get());
      }
    }

    @Override
    public void add(Long element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush(Collector<Long> ctx) {
      ctx.collect(sum.get());
    }

    @Override
    public void close() {
      sum.clear();
    }
  }
}
