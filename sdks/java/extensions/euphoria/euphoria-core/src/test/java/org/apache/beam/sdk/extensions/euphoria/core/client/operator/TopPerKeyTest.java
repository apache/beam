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

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.junit.Test;

/** Test behavior of operator {@code TopPerKey}. */
public class TopPerKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    DefaultTrigger trigger = DefaultTrigger.of();

    Dataset<Triple<String, Long, Long>> result =
        TopPerKey.named("TopPerKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(windowing)
            .triggeredBy(trigger)
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
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

    WindowingDesc windowingDesc = tpk.getWindowing();
    assertNotNull(windowingDesc);
    assertSame(windowing, windowingDesc.getWindowFn());
    assertSame(trigger, windowingDesc.getTrigger());
    assertSame(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    TopPerKey.of(dataset).keyBy(s -> s).valueBy(s -> 1L).scoreBy(s -> 1L).output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    assertEquals("TopPerKey", tpk.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    TopPerKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .scoreBy(s -> 1L)
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
        .triggeredBy(DefaultTrigger.of())
        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
        .output();

    assertTrue(Iterables.getOnlyElement(flow.operators()) instanceof TopPerKey);
    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    WindowingDesc windowingDesc = tpk.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }

  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    TopPerKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .scoreBy(s -> 1L)
        .applyIf(
            true,
            b ->
                b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .accumulatingFiredPanes())
        .output();

    TopPerKey tpk = (TopPerKey) Iterables.getOnlyElement(flow.operators());
    WindowingDesc windowingDesc = tpk.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }
}
