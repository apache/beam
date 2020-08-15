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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test behavior of operator {@code TopPerKey}. */
@RunWith(JUnit4.class)
public class TopPerKeyTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    final DefaultTrigger trigger = DefaultTrigger.of();
    final PCollection<Triple<String, Long, Long>> result =
        TopPerKey.named("TopPerKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(windowing)
            .triggeredBy(trigger)
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();
    final TopPerKey tpk = (TopPerKey) TestUtils.getProducer(result);
    assertTrue(tpk.getName().isPresent());
    assertEquals("TopPerKey1", tpk.getName().get());
    assertNotNull(tpk.getKeyExtractor());
    assertNotNull(tpk.getValueExtractor());
    assertNotNull(tpk.getScoreExtractor());

    assertTrue(tpk.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) tpk.getWindow().get());
    assertEquals(windowing, windowDesc.getWindowFn());
    assertEquals(trigger, windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
    assertEquals(Duration.millis(1000), windowDesc.getAllowedLateness());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Triple<String, Long, Long>> result =
        TopPerKey.of(dataset).keyBy(s -> s).valueBy(s -> 1L).scoreBy(s -> 1L).output();
    final TopPerKey tpk = (TopPerKey) TestUtils.getProducer(result);
    assertFalse(tpk.getName().isPresent());
  }

  @Test
  public void testBuild_Windowing() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Triple<String, Long, Long>> result =
        TopPerKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .scoreBy(s -> 1L)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    final TopPerKey tpk = (TopPerKey) TestUtils.getProducer(result);
    assertTrue(tpk.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) tpk.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
  }

  @Test
  public void testWindow_applyIf() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Triple<String, Long, Long>> result =
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
    final TopPerKey tpk = (TopPerKey) TestUtils.getProducer(result);
    assertTrue(tpk.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) tpk.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, windowDesc.getAccumulationMode());
  }
}
