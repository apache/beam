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

import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test behavior of operator {@code SumByKey}. */
@RunWith(JUnit4.class)
public class SumByKeyTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted =
        SumByKey.named("SumByKey1").of(dataset).keyBy(s -> s).output();
    final SumByKey sum = (SumByKey) TestUtils.getProducer(counted);
    assertTrue(sum.getName().isPresent());
    assertEquals("SumByKey1", sum.getName().get());
    assertNotNull(sum.getKeyExtractor());
    assertFalse(sum.getWindow().isPresent());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted = SumByKey.of(dataset).keyBy(s -> s).output();
    final SumByKey sum = (SumByKey) TestUtils.getProducer(counted);
    assertFalse(sum.getName().isPresent());
  }

  @Test
  public void testBuild_Windowing() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted =
        SumByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();
    final SumByKey sum = (SumByKey) TestUtils.getProducer(counted);
    assertTrue(sum.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) sum.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
    assertEquals(Duration.millis(1000), windowDesc.getAllowedLateness());
  }

  @Test
  public void testWindow_applyIf() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted =
        SumByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
            .output();
    final SumByKey sum = (SumByKey) TestUtils.getProducer(counted);
    assertTrue(sum.getWindow().isPresent());
    final Window<?> window = (Window) sum.getWindow().get();
    assertEquals(FixedWindows.of(org.joda.time.Duration.standardHours(1)), window.getWindowFn());
    assertEquals(DefaultTrigger.of(), WindowDesc.of(window).getTrigger());
    assertEquals(
        AccumulationMode.DISCARDING_FIRED_PANES, WindowDesc.of(window).getAccumulationMode());
  }
}
