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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;
import org.junit.Test;

/** Test operator Distinct. */
public class DistinctTest {

  @Test
  public void testBuild() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    final DefaultTrigger trigger = DefaultTrigger.of();
    final Dataset<String> uniq =
        Distinct.named("Distinct1")
            .of(dataset)
            .windowBy(windowing)
            .triggeredBy(trigger)
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();
    assertTrue(uniq.getProducer().isPresent());
    final Distinct distinct = (Distinct) uniq.getProducer().get();
    assertTrue(distinct.getName().isPresent());
    assertEquals("Distinct1", distinct.getName().get());

    assertTrue(distinct.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) distinct.getWindow().get());
    assertEquals(windowing, windowDesc.getWindowFn());
    assertEquals(trigger, windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
    assertEquals(Duration.millis(1000), windowDesc.getAllowedLateness());
  }

  @Test
  public void testBuild_ImplicitName() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> uniq = Distinct.of(dataset).output();
    assertTrue(uniq.getProducer().isPresent());
    final Distinct distinct = (Distinct) uniq.getProducer().get();
    assertFalse(distinct.getName().isPresent());
  }

  @Test
  public void testBuild_Windowing() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> uniq =
        Distinct.of(dataset)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    assertTrue(uniq.getProducer().isPresent());
    final Distinct distinct = (Distinct) uniq.getProducer().get();
    assertTrue(distinct.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) distinct.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
  }

  @Test
  public void testWindow_applyIf() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> uniq =
        Distinct.of(dataset)
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .discardingFiredPanes())
            .output();
    assertTrue(uniq.getProducer().isPresent());
    final Distinct distinct = (Distinct) uniq.getProducer().get();
    assertTrue(distinct.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) distinct.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
  }
}
