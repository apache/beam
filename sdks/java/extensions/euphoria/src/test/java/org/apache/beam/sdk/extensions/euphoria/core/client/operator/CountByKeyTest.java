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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator CountByKey. */
@RunWith(JUnit4.class)
public class CountByKeyTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    final DefaultTrigger trigger = DefaultTrigger.of();
    final PCollection<KV<String, Long>> counted =
        CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(windowing)
            .triggeredBy(trigger)
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();
    final CountByKey count = (CountByKey) TestUtils.getProducer(counted);
    assertTrue(count.getName().isPresent());
    assertEquals("CountByKey1", count.getName().get());
    assertNotNull(count.getKeyExtractor());
    assertTrue(count.getWindow().isPresent());
    final WindowDesc<?> desc = WindowDesc.of((Window<?>) count.getWindow().get());
    assertEquals(windowing, desc.getWindowFn());
    assertEquals(trigger, desc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, desc.getAccumulationMode());
    assertEquals(Duration.millis(1000), desc.getAllowedLateness());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted = CountByKey.of(dataset).keyBy(s -> s).output();
    final CountByKey count = (CountByKey) TestUtils.getProducer(counted);
    assertFalse(count.getName().isPresent());
  }

  @Test
  public void testBuild_Windowing() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> counted =
        CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    final CountByKey count = (CountByKey) TestUtils.getProducer(counted);
    assertTrue(count.getWindow().isPresent());
    final WindowDesc<?> desc = WindowDesc.of((Window<?>) count.getWindow().get());
    assertEquals(FixedWindows.of(org.joda.time.Duration.standardHours(1)), desc.getWindowFn());
    assertEquals(DefaultTrigger.of(), desc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, desc.getAccumulationMode());
  }

  @Test
  public void testWindow_applyIf() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    final DefaultTrigger trigger = DefaultTrigger.of();
    final PCollection<KV<String, Long>> counted =
        CountByKey.named("CountByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .applyIf(true, b -> b.windowBy(windowing).triggeredBy(trigger).discardingFiredPanes())
            .output();
    final CountByKey count = (CountByKey) TestUtils.getProducer(counted);
    assertTrue(count.getWindow().isPresent());
    final WindowDesc<?> desc = WindowDesc.of((Window<?>) count.getWindow().get());
    assertEquals(windowing, desc.getWindowFn());
    assertEquals(trigger, desc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, desc.getAccumulationMode());
  }

  @Test
  public void testBuildTypePropagation() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final TypeDescriptor<String> keyType = TypeDescriptors.strings();
    final PCollection<KV<String, Long>> counted =
        CountByKey.named("CountByKey1").of(dataset).keyBy(s -> s, keyType).output();
    final CountByKey count = (CountByKey) TestUtils.getProducer(counted);
    assertTrue(count.getKeyType().isPresent());
    assertEquals(count.getKeyType().get(), keyType);
    assertTrue(count.getOutputType().isPresent());
    assertEquals(
        TypeDescriptors.kvs(keyType, TypeDescriptors.longs()), count.getOutputType().get());
  }
}
