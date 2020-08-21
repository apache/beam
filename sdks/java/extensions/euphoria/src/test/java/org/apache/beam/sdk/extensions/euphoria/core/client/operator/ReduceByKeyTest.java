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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypePropagationAssert;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator ReduceByKey. */
@RunWith(JUnit4.class)
public class ReduceByKeyTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    final DefaultTrigger trigger = DefaultTrigger.of();
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.named("ReduceByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(windowing)
            .triggeredBy(trigger)
            .discardingFiredPanes()
            .withAllowedLateness(Duration.standardSeconds(1000))
            .output();

    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.getName().isPresent());
    assertEquals("ReduceByKey1", reduce.getName().get());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertTrue(reduce.isCombineFnStyle());
    assertNotNull(reduce.getAccumulatorFactory());
    assertNotNull(reduce.getAccumulate());
    assertNotNull(reduce.getAccumulatorType());
    assertNotNull(reduce.getMergeAccumulators());
    assertNotNull(reduce.getOutputFn());

    assertTrue(reduce.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) reduce.getWindow().get());
    assertEquals(windowing, windowDesc.getWindowFn());
    assertEquals(trigger, windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
    assertEquals(Duration.standardSeconds(1000), windowDesc.getAllowedLateness());
  }

  @Test
  public void testBuild_OutputValues() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Long> reduced =
        ReduceByKey.named("ReduceByKeyValues")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(Sums.ofLongs())
            .outputValues();

    final OutputValues outputValues = (OutputValues) TestUtils.getProducer(reduced);
    assertTrue(outputValues.getName().isPresent());
    assertEquals("ReduceByKeyValues", outputValues.getName().get());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset).keyBy(s -> s).valueBy(s -> 1L).combineBy(Sums.ofLongs()).output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertFalse(reduce.getName().isPresent());
  }

  @Test
  public void testBuild_ReduceBy() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(s -> s.mapToLong(e -> e).sum())
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertNotNull(reduce.getReducer());
    assertFalse(reduce.isCombineFnStyle());
  }

  @Test
  public void testBuild_CombineByStream() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(s -> s.mapToLong(e -> e).sum())
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertNotNull(reduce.getReducer());
    assertFalse(reduce.isCombineFnStyle());
  }

  @Test
  public void testBuild_CombineByFull() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Integer>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(
                () -> new ArrayList<>(),
                (acc, e) -> {
                  acc.add(e);
                  return acc;
                },
                (l, r) -> Lists.newArrayList(Iterables.concat(l, r)),
                List::size,
                TypeDescriptors.lists(TypeDescriptors.longs()),
                TypeDescriptors.integers())
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.isCombineFnStyle());
    assertNotNull(reduce.getAccumulatorFactory());
    assertNotNull(reduce.getAccumulatorType());
    assertNotNull(reduce.getAccumulate());
    assertNotNull(reduce.getMergeAccumulators());
    assertNotNull(reduce.getOutputFn());
    assertTrue(reduce.getOutputType().isPresent());
  }

  @Test
  public void testBuild_CombineBy() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(0L, (a, b) -> a + b)
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.isCombineFnStyle());
    assertNotNull(reduce.getAccumulatorFactory());
    assertNotNull(reduce.getAccumulate());
    assertNotNull(reduce.getMergeAccumulators());
    assertNotNull(reduce.getOutputFn());
    assertTrue(reduce.getOutputType().isPresent());
  }

  @Test
  public void testBuild_Windowing() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(FixedWindows.of(Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();

    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);

    assertTrue(reduce.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final Window<? extends BoundedWindow> window = (Window) reduce.getWindow().get();
    assertEquals(FixedWindows.of(org.joda.time.Duration.standardHours(1)), window.getWindowFn());
    assertEquals(DefaultTrigger.of(), WindowDesc.of(window).getTrigger());
    assertSame(
        AccumulationMode.DISCARDING_FIRED_PANES, WindowDesc.of(window).getAccumulationMode());
    assertFalse(reduce.getValueComparator().isPresent());
  }

  @Test
  public void testBuild_sortedValues() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, List<Long>>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(s -> s.collect(Collectors.toList()))
            .withSortedValues(Long::compare)
            .windowBy(FixedWindows.of(Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.getValueComparator().isPresent());
  }

  @Test
  public void testBuild_sortedValuesWithNoWindowing() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, List<Long>>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(s -> s.collect(Collectors.toList()))
            .withSortedValues(Long::compare)
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.getValueComparator().isPresent());
  }

  @Test
  public void testWindow_applyIf() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(Sums.ofLongs())
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertTrue(reduce.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final Window<? extends BoundedWindow> window = (Window) reduce.getWindow().get();
    assertEquals(FixedWindows.of(org.joda.time.Duration.standardHours(1)), window.getWindowFn());
    assertEquals(DefaultTrigger.of(), WindowDesc.of(window).getTrigger());
    assertSame(
        AccumulationMode.DISCARDING_FIRED_PANES, WindowDesc.of(window).getAccumulationMode());
  }

  @Test
  public void testWindow_applyIfNot() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(Sums.ofLongs())
            .applyIf(
                false,
                b ->
                    b.windowBy(FixedWindows.of(Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    assertFalse(reduce.getWindow().isPresent());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTypeHints_typePropagation() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final TypeDescriptor<String> keyType = TypeDescriptors.strings();
    final TypeDescriptor<Long> valueType = TypeDescriptors.longs();
    final TypeDescriptor<Long> outputType = TypeDescriptors.longs();
    final PCollection<KV<String, Long>> reduced =
        ReduceByKey.of(dataset)
            .keyBy(s -> s, keyType)
            .valueBy(s -> 1L, valueType)
            .combineBy(Sums.ofLongs())
            .output();
    final ReduceByKey reduce = (ReduceByKey) TestUtils.getProducer(reduced);
    TypePropagationAssert.assertOperatorTypeAwareness(reduce, keyType, valueType, outputType);
  }
}
