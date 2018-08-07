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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypePropagationAssert;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.junit.Test;

/** Test operator ReduceByKey. */
public class ReduceByKeyTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    FixedWindows windowing = FixedWindows.of(org.joda.time.Duration.standardHours(1));
    DefaultTrigger trigger = DefaultTrigger.of();

    Dataset<KV<String, Long>> reduced =
        ReduceByKey.named("ReduceByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(windowing)
            .triggeredBy(trigger)
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
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

    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertSame(windowing, windowingDesc.getWindowFn());
    assertSame(trigger, windowingDesc.getTrigger());
    assertSame(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }

  @Test
  public void testBuild_OutputValues() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> reduced =
        ReduceByKey.named("ReduceByKeyValues")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .outputValues();

    assertEquals(flow, reduced.getFlow());
    assertEquals(2, flow.size());

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceByKeyValues", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.getValueExtractor());
    assertNotNull(reduce.getReducer());
    assertNull(reduce.getWindowing());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceByKey.of(dataset)
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

    ReduceByKey.of(dataset)
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

    ReduceByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
        .triggeredBy(DefaultTrigger.of())
        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
        .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();

    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
    assertSame(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
    assertNull(reduce.valueComparator);
  }

  @Test
  public void testBuild_sortedValues() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
        .withSortedValues(Long::compare)
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
        .triggeredBy(DefaultTrigger.of())
        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
        .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.valueComparator);
  }

  @Test
  public void testBuild_sortedValuesWithNoWindowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
        .withSortedValues(Long::compare)
        .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.valueComparator);
  }

  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
        .withSortedValues(Long::compare)
        .applyIf(
            true,
            b ->
                b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
        .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
    assertSame(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }

  @Test
  public void testWindow_applyIfNot() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    ReduceByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
        .withSortedValues(Long::compare)
        .applyIf(
            false,
            b ->
                b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
        .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    WindowingDesc windowingDesc = reduce.getWindowing();
    assertNull(windowingDesc);
  }

  @Test
  public void testRBKTypePropagation() {
    Flow flow1 = Flow.create("TEST1");
    Dataset<String> dataset = Util.createMockDataset(flow1, 2);

    TypeDescriptor<String> keyType = TypeDescriptors.strings();
    TypeDescriptor<Long> valueType = TypeDescriptors.longs();
    TypeDescriptor<Long> combinedOutputType = TypeDescriptors.longs();

    ReduceByKey.of(dataset)
        .keyBy(s -> s, keyType)
        .valueBy(s -> 1L, valueType)
        .combineBy(n -> n.mapToLong(l -> l).sum(), combinedOutputType)
        .output();

    ReduceByKey rbk = (ReduceByKey) flow1.operators().iterator().next();
    TypePropagationAssert.assertOperatorTypeAwareness(rbk, combinedOutputType, keyType, valueType);

    Flow flow2 = Flow.create("TEST1");
    Dataset<String> dataset2 = Util.createMockDataset(flow2, 2);
    TypeDescriptor<String> reducedOutputType = TypeDescriptors.strings();
    ReduceByKey.of(dataset2)
        .keyBy(s -> s, keyType)
        .valueBy(s -> 1L, valueType)
        .reduceBy(n -> "Sum: " + n.mapToLong(l -> l).sum(), reducedOutputType)
        .output();

    rbk = (ReduceByKey) flow2.operators().iterator().next();
    TypePropagationAssert.assertOperatorTypeAwareness(rbk, reducedOutputType, keyType, valueType);
  }
}
