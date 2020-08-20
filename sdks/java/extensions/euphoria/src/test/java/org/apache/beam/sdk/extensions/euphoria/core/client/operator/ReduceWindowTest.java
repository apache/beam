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
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.translate.SingleValueContext;
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

/** Test behavior of operator {@code ReduceWindow}. */
@RunWith(JUnit4.class)
public class ReduceWindowTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Long> output =
        ReduceWindow.of(dataset).valueBy(e -> "").reduceBy(e -> 1L).output();
    final ReduceWindow rw = (ReduceWindow) TestUtils.getProducer(output);
    assertEquals(1L, (long) collectSingle(rw.getReducer(), Stream.of("blah")));
    assertEquals("", rw.getValueExtractor().apply("blah"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithoutValue() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();

    final ReduceWindow rw = (ReduceWindow) TestUtils.getProducer(output);

    assertEquals(1L, (long) collectSingle(rw.getReducer(), Stream.of("blah")));
    assertEquals("blah", rw.getValueExtractor().apply("blah"));

    assertTrue(rw.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) rw.getWindow().get());
    assertNotNull(windowDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(Duration.millis(1000), windowDesc.getAllowedLateness());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithValueSorted() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues(String::compareTo)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    final ReduceWindow rw = (ReduceWindow) TestUtils.getProducer(output);
    assertTrue(rw.getValueComparator().isPresent());
  }

  @Test
  public void testWindow_applyIf() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues(String::compareTo)
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .discardingFiredPanes())
            .output();
    final ReduceWindow rw = (ReduceWindow) TestUtils.getProducer(output);
    assertTrue(rw.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) rw.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
  }

  private <InputT, OutputT> OutputT collectSingle(
      ReduceFunctor<InputT, OutputT> fn, Stream<InputT> values) {
    final SingleValueContext<OutputT> context = new SingleValueContext<>();
    fn.apply(values, context);
    return context.getAndResetValue();
  }
}
