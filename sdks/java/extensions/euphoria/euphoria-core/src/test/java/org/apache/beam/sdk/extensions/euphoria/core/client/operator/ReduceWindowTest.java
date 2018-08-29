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

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.executor.util.SingleValueContext;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.junit.Test;

/** Test behavior of operator {@code ReduceWindow}. */
public class ReduceWindowTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> output = ReduceWindow.of(dataset).valueBy(e -> "").reduceBy(e -> 1L).output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertEquals(1L, (long) collectSingle(producer.getReducer(), Stream.of("blah")));
    assertEquals("", producer.valueExtractor.apply("blah"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithoutValue() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertEquals(1L, (long) collectSingle(producer.getReducer(), Stream.of("blah")));
    assertEquals("blah", producer.valueExtractor.apply("blah"));

    WindowingDesc windowingDesc = producer.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithValueSorted() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues((l, r) -> l.compareTo(r))
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(DefaultTrigger.of())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertNotNull(producer.valueComparator);
  }

  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues((l, r) -> l.compareTo(r))
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                        .triggeredBy(DefaultTrigger.of())
                        .discardingFiredPanes())
            .output();

    @SuppressWarnings("unchecked")
    ReduceWindow<String, String, Long, ?> producer =
        (ReduceWindow<String, String, Long, ?>) output.getProducer();
    WindowingDesc windowingDesc = producer.getWindowing();
    assertNotNull(windowingDesc);
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowingDesc.getWindowFn());
    assertEquals(DefaultTrigger.of(), windowingDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowingDesc.getAccumulationMode());
  }

  private <InputT, OutputT> OutputT collectSingle(
      ReduceFunctor<InputT, OutputT> fn, Stream<InputT> values) {

    SingleValueContext<OutputT> context;
    context = new SingleValueContext<>();
    fn.apply(values, context);
    return context.getAndResetValue();
  }
}
