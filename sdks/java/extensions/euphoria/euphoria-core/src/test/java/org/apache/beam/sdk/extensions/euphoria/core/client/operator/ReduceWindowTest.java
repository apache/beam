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

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Time;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.executor.util.SingleValueContext;
import org.junit.Test;

import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.Assert.*;

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
    Windowing<String, ?> windowing = Time.of(Duration.ofHours(1));

    Dataset<Long> output = ReduceWindow.of(dataset).reduceBy(e -> 1L).windowBy(windowing).output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertEquals(1L, (long) collectSingle(producer.getReducer(), Stream.of("blah")));
    assertEquals("blah", producer.valueExtractor.apply("blah"));
    assertEquals(windowing, producer.windowing);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithValueSorted() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);
    Windowing<String, ?> windowing = Time.of(Duration.ofHours(1));

    Dataset<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues((l, r) -> l.compareTo(r))
            .windowBy(windowing)
            .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertNotNull(producer.valueComparator);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);
    Windowing<String, ?> windowing = Time.of(Duration.ofHours(1));

    Dataset<Long> output =
        ReduceWindow.of(dataset)
            .reduceBy(e -> 1L)
            .withSortedValues((l, r) -> l.compareTo(r))
            .applyIf(true, b -> b.windowBy(windowing))
            .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertTrue(producer.windowing instanceof Time);
  }

  private <InputT, OutputT> OutputT collectSingle(
      ReduceFunctor<InputT, OutputT> fn, Stream<InputT> values) {

    SingleValueContext<OutputT> context;
    context = new SingleValueContext<>();
    fn.apply(values, context);
    return context.getAndResetValue();
  }
}
