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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/** Test that a sub-flow applied on sink is correctly preserved. */
public class SinkTest extends AbstractOperatorTest {

  @Test
  public void testOutputGroupingSorting() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Long>>() {
          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<Integer> input) {
            // ~ use stable event-time watermark
            input = AssignEventTime.of(input).using(e -> 0).output();
            return CountByKey.of(input)
                .keyBy(e -> e)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          public ListDataSink<KV<Integer, Long>> modifySink(ListDataSink<KV<Integer, Long>> sink) {

            return sink.withPrepareDataset(
                d -> {
                  ReduceByKey.of(d)
                      .keyBy(p -> p.getKey() % 2)
                      .valueBy(KV::getValue)
                      .reduceBy(
                          (Stream<Long> values, Collector<Long> c) -> values.forEach(c::collect))
                      .withSortedValues(Long::compare)
                      .output()
                      .persist(sink);
                });
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 10, 9, 8, 7, 6, 5, 4);
          }

          @Override
          public void validate(List<KV<Integer, Long>> outputs) throws AssertionError {

            // the output should be two arbitrarily interleaved
            // sorted sequences

            // split these sequences by key and collect back
            Map<Integer, List<KV<Integer, Long>>> split =
                outputs.stream().collect(Collectors.groupingBy(KV::getKey));

            // then verify that these sequences are sorted
            assertEquals(2, split.size());
            assertNotNull(split.get(0));
            assertNotNull(split.get(1));

            assertEquals(Arrays.asList(2, 4, 4, 6, 6, 8, 10), split.get(0));
            assertEquals(Arrays.asList(1, 3, 5, 5, 6, 6, 9), split.get(1));
          }
        });
  }
}
