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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.Arrays;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Simple test suite for RBK. */
public class ReduceByKeyTest {

  @Test
  public void testSimpleRBK() {
    final Flow flow = Flow.create();

    final ListDataSource<Integer> input =
        ListDataSource.unbounded(Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 4));

    final ListDataSink<KV<Integer, Integer>> output = ListDataSink.get();

    ReduceByKey.of(flow.createInput(input, e -> 1000L * e))
        .keyBy(i -> i % 2)
        .reduceBy(Sums.ofInts())
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
        .triggeredBy(AfterWatermark.pastEndOfWindow())
        .discardingFiredPanes()
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(), KV.of(0, 8), KV.of(1, 7));
  }

  @Test
  public void testEventTime() {

    Flow flow = Flow.create();
    ListDataSource<KV<Integer, Long>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                KV.of(1, 300L),
                KV.of(2, 600L),
                KV.of(3, 900L),
                KV.of(2, 1300L),
                KV.of(3, 1600L),
                KV.of(1, 1900L),
                KV.of(3, 2300L),
                KV.of(2, 2600L),
                KV.of(1, 2900L),
                KV.of(2, 3300L),
                KV.of(2, 300L),
                KV.of(4, 600L),
                KV.of(3, 900L),
                KV.of(4, 1300L),
                KV.of(2, 1600L),
                KV.of(3, 1900L),
                KV.of(4, 2300L),
                KV.of(1, 2600L),
                KV.of(3, 2900L),
                KV.of(4, 3300L),
                KV.of(3, 3600L)));

    ListDataSink<KV<Integer, Long>> sink = ListDataSink.get();
    Dataset<KV<Integer, Long>> input = flow.createInput(source);
    input = AssignEventTime.of(input).using(KV::getValue).output();

    ReduceByKey.of(input)
        .keyBy(KV::getKey)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
        .triggeredBy(AfterWatermark.pastEndOfWindow())
        .discardingFiredPanes()
        .output()
        .persist(sink);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        KV.of(2, 2L),
        KV.of(4, 1L), // first window
        KV.of(2, 2L),
        KV.of(4, 1L), // second window
        KV.of(2, 1L),
        KV.of(4, 1L), // third window
        KV.of(2, 1L),
        KV.of(4, 1L), // fourth window
        KV.of(1, 1L),
        KV.of(3, 2L), // first window
        KV.of(1, 1L),
        KV.of(3, 2L), // second window
        KV.of(1, 2L),
        KV.of(3, 2L), // third window
        KV.of(3, 1L)); // fourth window
  }

  @Test
  public void testElementTimestamp() {

    Flow flow = Flow.create();
    ListDataSource<KV<Integer, Long>> source =
        ListDataSource.bounded(
            Arrays.asList(
                // ~ KV.of(value, time)
                KV.of(1, 10_123L),
                KV.of(2, 11_234L),
                KV.of(3, 12_345L),
                // ~ note: exactly one element for the window on purpose (to test out
                // all is well even in case our `.combineBy` user function is not called.)
                KV.of(4, 21_456L)));
    Dataset<KV<Integer, Long>> input = flow.createInput(source);

    input = AssignEventTime.of(input).using(KV::getValue).output();

    Dataset<KV<String, Integer>> reduced =
        ReduceByKey.of(input)
            .keyBy(e -> "", TypeDescriptors.strings())
            .valueBy(KV::getKey, TypeDescriptors.integers())
            .combineBy(Sums.ofInts(), TypeDescriptors.integers())
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output();

    ListDataSink<KV<String, Integer>> sink = ListDataSink.get();
    reduced.persist(sink);

    BeamRunnerWrapper.ofDirect().executeSync(flow);
    DatasetAssert.unorderedEquals(
        sink.getOutputs(), KV.of("", 1), KV.of("", 2), KV.of("", 3), KV.of("", 4));
  }
}
