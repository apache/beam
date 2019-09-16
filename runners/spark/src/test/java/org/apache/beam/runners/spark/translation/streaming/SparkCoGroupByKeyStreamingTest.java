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
package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.beam.runners.spark.ReuseSparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** A test that verifies that CoGroupByKey works in streaming mode in spark runner. */
public class SparkCoGroupByKeyStreamingTest {

  private static final TupleTag<Integer> INPUT1_TAG = new TupleTag<>("input1");
  private static final TupleTag<Integer> INPUT2_TAG = new TupleTag<>("input2");

  @Rule public final transient ReuseSparkContextRule noContextResue = ReuseSparkContextRule.no();

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private Duration batchDuration() {
    return Duration.millis(
        (pipeline.getOptions().as(SparkPipelineOptions.class)).getBatchIntervalMillis());
  }

  @Category(StreamingTest.class)
  @Test
  public void testInStreamingMode() throws Exception {
    Instant instant = new Instant(0);
    CreateStream<KV<Integer, Integer>> source1 =
        CreateStream.of(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant)
            .nextBatch(
                TimestampedValue.of(KV.of(1, 1), instant),
                TimestampedValue.of(KV.of(1, 2), instant),
                TimestampedValue.of(KV.of(1, 3), instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(1L)))
            .nextBatch(
                TimestampedValue.of(KV.of(2, 4), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(KV.of(2, 5), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(KV.of(2, 6), instant.plus(Duration.standardSeconds(1L))))
            .advanceNextBatchWatermarkToInfinity();

    CreateStream<KV<Integer, Integer>> source2 =
        CreateStream.of(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant)
            .nextBatch(
                TimestampedValue.of(KV.of(1, 11), instant),
                TimestampedValue.of(KV.of(1, 12), instant),
                TimestampedValue.of(KV.of(1, 13), instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(1L)))
            .nextBatch(
                TimestampedValue.of(KV.of(2, 14), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(KV.of(2, 15), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(KV.of(2, 16), instant.plus(Duration.standardSeconds(1L))))
            .advanceNextBatchWatermarkToInfinity();

    PCollection<KV<Integer, Integer>> input1 =
        pipeline
            .apply("create source1", source1)
            .apply(
                "window input1",
                Window.<KV<Integer, Integer>>into(FixedWindows.of(Duration.standardSeconds(3L)))
                    .withAllowedLateness(Duration.ZERO));

    PCollection<KV<Integer, Integer>> input2 =
        pipeline
            .apply("create source2", source2)
            .apply(
                "window input2",
                Window.<KV<Integer, Integer>>into(FixedWindows.of(Duration.standardSeconds(3L)))
                    .withAllowedLateness(Duration.ZERO));

    PCollection<KV<Integer, CoGbkResult>> output =
        KeyedPCollectionTuple.of(INPUT1_TAG, input1)
            .and(INPUT2_TAG, input2)
            .apply(CoGroupByKey.create());
    PAssert.that("Wrong output of the join using CoGroupByKey in streaming mode", output)
        .satisfies(
            (SerializableFunction<Iterable<KV<Integer, CoGbkResult>>, Void>)
                input -> {
                  assertEquals("Wrong size of the output PCollection", 2, Iterables.size(input));
                  for (KV<Integer, CoGbkResult> element : input) {
                    if (element.getKey() == 1) {
                      Iterable<Integer> input1Elements = element.getValue().getAll(INPUT1_TAG);
                      assertEquals(
                          "Wrong number of values for output elements for tag input1 and key 1",
                          3,
                          Iterables.size(input1Elements));
                      assertThat(
                          "Elements of PCollection input1 for key \"1\" are not present in the output PCollection",
                          input1Elements,
                          containsInAnyOrder(1, 2, 3));
                      Iterable<Integer> input2Elements = element.getValue().getAll(INPUT2_TAG);
                      assertEquals(
                          "Wrong number of values for output elements for tag input2 and key 1",
                          3,
                          Iterables.size(input2Elements));
                      assertThat(
                          "Elements of PCollection input2 for key \"1\" are not present in the output PCollection",
                          input2Elements,
                          containsInAnyOrder(11, 12, 13));
                    } else if (element.getKey() == 2) {
                      Iterable<Integer> input1Elements = element.getValue().getAll(INPUT1_TAG);
                      assertEquals(
                          "Wrong number of values for output elements for tag input1 and key 2",
                          3,
                          Iterables.size(input1Elements));
                      assertThat(
                          "Elements of PCollection input1 for key \"2\" are not present in the output PCollection",
                          input1Elements,
                          containsInAnyOrder(4, 5, 6));
                      Iterable<Integer> input2Elements = element.getValue().getAll(INPUT2_TAG);
                      assertEquals(
                          "Wrong number of values for output elements for tag input2 and key 2",
                          3,
                          Iterables.size(input2Elements));
                      assertThat(
                          "Elements of PCollection input2 for key \"2\" are not present in the output PCollection",
                          input2Elements,
                          containsInAnyOrder(14, 15, 16));
                    } else {
                      fail("Unknown key in the output PCollection");
                    }
                  }
                  return null;
                });
    pipeline.run();
  }
}
