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
package org.apache.beam.runners.spark.aggregators.metrics.sink;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.spark.ReuseSparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;

/**
 * A test that verifies Beam metrics are reported to Spark's metrics sink in both batch and
 * streaming modes.
 */
public class SparkMetricsSinkTest {
  @Rule public ExternalResource inMemoryMetricsSink = new InMemoryMetricsSinkRule();
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ReuseSparkContextRule noContextResue = ReuseSparkContextRule.no();

  private static final ImmutableList<String> WORDS =
      ImmutableList.of("hi there", "hi", "hi sue bob", "hi sue", "", "bob hi");
  private static final ImmutableSet<String> EXPECTED_COUNTS =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testInBatchMode() throws Exception {
    assertThat(InMemoryMetrics.valueOf("emptyLines"), is(nullValue()));

    final PCollection<String> output =
        pipeline
            .apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()))
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));
    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNTS);
    pipeline.run();

    assertThat(InMemoryMetrics.<Double>valueOf("emptyLines"), is(1d));
  }

  @Category(StreamingTest.class)
  @Test
  public void testInStreamingMode() throws Exception {
    assertThat(InMemoryMetrics.valueOf("emptyLines"), is(nullValue()));

    Instant instant = new Instant(0);
    CreateStream<String> source =
        CreateStream.of(
                StringUtf8Coder.of(),
                Duration.millis(
                    (pipeline.getOptions().as(SparkPipelineOptions.class))
                        .getBatchIntervalMillis()))
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant)
            .nextBatch(
                TimestampedValue.of(WORDS.get(0), instant),
                TimestampedValue.of(WORDS.get(1), instant),
                TimestampedValue.of(WORDS.get(2), instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(2L)))
            .nextBatch(
                TimestampedValue.of(WORDS.get(3), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(WORDS.get(4), instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(WORDS.get(5), instant.plus(Duration.standardSeconds(1L))))
            .advanceNextBatchWatermarkToInfinity();
    PCollection<String> output =
        pipeline
            .apply(source)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(3L)))
                    .withAllowedLateness(Duration.ZERO))
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));
    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNTS);
    pipeline.run();

    assertThat(InMemoryMetrics.<Double>valueOf("emptyLines"), is(1d));
  }
}
