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

package org.apache.beam.runners.spark.metrics;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.metrics.DummyMetricsSink;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.spark.ReuseSparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test that verifies that metrics push system works in spark runner.
 */
public class SparkMetricsPusherTest {

  @Rule
  public final transient ReuseSparkContextRule noContextResue = ReuseSparkContextRule.no();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  private Duration batchDuration() {
    return Duration.millis(
        (pipeline.getOptions().as(SparkPipelineOptions.class)).getBatchIntervalMillis());
  }

  @Category(StreamingTest.class)
  @Test
  public void testInStreamingMode() throws Exception {
    assertThat(DummyMetricsSink.getCounterValue(), is(0L));

    Instant instant = new Instant(0);
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant)
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(1L)))
            .nextBatch(
                TimestampedValue.of(4, instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(5, instant.plus(Duration.standardSeconds(1L))),
                TimestampedValue.of(6, instant.plus(Duration.standardSeconds(1L))))
            .advanceNextBatchWatermarkToInfinity();
    pipeline
        .apply(source)
        .apply(
            Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(3L)))
                .withAllowedLateness(Duration.ZERO))
        .apply(ParDo.of(new CountingDoFn()));

    pipeline.run();
    // give metrics pusher time to push
    Thread.sleep((MetricsPusher.DEFAULT_PERIOD + 1L) * 1000);
    assertThat(DummyMetricsSink.getCounterValue(), is(6L));
  }

  private static class CountingDoFn extends DoFn<Integer, Integer>{
    private final Counter counter = Metrics
        .counter(SparkMetricsPusherTest.class, "counter");
    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        counter.inc();
        context.output(context.element());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Category(ValidatesRunner.class)
  @Test
  public void testInSBatchMode() throws Exception {
    assertThat(DummyMetricsSink.getCounterValue(), is(0L));
    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6))
        .apply(ParDo.of(new CountingDoFn()));

    pipeline.run();
    // give metrics pusher time to push
    Thread.sleep((MetricsPusher.DEFAULT_PERIOD + 1L) * 1000);
    assertThat(DummyMetricsSink.getCounterValue(), is(6L));
  }

}
