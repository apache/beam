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
package org.apache.beam.runners.spark;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RegexMatcher;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * A test suite for the propagation of watermarks in the Spark runner.
 */
public class WatermarkTest {

  @Rule
  public ClearWatermarksRule clearWatermarksRule = new ClearWatermarksRule();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ReuseSparkContext reuseContext = ReuseSparkContext.yes();

  private static final SparkPipelineOptions options =
      PipelineOptionsFactory.create().as(SparkPipelineOptions.class);

  private static final String INSTANT_PATTERN =
      "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z";

  @Test
  public void testLowHighWatermarksAdvance() {
    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);

    Instant instant = new Instant(0);
    // low == high.
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(5)),
            instant.plus(Duration.millis(5)),
            instant));
    GlobalWatermarkHolder.advance(jsc);
    // low < high.
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(10)),
            instant.plus(Duration.millis(15)),
            instant.plus(Duration.millis(100))));
    GlobalWatermarkHolder.advance(jsc);

    // assert watermarks in Broadcast.
    SparkWatermarks currentWatermarks = GlobalWatermarkHolder.get().getValue().get(1);
    assertThat(currentWatermarks.getLowWatermark(), equalTo(instant.plus(Duration.millis(10))));
    assertThat(currentWatermarks.getHighWatermark(), equalTo(instant.plus(Duration.millis(15))));
    assertThat(currentWatermarks.getSynchronizedProcessingTime(),
        equalTo(instant.plus(Duration.millis(100))));

    // assert illegal watermark advance.
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        RegexMatcher.matches(
            "Low watermark " + INSTANT_PATTERN + " cannot be later then high watermark "
            + INSTANT_PATTERN));
    // low > high -> not allowed!
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(25)),
            instant.plus(Duration.millis(20)),
            instant.plus(Duration.millis(200))));
    GlobalWatermarkHolder.advance(jsc);
  }

  @Test
  public void testSynchronizedTimeMonotonic() {
    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);

    Instant instant = new Instant(0);
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(5)),
            instant.plus(Duration.millis(10)),
            instant));
    GlobalWatermarkHolder.advance(jsc);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Synchronized processing time must advance.");
    // no actual advancement of watermarks - fine by Watermarks
    // but not by synchronized processing time.
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(5)),
            instant.plus(Duration.millis(10)),
            instant));
    GlobalWatermarkHolder.advance(jsc);
  }

  @Test
  public void testMultiSource() {
    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);

    Instant instant = new Instant(0);
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(5)),
            instant.plus(Duration.millis(10)),
            instant));
    GlobalWatermarkHolder.add(2,
        new SparkWatermarks(
            instant.plus(Duration.millis(3)),
            instant.plus(Duration.millis(6)),
            instant));

    GlobalWatermarkHolder.advance(jsc);

    // assert watermarks for source 1.
    SparkWatermarks watermarksForSource1 = GlobalWatermarkHolder.get().getValue().get(1);
    assertThat(watermarksForSource1.getLowWatermark(), equalTo(instant.plus(Duration.millis(5))));
    assertThat(watermarksForSource1.getHighWatermark(), equalTo(instant.plus(Duration.millis(10))));

    // assert watermarks for source 2.
    SparkWatermarks watermarksForSource2 = GlobalWatermarkHolder.get().getValue().get(2);
    assertThat(watermarksForSource2.getLowWatermark(), equalTo(instant.plus(Duration.millis(3))));
    assertThat(watermarksForSource2.getHighWatermark(), equalTo(instant.plus(Duration.millis(6))));
  }

  @Test
  @Ignore(
      "BEAM-1526 - This test is flaky, and is expected to be fixed in "
          + "https://github.com/apache/beam/pull/2050")
  public void testInDoFn() {
    // because watermark advances onBatchCompleted.
    Iterable<Integer> zeroBatch = Collections.emptyList();
    Iterable<Integer> firstBatch = Collections.singletonList(1);
    Iterable<Integer> secondBatch = Collections.singletonList(2);

    Instant instant = new Instant(0);
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(5)),
            instant.plus(Duration.millis(10)),
            instant));
    GlobalWatermarkHolder.add(1,
        new SparkWatermarks(
            instant.plus(Duration.millis(10)),
            instant.plus(Duration.millis(15)),
            instant.plus(options.getBatchIntervalMillis())));

    options.setRunner(SparkRunner.class);
    options.setStreaming(true);
    options.setBatchIntervalMillis(500L);
    Pipeline p = Pipeline.create(options);

    CreateStream.QueuedValues<Integer> queueStream =
        CreateStream.fromQueue(Arrays.asList(zeroBatch, firstBatch, secondBatch));

    p.apply(queueStream).setCoder(VarIntCoder.of()).apply(ParDo.of(new WatermarksDoFn(1)));

    p.run().waitUntilFinish(Duration.millis(options.getBatchIntervalMillis()).multipliedBy(3));

    // this is a hacky way to assert but it will do until triggers are supported.
    assertThat(
        WatermarksDoFn.strings,
        containsInAnyOrder(
            "element: 1 lowWatermark: 5 highWatermark: 10 processingTime: 0",
            "element: 2 lowWatermark: 10 highWatermark: 15 processingTime: 1000"));
  }

  private static class WatermarksDoFn extends DoFn<Integer, String> {
    private final int sourceId;

    static List<String> strings = new ArrayList<>();

    private WatermarksDoFn(int sourceId) {
      this.sourceId = sourceId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (GlobalWatermarkHolder.get() == null
          || GlobalWatermarkHolder.get().getValue().get(sourceId) == null) {
        // watermark not yet updated.
        return;
      }
      SparkWatermarks sparkWatermarks = GlobalWatermarkHolder.get().getValue().get(sourceId);
      Integer element = c.element();
      String output =
          "element: " + element
          + " lowWatermark: " + sparkWatermarks.getLowWatermark().getMillis()
          + " highWatermark: " + sparkWatermarks.getHighWatermark().getMillis()
          + " processingTime: " + sparkWatermarks.getSynchronizedProcessingTime().getMillis();
      strings.add(output);
    }
  }

}
