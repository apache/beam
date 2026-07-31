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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Windowed {@code GroupByKey} (via {@link Count#perKey()}, which auto-expands to {@code GroupByKey}
 * + {@code Combine} since {@code Combine.PerKey} is deliberately unregistered for streaming, see
 * {@code PipelineTranslatorStreaming}). This is hosted by the generic {@code transformWithState}
 * super-operator in {@code BeamStatefulProcessorConfig.Mode #GROUP_ALSO_BY_WINDOW}, so every test
 * here needs the same Kryo relaxation as {@code BeamStatefulProcessorTest}.
 *
 * <p>Every window this suite asserts on is followed, in the input list, by an element timestamped
 * well past that window's end, per the watermark rule documented on {@link StreamingTestUtils}: the
 * watermark only advances on new data and only fires a window once it has passed the window's end.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class WindowedGroupByKeyStreamingTest implements Serializable {

  /**
   * See {@code BeamStatefulProcessorTest}: {@code transformWithState} broadcasts {@code
   * StateSchemaMetadata} through Kryo, which is not registered anywhere, so the test JVM default of
   * {@code spark.kryo.registrationRequired=true} (runners/spark/spark_runner.gradle) must be
   * relaxed for any query that ends up hosting a stateful operator, windowed GroupByKey included.
   */
  @ClassRule
  public static final SparkSessionRule SESSION =
      new SparkSessionRule(KV.of("spark.kryo.registrationRequired", "false"));

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);
  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);

  private SparkStructuredStreamingPipelineOptions options() throws Exception {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    return options;
  }

  @Test(timeout = 300_000)
  @Ignore(
      "Needs GroupByKeyStreamingTranslator (WS-D2), which hosts the expanded GroupByKey via "
          + "BeamStatefulProcessorConfig.Mode.GROUP_ALSO_BY_WINDOW; GroupByKey currently has no "
          + "streaming translation and pipeline.run() throws before any window can fire.")
  public void fixedWindowsCountPerKey() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("fixed-windows");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
    // All three fall in the first ten second window [0s, 10s).
    elements.add(TimestampedValue.of(KV.of("a", "x"), BASE));
    elements.add(TimestampedValue.of(KV.of("a", "y"), BASE.plus(Duration.standardSeconds(1))));
    elements.add(TimestampedValue.of(KV.of("b", "z"), BASE.plus(Duration.standardSeconds(2))));
    // Watermark rule: a much later element so the watermark passes the first window's end.
    elements.add(
        TimestampedValue.of(KV.of("sentinel", "s"), BASE.plus(Duration.standardSeconds(60))));

    TestPipeline pipeline = TestPipeline.fromOptions(options());
    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    elements, KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))))
        .apply("FixedWindows", Window.into(FixedWindows.of(WINDOW_SIZE)))
        .apply("CountPerKey", Count.perKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // TODO(WS-D2): assert StreamingTestUtils.<KV<String, Long>>getCollected(collectorId) contains
    // exactly KV.of("a", 2L) and KV.of("b", 1L) for the [0s, 10s) window. Remember the one
    // micro-batch timer latency floor documented on StreamingTestUtils: the end-of-window timer
    // fires one micro-batch after the sentinel's batch, not within it.
  }

  @Test(timeout = 300_000)
  @Ignore("Needs GroupByKeyStreamingTranslator (WS-D2), see fixedWindowsCountPerKey for why.")
  public void slidingWindowsCountPerKey() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("sliding-windows");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
    // A five second sliding window every five seconds overlapping the ten second fixed window
    // above: this element falls in two sliding windows, [-5s, 5s) and [0s, 10s).
    elements.add(TimestampedValue.of(KV.of("a", "x"), BASE.plus(Duration.standardSeconds(2))));
    elements.add(TimestampedValue.of(KV.of("a", "y"), BASE.plus(Duration.standardSeconds(3))));
    // Watermark rule: push well past every window under test.
    elements.add(
        TimestampedValue.of(KV.of("sentinel", "s"), BASE.plus(Duration.standardSeconds(60))));

    TestPipeline pipeline = TestPipeline.fromOptions(options());
    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    elements, KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))))
        .apply(
            "SlidingWindows",
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(5))))
        .apply("CountPerKey", Count.perKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // TODO(WS-D2): assert StreamingTestUtils.<KV<String, Long>>getCollected(collectorId) contains
    // one KV.of("a", 2L) pane per overlapping sliding window the two "a" elements both fall into
    // (out of scope note: this suite only ever asserts on non-merging windows; session windows are
    // out of POC scope per the roadmap).
  }

  @Test(timeout = 300_000)
  @Ignore("Needs GroupByKeyStreamingTranslator (WS-D2), see fixedWindowsCountPerKey for why.")
  public void lateDataIsDropped() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("late-data-dropped");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
    // On-time element in the first window [0s, 10s).
    elements.add(
        TimestampedValue.of(KV.of("a", "on-time"), BASE.plus(Duration.standardSeconds(1))));
    // Jump the watermark far past the first window's end (and its zero allowed lateness) before
    // the late element arrives: this is the whole point of the test, the watermark is monotonic
    // in the *order elements are read*, not in event time order, so a small timestamp read after a
    // much larger one is unambiguously late.
    elements.add(
        TimestampedValue.of(KV.of("sentinel", "s"), BASE.plus(Duration.standardSeconds(60))));
    // Late: arrives after the watermark has already passed the end of the first window, and the
    // default windowing strategy has zero allowed lateness, so this must be dropped, not emitted
    // as a second, late pane.
    elements.add(TimestampedValue.of(KV.of("a", "late"), BASE.plus(Duration.standardSeconds(2))));
    // One more push so there is a micro-batch that can observe the watermark has not moved
    // backwards and the drop truly happened rather than merely not having fired yet.
    elements.add(
        TimestampedValue.of(KV.of("sentinel", "t"), BASE.plus(Duration.standardSeconds(90))));

    TestPipeline pipeline = TestPipeline.fromOptions(options());
    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    elements, KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))))
        .apply("FixedWindows", Window.into(FixedWindows.of(WINDOW_SIZE)))
        .apply("CountPerKey", Count.perKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // TODO(WS-D2): assert StreamingTestUtils.<KV<String, Long>>getCollected(collectorId) contains
    // KV.of("a", 1L) (the on-time element only) for the first window, and never a count of 2:  the
    // late "a" element must be dropped, not merged into a late pane.
  }
}
