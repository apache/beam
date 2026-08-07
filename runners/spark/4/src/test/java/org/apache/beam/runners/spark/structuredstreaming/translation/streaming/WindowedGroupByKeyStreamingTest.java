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

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
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
 * super-operator in {@code BeamStatefulProcessorConfig.Mode #GROUP_ALSO_BY_WINDOW}.
 *
 * <p>Every window this suite asserts on is followed, in the input list, by an element timestamped
 * well past that window's end, per the watermark rule documented on {@link StreamingTestUtils}: the
 * watermark only advances on new data and only fires a window once it has passed the window's end.
 * The mirror image of that rule is that the trailing "sentinel" elements' own windows are never
 * asserted on, because nothing arrives after them to push the watermark past their ends, so they
 * simply never fire.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class WindowedGroupByKeyStreamingTest implements Serializable {

  /**
   * Runs with the module default of {@code spark.kryo.registrationRequired=true}, see {@code
   * BeamStatefulProcessorTest} for why a {@code transformWithState} query needs {@code
   * SparkSessionFactory.SparkKryoRegistrator} to know about {@code StateSchemaMetadata} for that to
   * hold.
   */
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);
  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);

  private SparkStructuredStreamingPipelineOptions options() throws Exception {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    return options;
  }

  /** Renders the collected panes as a sorted {@code key=count} list, for a readable assertion. */
  private static String collectedCounts(String collectorId) {
    List<String> rendered = new ArrayList<>();
    for (KV<String, Long> kv : StreamingTestUtils.<KV<String, Long>>getCollected(collectorId)) {
      rendered.add(kv.getKey() + "=" + kv.getValue());
    }
    Collections.sort(rendered);
    return rendered.toString();
  }

  @Test(timeout = 300_000)
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

    Pipeline pipeline = Pipeline.create(options());
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

    // Only [0s, 10s) ever fires: the sentinel's own window [60s, 70s) has nothing after it to push
    // the watermark past 70s.
    assertEquals("pipeline state=" + result.getState(), "[a=2, b=1]", collectedCounts(collectorId));
  }

  @Test(timeout = 300_000)
  public void slidingWindowsCountPerKey() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("sliding-windows");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
    // A ten second sliding window every five seconds: both these elements fall in exactly two
    // sliding windows, [-5s, 5s) and [0s, 10s).
    elements.add(TimestampedValue.of(KV.of("a", "x"), BASE.plus(Duration.standardSeconds(2))));
    elements.add(TimestampedValue.of(KV.of("a", "y"), BASE.plus(Duration.standardSeconds(3))));
    // Watermark rule: push well past every window under test.
    elements.add(
        TimestampedValue.of(KV.of("sentinel", "s"), BASE.plus(Duration.standardSeconds(60))));

    Pipeline pipeline = Pipeline.create(options());
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

    // One a=2 pane per sliding window the two "a" elements share, so exactly two of them. The
    // sentinel's own windows [55s, 65s) and [60s, 70s) both end after the final watermark of 60s
    // and therefore never fire. Out of scope note: this suite only ever asserts on non-merging
    // windows, session windows are out of POC scope per the roadmap and are rejected outright by
    // GroupByKeyStreamingTranslator#canTranslate.
    assertEquals("pipeline state=" + result.getState(), "[a=2, a=2]", collectedCounts(collectorId));
  }

  @Test(timeout = 300_000)
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

    SparkStructuredStreamingPipelineOptions options = options();
    // Pin one record per split per micro-batch. Without this the whole four element list lands in
    // a single micro-batch, whose start watermark is still -infinity, and the "late" element is
    // then perfectly on time. See the comment below on how the splitting interacts with this.
    options.setMaxRecordsPerMicroBatch(1);

    // This test, alone in the suite, depends on how ListBackedUnboundedSource round robins its
    // elements across splits, so make that dependency loud rather than silent. The session is
    // local[2], so UnboundedSourceDataset asks for two splits and gets
    //   split 0: [a@1s, a@2s]   split 1: [sentinel@60s, sentinel@90s]
    // With one record per split per micro-batch that gives batch 1 = {a@1s, sentinel@60s} (start
    // watermark -infinity, both on time, end watermark 60s) and batch 2 = {a@2s, sentinel@90s}
    // (start watermark 60s, so a@2s in window [0s, 10s) is late and dropped, while the same
    // batch's start watermark fires that window with the single on-time element in it).
    assertEquals(
        "this test assumes a two split source, see the comment above",
        2,
        SESSION.getSession().sparkContext().defaultParallelism());

    Pipeline pipeline = Pipeline.create(options);
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

    // a=1, never a=2: the late "a" was dropped rather than merged into a late pane. sentinel=1 is
    // the sentinel's [60s, 70s) window, which the trailing sentinel@90s pushes the watermark past;
    // its [90s, 100s) window has nothing after it and never fires.
    assertEquals(
        "pipeline state=" + result.getState(), "[a=1, sentinel=1]", collectedCounts(collectorId));
  }
}
