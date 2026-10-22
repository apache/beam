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
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End to end tests for windowed GroupByKey, Combine.perKey and Combine.globally on Spark 4. */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class WindowedGroupByKeyStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String TAG_FIXED = "fixed-gbk";
  private static final String TAG_SLIDING = "sliding-combine";
  private static final String TAG_LATE = "late-dropped";
  private static final String TAG_CHAINED = "chained-timer";
  private static final String TAG_GLOBALLY = "globally-combine";
  private static final String TAG_EXPIRY = "gbk-expiry";

  @After
  public void tearDown() {
    TestUnboundedSource.forget(TAG_FIXED);
    TestUnboundedSource.forget(TAG_SLIDING);
    TestUnboundedSource.forget(TAG_LATE);
    TestUnboundedSource.forget(TAG_CHAINED);
    TestUnboundedSource.forget(TAG_GLOBALLY);
    TestUnboundedSource.forget(TAG_EXPIRY);
  }

  private SparkStructuredStreamingPipelineOptions options(String tag) throws Exception {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(tempFolder.newFolder(tag).getAbsolutePath());
    SESSION.configure(options);
    options.setMaxRecordsPerBatch(1L);
    return options;
  }

  @Test
  public void testFixedWindowsGroupByKey() throws Exception {
    String col = StreamingTestUtils.newCollectorId(TAG_FIXED);
    Pipeline p = Pipeline.create(options(TAG_FIXED));
    p.apply(Read.from(new TestUnboundedSource(TAG_FIXED, 1, 6)))
        .apply(ParDo.of(new ToKvFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new FormatGbkFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + ":k=2"));
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + 2000 + ":k=2"));
  }

  @Test
  public void testSlidingWindowsCombinePerKey() throws Exception {
    String col = StreamingTestUtils.newCollectorId(TAG_SLIDING);
    Pipeline p = Pipeline.create(options(TAG_SLIDING));
    p.apply(Read.from(new TestUnboundedSource(TAG_SLIDING, 1, 6)))
        .apply(ParDo.of(new ToKvFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(4)).every(Duration.standardSeconds(2))))
        .apply(Count.perKey())
        .apply(ParDo.of(new FormatCountFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains((TestUnboundedSource.BASE_MILLIS - 2000) + ":k=2:ON_TIME"));
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + ":k=4:ON_TIME"));
  }

  @Test
  public void testCombineGloballyWithoutDefaults() throws Exception {
    String col = StreamingTestUtils.newCollectorId(TAG_GLOBALLY);
    Pipeline p = Pipeline.create(options(TAG_GLOBALLY));
    p.apply(Read.from(new TestUnboundedSource(TAG_GLOBALLY, 1, 6)))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
        .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
        .apply(ParDo.of(new FormatGlobalCountFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + "=2"));
    assertTrue(res.contains((TestUnboundedSource.BASE_MILLIS + 2000) + "=2"));
  }

  @Test
  public void testLateDataDropped() throws Exception {
    String col = StreamingTestUtils.newCollectorId(TAG_LATE);
    Pipeline p = Pipeline.create(options(TAG_LATE));
    p.apply(Read.from(new TestUnboundedSource(TAG_LATE, 2, 8)))
        .apply(ParDo.of(new ToKvFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(4))))
        .apply(Count.perKey())
        .apply(ParDo.of(new FormatCountFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + ":k=1:ON_TIME"));
    assertTrue(res.stream().noneMatch(s -> s.endsWith("LATE")));
  }

  @Test
  public void testChainedStatefulParDoTimerOutputSurvives() throws Exception {
    String col = StreamingTestUtils.newCollectorId(TAG_CHAINED);
    Pipeline p = Pipeline.create(options(TAG_CHAINED));
    p.apply(Read.from(new TestUnboundedSource(TAG_CHAINED, 1, 4)))
        .apply(ParDo.of(new ToKvFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(ParDo.of(new TimerEmitterFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(Window.into(FixedWindows.of(Duration.millis(500))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new FormatGbkFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + ":chained=2"));
  }

  @Test
  public void testExpiredKeyLeavesNoState() throws Exception {
    String checkpoint = tempFolder.newFolder("cp").getAbsolutePath();
    String col = StreamingTestUtils.newCollectorId(TAG_EXPIRY);
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpoint);
    options.setMaxRecordsPerBatch(1L);
    Pipeline p = Pipeline.create(options);
    p.apply(Read.from(new TestUnboundedSource(TAG_EXPIRY, 1, 8)))
        .apply(ParDo.of(new ExpiryKeySelectorFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new FormatGbkFn()))
        .apply(ParDo.of(new StreamingTestUtils.CollectDoFn<>(col)));

    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p).getState());
    Set<String> res = StreamingTestUtils.collected(col);
    assertTrue(res.contains(TestUnboundedSource.BASE_MILLIS + ":k=1"));
    for (String stateVarName : Arrays.asList("beamState", "beamTimers", "beamWatermark")) {
      assertEquals(stateVarName, 0L, StreamingTestUtils.stateRows(checkpoint, stateVarName));
    }
  }

  private static final class ToKvFn extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void process(@Element String el, OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("k", TestUnboundedSource.indexOf(el)));
    }
  }

  private static final class ExpiryKeySelectorFn extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void process(@Element String el, OutputReceiver<KV<String, Integer>> out) {
      int i = TestUnboundedSource.indexOf(el);
      if (i == 0) {
        out.output(KV.of("k", i));
      }
    }
  }

  private static final class FormatGbkFn extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void process(
        @Element KV<String, Iterable<Integer>> el, IntervalWindow w, OutputReceiver<String> out) {
      out.output(w.start().getMillis() + ":" + el.getKey() + "=" + Iterables.size(el.getValue()));
    }
  }

  private static final class FormatCountFn extends DoFn<KV<String, Long>, String> {
    @ProcessElement
    public void process(
        @Element KV<String, Long> el, IntervalWindow w, PaneInfo pane, OutputReceiver<String> out) {
      out.output(
          w.start().getMillis() + ":" + el.getKey() + "=" + el.getValue() + ":" + pane.getTiming());
    }
  }

  private static final class FormatGlobalCountFn extends DoFn<Long, String> {
    @ProcessElement
    public void process(@Element Long count, IntervalWindow w, OutputReceiver<String> out) {
      out.output(w.start().getMillis() + "=" + count);
    }
  }

  private static final class TimerEmitterFn extends DoFn<KV<String, Integer>, KV<String, Integer>> {
    @TimerId("t")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Integer> el,
        @Timestamp Instant ts,
        @TimerId("t") Timer t,
        OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("chained", el.getValue()));
      if (el.getValue() == 0) {
        t.set(ts.plus(Duration.millis(250)));
      }
    }

    @OnTimer("t")
    public void onTimer(@Timestamp Instant ts, OutputReceiver<KV<String, Integer>> out) {
      out.outputWithTimestamp(KV.of("chained", 1), ts);
    }
  }
}
