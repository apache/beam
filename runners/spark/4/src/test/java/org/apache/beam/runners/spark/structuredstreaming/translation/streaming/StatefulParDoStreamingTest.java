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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamReaderCache;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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

/** End to end tests for stateful ParDo on Spark 4 Structured Streaming. */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StatefulParDoStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String TAG_BATCHES = "stateful-batches";
  private static final String TAG_RESTART = "stateful-restart";

  @After
  public void tearDown() {
    TestUnboundedSource.forget(TAG_BATCHES);
    TestUnboundedSource.forget(TAG_RESTART);
  }

  private static Pipeline pipeline(
      SparkStructuredStreamingPipelineOptions options,
      String tag,
      int count,
      DoFn<KV<String, String>, String> fn,
      String collectorId) {
    options.setMaxRecordsPerBatch(1L);
    Pipeline p = Pipeline.create(options);
    p.apply("Read", Read.from(new TestUnboundedSource(tag, 1, count)))
        .apply("KeySelector", ParDo.of(new KeySelectorFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("StatefulParDo", ParDo.of(fn))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));
    return p;
  }

  @Test
  public void testStateAndTimersAcrossBatches() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId(TAG_BATCHES);
    Pipeline pipeline =
        pipeline(
            StreamingTestUtils.streamingOptions(tempFolder),
            TAG_BATCHES,
            10,
            new StateAndTimerTestFn(),
            collectorId);
    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(pipeline).getState());

    Set<String> collected = StreamingTestUtils.collected(collectorId);
    assertTrue(
        collected.containsAll(
            Arrays.asList("k1-seen-1", "k1-seen-2", "k2-seen-1", "k1-timer", "k2-early-1")));
    assertFalse(collected.contains("k2-early-2"));
    assertFalse(collected.contains("k2-late"));
  }

  @Test
  public void testCheckpointRestartPreservesStateAndTimers() throws Exception {
    String checkpoint = tempFolder.newFolder("cp").getAbsolutePath();
    String c1 = StreamingTestUtils.newCollectorId("r1");
    String c2 = StreamingTestUtils.newCollectorId("r2");

    Pipeline p1 =
        pipeline(
            StreamingTestUtils.streamingOptions(checkpoint),
            TAG_RESTART,
            3,
            new RestartTestFn(),
            c1);
    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p1).getState());
    BeamReaderCache.invalidateAll();

    TestUnboundedSource.extend(TAG_RESTART, 10);

    Pipeline p2 =
        pipeline(
            StreamingTestUtils.streamingOptions(checkpoint),
            TAG_RESTART,
            10,
            new RestartTestFn(),
            c2);
    assertEquals(PipelineResult.State.DONE, StreamingTestUtils.run(p2).getState());

    Set<String> collected = StreamingTestUtils.collected(c2);
    assertTrue(collected.contains("k1-resumed-saved-val") && collected.contains("k1-timer"));
    assertFalse(collected.contains("k1-started"));
  }

  private static final class KeySelectorFn extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void process(@Element String element, OutputReceiver<KV<String, String>> out) {
      int i = TestUnboundedSource.indexOf(element);
      out.output(KV.of((i == 0 || i == 4) ? "k1" : (i == 1 || i == 3) ? "k2" : "k_other", element));
    }
  }

  private static final class StateAndTimerTestFn extends DoFn<KV<String, String>, String> {
    @StateId("count")
    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

    @StateId("earlyFired")
    private final StateSpec<ValueState<Integer>> earlyFiredSpec = StateSpecs.value();

    @TimerId("timer1")
    private final TimerSpec timer1Spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("timerEarly")
    private final TimerSpec timerEarlySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("timerLate")
    private final TimerSpec timerLateSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, String> element,
        @Timestamp Instant ts,
        @StateId("count") ValueState<Integer> countState,
        @TimerId("timer1") Timer timer1,
        @TimerId("timerEarly") Timer timerEarly,
        @TimerId("timerLate") Timer timerLate,
        OutputReceiver<String> out) {
      String key = element.getKey();
      int count = (countState.read() == null ? 0 : countState.read()) + 1;
      countState.write(count);
      out.output(key + "-seen-" + count);

      if ("k1".equals(key) && count == 1) {
        timer1.set(ts.plus(Duration.millis(3000)));
      } else if ("k2".equals(key) && count == 1) {
        timerEarly.set(ts.plus(Duration.millis(1000)));
        timerLate.set(ts.plus(Duration.millis(1500)));
      }
    }

    @OnTimer("timer1")
    public void onTimer1(OutputReceiver<String> out) {
      out.output("k1-timer");
    }

    @OnTimer("timerEarly")
    public void onTimerEarly(
        @TimerId("timerLate") Timer timerLate,
        @StateId("earlyFired") ValueState<Integer> earlyFiredState,
        OutputReceiver<String> out) {
      int count = (earlyFiredState.read() == null ? 0 : earlyFiredState.read()) + 1;
      earlyFiredState.write(count);
      out.output("k2-early-" + count);
      timerLate.clear();
    }

    @OnTimer("timerLate")
    public void onTimerLate(OutputReceiver<String> out) {
      out.output("k2-late");
    }
  }

  private static final class RestartTestFn extends DoFn<KV<String, String>, String> {
    @StateId("stored")
    private final StateSpec<ValueState<String>> storedSpec = StateSpecs.value();

    @TimerId("timer")
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, String> element,
        @Timestamp Instant ts,
        @StateId("stored") ValueState<String> storedState,
        @TimerId("timer") Timer timer,
        OutputReceiver<String> out) {
      String key = element.getKey();
      String stored = storedState.read();
      if (stored == null) {
        storedState.write("saved-val");
        timer.set(ts.plus(Duration.millis(4000)));
        out.output(key + "-started");
      } else {
        out.output(key + "-resumed-" + stored);
      }
    }

    @OnTimer("timer")
    public void onTimer(OutputReceiver<String> out) {
      out.output("k1-timer");
    }
  }
}
