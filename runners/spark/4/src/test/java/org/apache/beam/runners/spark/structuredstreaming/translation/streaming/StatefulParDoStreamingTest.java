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
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
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
 * A stateful {@code ParDo} in the global window: a {@code @StateId ValueState<Boolean>} dedups
 * repeated keys, and an event time {@code @TimerId} emits a sentinel once it expires. Hosted by the
 * generic {@code transformWithState} super-operator ({@code
 * BeamStatefulProcessorConfig.Mode#STATEFUL_PARDO}), so needs the same Kryo relaxation as {@code
 * BeamStatefulProcessorTest}.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StatefulParDoStreamingTest implements Serializable {

  /**
   * Runs with the module default of {@code spark.kryo.registrationRequired=true}, see {@code
   * BeamStatefulProcessorTest}.
   */
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);
  private static final String SENTINEL = "TIMER-FIRED";

  /**
   * Dedups repeated keys with a {@code ValueState<Boolean>} and, on the first sighting of a key,
   * arms an event-time timer thirty seconds out; once that timer fires it emits {@link #SENTINEL}.
   */
  private static class DedupWithExpiryFn extends DoFn<KV<String, String>, String> {
    @StateId("seen")
    private final StateSpec<ValueState<Boolean>> seenSpec = StateSpecs.value();

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, String> element,
        @Timestamp Instant timestamp,
        @StateId("seen") ValueState<Boolean> seen,
        @TimerId("expiry") Timer expiryTimer,
        OutputReceiver<String> out) {
      Boolean alreadySeen = seen.read();
      if (alreadySeen == null) {
        expiryTimer.set(timestamp.plus(Duration.standardSeconds(30)));
      }
      if (alreadySeen == null || !alreadySeen) {
        seen.write(true);
        out.output(element.getValue());
      }
    }

    @OnTimer("expiry")
    public void onExpiry(OutputReceiver<String> out) {
      out.output(SENTINEL);
    }
  }

  @Test(timeout = 300_000)
  public void dedupsRepeatedKeysAndFiresTimerSentinel() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("stateful-pardo-dedup");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<String>> elements = new ArrayList<>();
    elements.add(TimestampedValue.of("a", BASE));
    // Duplicate key "a": the dedup state must suppress this one.
    elements.add(TimestampedValue.of("a", BASE.plus(Duration.standardSeconds(1))));
    elements.add(TimestampedValue.of("b", BASE.plus(Duration.standardSeconds(2))));
    // Watermark rule: push well past the 30s timer deadline armed for key "a" (and "b").
    elements.add(TimestampedValue.of("c", BASE.plus(Duration.standardSeconds(90))));

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(elements, StringUtf8Coder.of())))
        .apply("WithKeys", WithKeys.<String, String>of(value -> value))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("DedupWithExpiry", ParDo.of(new DedupWithExpiryFn()))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // One "a" (the second sighting is suppressed by the dedup state), one "b", one "c", plus two
    // SENTINELs: the timers armed at 0s+30s for "a" and at 2s+30s for "b" both expire under the
    // final watermark of 90s, while "c"'s own timer at 90s+30s = 120s never does. The sentinels
    // arrive one micro-batch after the batch carrying "c", per the timer latency floor documented
    // on StreamingTestUtils.
    List<String> collected = new ArrayList<>(StreamingTestUtils.<String>getCollected(collectorId));
    Collections.sort(collected);
    assertEquals(
        "pipeline state=" + result.getState(),
        "[" + SENTINEL + ", " + SENTINEL + ", a, b, c]",
        collected.toString());
  }
}
