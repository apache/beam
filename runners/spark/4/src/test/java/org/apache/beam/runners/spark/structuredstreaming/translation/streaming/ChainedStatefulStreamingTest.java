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
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
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
 * A dedup stateful {@code ParDo} feeding a windowed {@code GroupByKey} sum: two {@code
 * transformWithState} operators chained in a single query.
 *
 * <p><b>This is the most important test in the suite.</b> It is the one POC scenario that actually
 * exercises cross-operator watermark propagation, the key claim of the whole Phase 1-4 plan: that a
 * Spark 4 micro-batch's watermark, computed once at the source, keeps meaning the same thing as it
 * flows through a chain of independently-hosted stateful operators, so a downstream window can
 * still correctly decide when it has seen everything it is going to see. Everything else in this
 * package tests one operator at a time; this one tests that they compose.
 *
 * <p>Needs the same Kryo relaxation as {@code BeamStatefulProcessorTest}, for the same reason.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class ChainedStatefulStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);
  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);

  /**
   * Dedups by the outer {@code id} key (simulating at-least-once redelivery of the same logical
   * event) and, on the first sighting of an id, passes the inner {@code KV<groupKey, value>}
   * through for the downstream windowed sum.
   */
  private static class DedupByIdFn extends DoFn<KV<String, KV<String, Long>>, KV<String, Long>> {
    @StateId("seen")
    private final StateSpec<ValueState<Boolean>> seenSpec = StateSpecs.value();

    @ProcessElement
    public void process(
        @Element KV<String, KV<String, Long>> element,
        @StateId("seen") ValueState<Boolean> seen,
        OutputReceiver<KV<String, Long>> out) {
      Boolean alreadySeen = seen.read();
      if (alreadySeen == null || !alreadySeen) {
        seen.write(true);
        out.output(element.getValue());
      }
    }
  }

  @Test(timeout = 300_000)
  public void dedupThenWindowedSumPropagatesWatermarkAcrossOperators() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("chained-stateful");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<KV<String, KV<String, Long>>>> elements = new ArrayList<>();
    // id "1" reported twice (redelivery), must count towards key "a" only once.
    elements.add(TimestampedValue.of(KV.of("1", KV.of("a", 5L)), BASE));
    elements.add(
        TimestampedValue.of(KV.of("1", KV.of("a", 5L)), BASE.plus(Duration.standardSeconds(1))));
    elements.add(
        TimestampedValue.of(KV.of("2", KV.of("a", 3L)), BASE.plus(Duration.standardSeconds(2))));
    elements.add(
        TimestampedValue.of(KV.of("3", KV.of("b", 10L)), BASE.plus(Duration.standardSeconds(3))));
    // Watermark rule: a much later element so the watermark passes the first window's end at both
    // the dedup operator and the downstream windowed sum operator.
    elements.add(
        TimestampedValue.of(
            KV.of("sentinel", KV.of("sentinel", 0L)), BASE.plus(Duration.standardSeconds(60))));

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    elements,
                    KvCoder.of(
                        StringUtf8Coder.of(),
                        KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))))
        .apply("DedupById", ParDo.of(new DedupByIdFn()))
        .apply("FixedWindows", Window.into(FixedWindows.of(WINDOW_SIZE)))
        .apply("SumPerKey", Sum.longsPerKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // a=8 is 5 + 3, the redelivered id "1" excluded by the upstream dedup operator; b=10 is the
    // single "3" element. Both are the [0s, 10s) window firing in the downstream operator, which
    // only happens if the watermark computed once at the source still reaches the second stateful
    // operator intact. a=13 would mean the dedup state was lost, and an empty result would mean the
    // watermark got stuck between the two operators; getting exactly [a=8, b=10] is the observable
    // proof that neither happened. The sentinel's own [60s, 70s) window never fires, nothing
    // arrives after it to push the watermark past 70s.
    List<String> collected = new ArrayList<>();
    for (KV<String, Long> kv : StreamingTestUtils.<KV<String, Long>>getCollected(collectorId)) {
      collected.add(kv.getKey() + "=" + kv.getValue());
    }
    Collections.sort(collected);
    assertEquals("pipeline state=" + result.getState(), "[a=8, b=10]", collected.toString());
  }
}
