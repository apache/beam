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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.apache.spark.sql.streaming.StateOperatorProgress;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * The chained stateful pipeline, asserted against Spark's own {@link StreamingQueryProgress} rather
 * than only against the pipeline's output.
 *
 * <p>{@link ChainedStatefulStreamingTest} asserts <em>what</em> the chained pipeline computes. This
 * one asserts <em>how</em> Spark ran it, which is the part of the POC claim a correct output alone
 * does not evidence:
 *
 * <ol>
 *   <li>Two distinct {@code transformWithState} operators live inside one single Spark streaming
 *       query, rather than the pipeline being cut into two queries that would each carry their own
 *       independent watermark.
 *   <li>The event time watermark of that one query genuinely advances over successive
 *       micro-batches, instead of the whole input landing in one batch where every element is
 *       trivially on time.
 *   <li>A record whose window the watermark has already passed is excluded from the result rather
 *       than quietly folded into it, and it is excluded at the downstream windowed operator, after
 *       having passed cleanly through the upstream stateful one.
 * </ol>
 *
 * <p>Both tests print the raw per micro-batch progress they recorded. That printout is the evidence
 * the phase gate report quotes, so keep it printing.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class ChainedStatefulStreamingEvidenceTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final org.joda.time.Instant BASE = new org.joda.time.Instant(0);
  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);

  /** Spark's short name for the physical operator a {@code transformWithState} compiles down to. */
  private static final String TWS_OPERATOR_NAME = "transformWithStateExec";

  /** Records every {@link StreamingQueryProgress} a run produced, in order. */
  private static final class ProgressRecorder extends StreamingQueryListener {
    private final List<StreamingQueryProgress> progresses =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onQueryStarted(QueryStartedEvent event) {}

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
      progresses.add(event.progress());
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {}

    List<StreamingQueryProgress> snapshot() {
      synchronized (progresses) {
        return new ArrayList<>(progresses);
      }
    }
  }

  /**
   * Dedups by the outer {@code id} key and passes the inner {@code KV<groupKey, value>} on.
   * Deliberately identical to the one in {@link ChainedStatefulStreamingTest}, so both tests
   * describe the same pipeline.
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

  private static String render(String collectorId) {
    List<String> rendered = new ArrayList<>();
    for (KV<String, Long> kv : StreamingTestUtils.<KV<String, Long>>getCollected(collectorId)) {
      rendered.add(kv.getKey() + "=" + kv.getValue());
    }
    Collections.sort(rendered);
    return rendered.toString();
  }

  /** Prints one line per micro-batch: batch id, input rows, watermark, and each state operator. */
  private static void printProgress(String label, List<StreamingQueryProgress> progresses) {
    StringBuilder out = new StringBuilder();
    out.append(System.lineSeparator()).append("===== ").append(label).append(" =====");
    for (StreamingQueryProgress progress : progresses) {
      out.append(System.lineSeparator())
          .append("queryId=")
          .append(progress.id())
          .append(" batchId=")
          .append(progress.batchId())
          .append(" numInputRows=")
          .append(progress.numInputRows())
          .append(" eventTime=")
          .append(progress.eventTime());
      for (StateOperatorProgress operator : progress.stateOperators()) {
        out.append(System.lineSeparator())
            .append("    stateOperator name=")
            .append(operator.operatorName())
            .append(" numRowsTotal=")
            .append(operator.numRowsTotal())
            .append(" numRowsUpdated=")
            .append(operator.numRowsUpdated())
            .append(" numRowsRemoved=")
            .append(operator.numRowsRemoved())
            .append(" numRowsDroppedByWatermark=")
            .append(operator.numRowsDroppedByWatermark())
            .append(" numStateStoreInstances=")
            .append(operator.numStateStoreInstances());
      }
    }
    out.append(System.lineSeparator()).append("===== end ").append(label).append(" =====");
    // Deliberately System.out: this printout is an artefact the phase gate report quotes, and it
    // has to survive whatever log configuration the module happens to run with.
    System.out.println(out);
  }

  /** The event time watermark Spark used for a micro-batch, or null if it had none yet. */
  private static @Nullable Instant watermarkOf(StreamingQueryProgress progress) {
    String watermark = progress.eventTime().get("watermark");
    return watermark == null ? null : Instant.parse(watermark);
  }

  /** The distinct watermark values a run went through, in the order they first appeared. */
  private static List<Instant> distinctWatermarks(List<StreamingQueryProgress> progresses) {
    List<Instant> watermarks = new ArrayList<>();
    for (StreamingQueryProgress progress : progresses) {
      Instant watermark = watermarkOf(progress);
      if (watermark != null
          && (watermarks.isEmpty() || !watermark.equals(watermarks.get(watermarks.size() - 1)))) {
        watermarks.add(watermark);
      }
    }
    return watermarks;
  }

  /** Asserts every recorded progress belongs to one and the same streaming query. */
  private static void assertSingleQuery(List<StreamingQueryProgress> progresses) {
    Set<UUID> ids = new LinkedHashSet<>();
    for (StreamingQueryProgress progress : progresses) {
      ids.add(progress.id());
    }
    assertEquals(
        "expected the whole pipeline to run as one streaming query, saw " + ids, 1, ids.size());
  }

  private SparkStructuredStreamingPipelineOptions oneRecordPerSplitPerBatchOptions()
      throws Exception {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    // One record per split per micro-batch, so the watermark climbs in visible steps instead of
    // reaching its final value inside a single batch. Both tests here are about what happens
    // between micro-batches, which a single batch run cannot show at all.
    options.setMaxRecordsPerBatch(1L);
    return options;
  }

  private static Read.Unbounded<KV<String, KV<String, Long>>> readOf(
      List<TimestampedValue<KV<String, KV<String, Long>>>> elements) {
    return Read.from(
        new StreamingTestUtils.ListBackedUnboundedSource<>(
            elements,
            KvCoder.of(StringUtf8Coder.of(), KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))));
  }

  /**
   * The on-time chained pipeline, asserted against Spark's own view of the run: one query, two
   * {@code transformWithState} operators inside it, and a watermark that moves.
   */
  @Test
  public void twoStatefulOperatorsShareOneQueryAndItsAdvancingWatermark() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("evidence-chained");
    StreamingTestUtils.clear(collectorId);

    // Same data as ChainedStatefulStreamingTest: id "1" is redelivered and must count only once.
    List<TimestampedValue<KV<String, KV<String, Long>>>> elements = new ArrayList<>();
    elements.add(TimestampedValue.of(KV.of("1", KV.of("a", 5L)), BASE));
    elements.add(
        TimestampedValue.of(KV.of("1", KV.of("a", 5L)), BASE.plus(Duration.standardSeconds(1))));
    elements.add(
        TimestampedValue.of(KV.of("2", KV.of("a", 3L)), BASE.plus(Duration.standardSeconds(2))));
    elements.add(
        TimestampedValue.of(KV.of("3", KV.of("b", 10L)), BASE.plus(Duration.standardSeconds(3))));
    elements.add(
        TimestampedValue.of(
            KV.of("sentinel", KV.of("sentinel", 0L)), BASE.plus(Duration.standardSeconds(60))));

    Pipeline pipeline = Pipeline.create(oneRecordPerSplitPerBatchOptions());
    pipeline
        .apply("ReadUnbounded", readOf(elements))
        .apply("DedupById", ParDo.of(new DedupByIdFn()))
        .apply("FixedWindows", Window.into(FixedWindows.of(WINDOW_SIZE)))
        .apply("SumPerKey", Sum.longsPerKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    ProgressRecorder recorder = new ProgressRecorder();
    SESSION.getSession().streams().addListener(recorder);
    PipelineResult result;
    try {
      result = StreamingTestUtils.run(pipeline);
    } finally {
      SESSION.getSession().streams().removeListener(recorder);
    }

    List<StreamingQueryProgress> progresses = recorder.snapshot();
    printProgress("chained stateful, on time", progresses);
    List<Instant> watermarks = distinctWatermarks(progresses);
    System.out.println("distinct watermarks in order: " + watermarks);

    assertEquals("pipeline state=" + result.getState(), "[a=8, b=10]", render(collectorId));

    // (1) One query, and inside it two transformWithState operators reported together in the same
    // micro-batch. A progress record is scoped to exactly one query, so two entries in one record
    // cannot be two separate queries being conflated.
    assertSingleQuery(progresses);
    StreamingQueryProgress twoOperators = null;
    for (StreamingQueryProgress progress : progresses) {
      if (progress.stateOperators().length == 2) {
        twoOperators = progress;
        break;
      }
    }
    assertNotNull(
        "no micro-batch reported two state operators, see the printed progress above",
        twoOperators);
    for (StateOperatorProgress operator : twoOperators.stateOperators()) {
      assertEquals(TWS_OPERATOR_NAME, operator.operatorName());
    }

    // (2) The watermark of that one query advances over micro-batches, and never moves backwards.
    assertTrue(
        "expected the watermark to take at least three distinct values over the run, saw "
            + watermarks,
        watermarks.size() >= 3);
    for (int i = 1; i < watermarks.size(); i++) {
      assertTrue(
          "watermark moved backwards: " + watermarks,
          watermarks.get(i).isAfter(watermarks.get(i - 1)));
    }
  }

  /**
   * A record whose event time falls in a window the watermark has already passed is excluded from
   * that window's result. The tap between the two operators is what makes this a statement about
   * lateness rather than about the record having gone missing somewhere upstream: the very same
   * record is observed leaving the dedup operator and absent from the windowed sum.
   *
   * <p>Deterministic only because of the split and batch arithmetic spelled out below. This test
   * and {@code WindowedGroupByKeyStreamingTest#lateDataIsDropped} are the only two in the suite
   * that depend on it.
   */
  @Test
  public void lateRecordIsExcludedByTheDownstreamWindowNotLostUpstream() throws Exception {
    String tapId = StreamingTestUtils.newCollectorId("evidence-late-tap");
    String collectorId = StreamingTestUtils.newCollectorId("evidence-late");
    StreamingTestUtils.clear(tapId);
    StreamingTestUtils.clear(collectorId);

    // ListBackedUnboundedSource round robins, so split 0 gets indices 0 and 2, split 1 gets 1 and
    // 3. With one record per split per micro-batch that gives:
    //   batch 1 = {a@0s, z@60s}  start watermark -infinity, both on time, end watermark 60s
    //   batch 2 = {a@2s, z@90s}  start watermark 60s, so a@2s in window [0s, 10s) is already late
    //                            and is dropped, while the same batch's start watermark fires that
    //                            window with the single on-time element it holds
    //   batch 3 = {}             start watermark 90s, fires window [60s, 70s)
    List<TimestampedValue<KV<String, KV<String, Long>>>> elements = new ArrayList<>();
    elements.add(TimestampedValue.of(KV.of("1", KV.of("a", 5L)), BASE));
    elements.add(
        TimestampedValue.of(KV.of("s1", KV.of("z", 7L)), BASE.plus(Duration.standardSeconds(60))));
    elements.add(
        TimestampedValue.of(KV.of("2", KV.of("a", 3L)), BASE.plus(Duration.standardSeconds(2))));
    elements.add(
        TimestampedValue.of(KV.of("s2", KV.of("z", 11L)), BASE.plus(Duration.standardSeconds(90))));

    SparkStructuredStreamingPipelineOptions options = oneRecordPerSplitPerBatchOptions();
    assertEquals(
        "this test assumes a two split source, see the comment above",
        2,
        SESSION.getSession().sparkContext().defaultParallelism());

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("ReadUnbounded", readOf(elements))
        .apply("DedupById", ParDo.of(new DedupByIdFn()))
        // Tap between the two stateful operators: whatever this sees did leave operator one and
        // did reach operator two's input.
        .apply("TapAfterDedup", ParDo.of(new StreamingTestUtils.CollectDoFn<>(tapId)))
        .apply("FixedWindows", Window.into(FixedWindows.of(WINDOW_SIZE)))
        .apply("SumPerKey", Sum.longsPerKey())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    ProgressRecorder recorder = new ProgressRecorder();
    SESSION.getSession().streams().addListener(recorder);
    PipelineResult result;
    try {
      result = StreamingTestUtils.run(pipeline);
    } finally {
      SESSION.getSession().streams().removeListener(recorder);
    }

    printProgress("chained stateful, late record", recorder.snapshot());
    System.out.println("after dedup: " + render(tapId));
    System.out.println("windowed sums: " + render(collectorId));

    // The late record was emitted by the dedup operator, so it did reach the windowed operator.
    assertEquals(
        "the late record never made it past the dedup operator, so this test would prove nothing"
            + " about lateness",
        "[a=3, a=5, z=11, z=7]",
        render(tapId));

    // And it is still not in the result. a=5, never a=8: the a=3 that arrived after the watermark
    // had passed the end of [0s, 10s) was excluded rather than folded into the sum. z=7 is window
    // [60s, 70s); the z=11 in [90s, 100s) has nothing after it to push the watermark past its end,
    // so that window never fires.
    assertEquals("pipeline state=" + result.getState(), "[a=5, z=7]", render(collectorId));
  }
}
