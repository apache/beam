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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Execution tests for {@link StatefulParDoTranslatorBatch} / {@link StatefulDoFnGroupFunction}:
 * these run full pipelines, unlike {@link StatefulParDoTranslatorBatchTest} which only covers
 * dispatch and translation preconditions.
 */
@RunWith(JUnit4.class)
public class StatefulParDoExecutionTest implements Serializable {
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @BeforeClass
  public static void requireSortedGroupsApi() {
    Assume.assumeTrue(
        "Stateful ParDo requires Spark 3.4+", StatefulParDoTranslatorBatch.isSupported());
  }

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.fromOptions(SESSION.createPipelineOptions());

  /** {@link ValueState} accumulates per key: totals must be scoped to the key, not shared. */
  @Test
  public void testStatefulAccumulationPerKey() {
    PCollection<KV<String, Integer>> result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), sec(1)),
                    TimestampedValue.of(KV.of("a", 2), sec(2)),
                    TimestampedValue.of(KV.of("a", 3), sec(3)),
                    TimestampedValue.of(KV.of("b", 10), sec(1)),
                    TimestampedValue.of(KV.of("b", 20), sec(2))))
            .apply(ParDo.of(new RunningSumDoFn()));

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of("a", 1), KV.of("a", 3), KV.of("a", 6), KV.of("b", 10), KV.of("b", 30));
    pipeline.run();
  }

  /**
   * Many keys share the two shuffle partitions of the {@code local[2]} session, so several keys are
   * served in sequence by the same DoFn instance and {@code MutableStepContext}. Each key's sums
   * must be independent; any state bleeding between keys corrupts them.
   */
  @Test
  public void testStateIsolationAcrossManyKeysInOnePartition() {
    List<TimestampedValue<KV<String, Integer>>> input = new ArrayList<>();
    List<KV<String, Integer>> expected = new ArrayList<>();
    for (int i = 0; i < 60; i++) {
      String key = "key-" + i;
      input.add(TimestampedValue.of(KV.of(key, i), sec(1)));
      input.add(TimestampedValue.of(KV.of(key, 1000 + i), sec(2)));
      expected.add(KV.of(key, i));
      expected.add(KV.of(key, 1000 + 2 * i));
    }

    PCollection<KV<String, Integer>> result =
        pipeline.apply(Create.timestamped(input)).apply(ParDo.of(new RunningSumDoFn()));

    PAssert.that(result).containsInAnyOrder(expected);
    pipeline.run();
  }

  /**
   * Elements are created out of order but must reach a {@link DoFn.RequiresTimeSortedInput} DoFn in
   * ascending timestamp order. The DoFn appends each value to state and emits the sequence so far,
   * so the multiset of outputs pins the exact observation order.
   */
  @Test
  public void testRequiresTimeSortedInput() {
    PCollection<String> result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("k", 4), sec(4)),
                    TimestampedValue.of(KV.of("k", 1), sec(1)),
                    TimestampedValue.of(KV.of("k", 6), sec(6)),
                    TimestampedValue.of(KV.of("k", 3), sec(3)),
                    TimestampedValue.of(KV.of("k", 2), sec(2)),
                    TimestampedValue.of(KV.of("k", 5), sec(5))))
            .apply(ParDo.of(new TimeSortedSequenceDoFn()));

    PAssert.that(result)
        .containsInAnyOrder("1", "1,2", "1,2,3", "1,2,3,4", "1,2,3,4,5", "1,2,3,4,5,6");
    pipeline.run();
  }

  /** An event time timer set in {@code @ProcessElement} must fire its {@code @OnTimer}. */
  @Test
  public void testEventTimeTimerFires() {
    PCollection<String> result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("k", 1), sec(1)),
                    TimestampedValue.of(KV.of("k", 2), sec(2))))
            .apply(ParDo.of(new EventTimeTimerDoFn()));

    PAssert.that(result).containsInAnyOrder("elem-1", "elem-2", "timer-fired");
    pipeline.run();
  }

  /**
   * An {@code @OnTimer} that re-sets its own timer must see every iteration fire: draining a
   * snapshot of pending timers silently truncates such chains (the failure mode recorded for the
   * RDD based runner in https://issues.apache.org/jira/browse/BEAM-12712).
   */
  @Test
  public void testLoopingTimerFiresAllIterations() {
    PCollection<String> result =
        pipeline
            .apply(Create.timestamped(TimestampedValue.of(KV.of("k", 1), sec(1))))
            .apply(ParDo.of(new LoopingTimerDoFn()));

    PAssert.that(result).containsInAnyOrder("fire-1", "fire-2", "fire-3", "fire-4", "fire-5");
    pipeline.run();
  }

  /**
   * Timers fire while the bundle is still open: a buffer flushed by {@code @FinishBundle} must
   * contain the {@code @OnTimer} contribution. Running {@code finishBundle} before the timers
   * silently drops the timer's data.
   */
  @Test
  public void testFinishBundleFlushesTimerOutput() {
    PCollection<String> result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("k", 1), sec(1)),
                    TimestampedValue.of(KV.of("k", 2), sec(2))))
            .apply(ParDo.of(new BufferUntilFinishBundleDoFn()));

    PAssert.that(result).containsInAnyOrder("elem-1", "elem-2", "timer");
    pipeline.run();
  }

  /**
   * A stateful {@link DoFn} with additional (tagged) outputs: per element the running sum goes to
   * the main output while even values are also emitted to the additional output.
   */
  @Test
  public void testTaggedAdditionalOutput() {
    TupleTag<KV<String, Integer>> sums = new TupleTag<KV<String, Integer>>() {};
    TupleTag<Integer> evens = new TupleTag<Integer>() {};

    PCollectionTuple result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), sec(1)),
                    TimestampedValue.of(KV.of("a", 2), sec(2)),
                    TimestampedValue.of(KV.of("b", 4), sec(1))))
            .apply(
                ParDo.of(new RunningSumWithEvensDoFn(evens))
                    .withOutputTags(sums, TupleTagList.of(evens)));

    PAssert.that(result.get(sums)).containsInAnyOrder(KV.of("a", 1), KV.of("a", 3), KV.of("b", 4));
    PAssert.that(result.get(evens)).containsInAnyOrder(2, 4);
    pipeline.run();
  }

  private static Instant sec(long seconds) {
    return new Instant(seconds * 1000);
  }

  /** Emits the per key running sum for every element. */
  private static class RunningSumDoFn extends DoFn<KV<String, Integer>, KV<String, Integer>> {
    @StateId("sum")
    private final StateSpec<ValueState<Integer>> sumSpec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("sum") ValueState<Integer> sum) {
      Integer current = sum.read();
      int newSum = (current == null ? 0 : current) + c.element().getValue();
      sum.write(newSum);
      c.output(KV.of(c.element().getKey(), newSum));
    }
  }

  /** Emits the per key running sum to the main output and even values to {@code evens}. */
  private static class RunningSumWithEvensDoFn
      extends DoFn<KV<String, Integer>, KV<String, Integer>> {
    private final TupleTag<Integer> evens;

    @StateId("sum")
    private final StateSpec<ValueState<Integer>> sumSpec = StateSpecs.value(VarIntCoder.of());

    RunningSumWithEvensDoFn(TupleTag<Integer> evens) {
      this.evens = evens;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("sum") ValueState<Integer> sum) {
      Integer current = sum.read();
      int value = c.element().getValue();
      int newSum = (current == null ? 0 : current) + value;
      sum.write(newSum);
      c.output(KV.of(c.element().getKey(), newSum));
      if (value % 2 == 0) {
        c.output(evens, value);
      }
    }
  }

  /** Appends each value to state and emits the sequence observed so far. */
  private static class TimeSortedSequenceDoFn extends DoFn<KV<String, Integer>, String> {
    @StateId("seen")
    private final StateSpec<ValueState<String>> seenSpec = StateSpecs.value(StringUtf8Coder.of());

    @RequiresTimeSortedInput
    @ProcessElement
    public void processElement(ProcessContext c, @StateId("seen") ValueState<String> seen) {
      String previous = seen.read();
      String sequence =
          previous == null
              ? c.element().getValue().toString()
              : previous + "," + c.element().getValue();
      seen.write(sequence);
      c.output(sequence);
    }
  }

  /** Sets one event time timer (re-set by each element, so it fires once). */
  private static class EventTimeTimerDoFn extends DoFn<KV<String, Integer>, String> {
    @TimerId("timer")
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(ProcessContext c, @TimerId("timer") Timer timer) {
      c.output("elem-" + c.element().getValue());
      timer.set(c.timestamp().plus(Duration.standardSeconds(10)));
    }

    @OnTimer("timer")
    public void onTimer(OnTimerContext c) {
      c.output("timer-fired");
    }
  }

  /** A bounded looping timer: each firing re-sets the timer until five have fired. */
  private static class LoopingTimerDoFn extends DoFn<KV<String, Integer>, String> {
    @TimerId("loop")
    private final TimerSpec loopSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("fires")
    private final StateSpec<ValueState<Integer>> firesSpec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @TimerId("loop") Timer loop) {
      loop.set(c.timestamp().plus(Duration.standardSeconds(1)));
    }

    @OnTimer("loop")
    public void onTimer(
        OnTimerContext c,
        @TimerId("loop") Timer loop,
        @StateId("fires") ValueState<Integer> fires) {
      Integer current = fires.read();
      int fired = (current == null ? 0 : current) + 1;
      fires.write(fired);
      c.output("fire-" + fired);
      if (fired < 5) {
        loop.set(c.fireTimestamp().plus(Duration.standardSeconds(1)));
      }
    }
  }

  /**
   * Buffers in the instance across {@code @ProcessElement} and {@code @OnTimer} and only outputs
   * from {@code @FinishBundle}.
   */
  private static class BufferUntilFinishBundleDoFn extends DoFn<KV<String, Integer>, String> {
    @TimerId("flush")
    private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private transient List<String> buffer;

    @StartBundle
    public void startBundle() {
      buffer = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c, @TimerId("flush") Timer flush) {
      buffer.add("elem-" + c.element().getValue());
      flush.set(c.timestamp().plus(Duration.standardSeconds(10)));
    }

    @OnTimer("flush")
    public void onTimer() {
      buffer.add("timer");
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      for (String value : buffer) {
        c.output(value, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
      }
      buffer = new ArrayList<>();
    }
  }
}
