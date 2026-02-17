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
package org.apache.beam.runners.spark.translation.streaming;

import static org.apache.beam.runners.spark.translation.streaming.CreateStreamTest.streamingOptions;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
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
import org.apache.beam.sdk.testing.UsesProcessingTimeTimers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings({"unchecked", "unused"})
public class StatefulStreamingParDoEvaluatorTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(streamingOptions());

  private PTransform<PBegin, PCollection<KV<Integer, Integer>>> createStreamingSource(
      Pipeline pipeline) {
    Instant instant = new Instant(0);
    final KvCoder<Integer, Integer> coder = KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
    final Duration batchDuration = batchDuration(pipeline);
    return CreateStream.of(coder, batchDuration)
        .emptyBatch()
        .advanceWatermarkForNextBatch(instant)
        .nextBatch(
            TimestampedValue.of(KV.of(1, 1), instant),
            TimestampedValue.of(KV.of(1, 2), instant),
            TimestampedValue.of(KV.of(1, 3), instant))
        .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(1L)))
        .nextBatch(
            TimestampedValue.of(KV.of(2, 4), instant.plus(Duration.standardSeconds(1L))),
            TimestampedValue.of(KV.of(2, 5), instant.plus(Duration.standardSeconds(1L))),
            TimestampedValue.of(KV.of(2, 6), instant.plus(Duration.standardSeconds(1L))))
        .advanceNextBatchWatermarkToInfinity();
  }

  private PTransform<PBegin, PCollection<KV<Integer, Integer>>> createStreamingSource(
      Pipeline pipeline, int iterCount) {
    Instant instant = new Instant(0);
    final KvCoder<Integer, Integer> coder = KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
    final Duration batchDuration = batchDuration(pipeline);

    CreateStream<KV<Integer, Integer>> createStream =
        CreateStream.of(coder, batchDuration).emptyBatch().advanceWatermarkForNextBatch(instant);

    int value = 1;
    for (int i = 0; i < iterCount; i++) {
      createStream =
          createStream.nextBatch(
              TimestampedValue.of(KV.of(1, value++), instant),
              TimestampedValue.of(KV.of(1, value++), instant),
              TimestampedValue.of(KV.of(1, value++), instant));

      instant = instant.plus(Duration.standardSeconds(1L));
      createStream = createStream.advanceWatermarkForNextBatch(instant);

      createStream =
          createStream.nextBatch(
              TimestampedValue.of(KV.of(2, value++), instant),
              TimestampedValue.of(KV.of(2, value++), instant),
              TimestampedValue.of(KV.of(2, value++), instant));

      instant = instant.plus(Duration.standardSeconds(1L));
      createStream = createStream.advanceWatermarkForNextBatch(instant);
    }

    return createStream.advanceNextBatchWatermarkToInfinity();
  }

  private abstract static class AbstractStatefulWithTimer<KeyT, ValueT, OutputT>
      extends DoFn<KV<KeyT, ValueT>, OutputT> {
    @StateId("current-key")
    private final StateSpec<ValueState<KeyT>> currentKeyStateSpec;

    @StateId("timer-set")
    private final StateSpec<ValueState<Boolean>> timerSetStateSpec = StateSpecs.value();

    @TimerId("some-timer")
    private final TimerSpec someTimerSpec;

    private @Nullable OutputT shouldBeOutput;

    private AbstractStatefulWithTimer(
        TimeDomain timeDomain, Coder<KeyT> keyCoder, @Nullable OutputT shouldBeOutput) {
      this.someTimerSpec = TimerSpecs.timer(timeDomain);
      this.currentKeyStateSpec = StateSpecs.value(keyCoder);
      this.shouldBeOutput = shouldBeOutput;
    }

    @ProcessElement
    public void process(
        @Element KV<KeyT, ValueT> element,
        @StateId("timer-set") ValueState<Boolean> timerSetState,
        @StateId("current-key") ValueState<KeyT> currentKeyState,
        @TimerId("some-timer") Timer someTimer) {
      @Nullable KeyT currentKey = currentKeyState.read();
      if (currentKey == null) {
        currentKeyState.write(element.getKey());
      }

      final boolean isTimerSet = firstNonNull(timerSetState.read(), false);

      if (!isTimerSet) {
        timerSetState.write(true);
        someTimer.offset(Duration.millis(500L)).setRelative();
      }
    }

    @OnTimer("some-timer")
    public void onTimer(
        OnTimerContext context,
        @StateId("timer-set") ValueState<Boolean> timerSetState,
        @StateId("current-key") ValueState<KeyT> currentKeyState,
        @TimerId("some-timer") Timer timer) {
      if (this.shouldBeOutput != null) {
        context.output(shouldBeOutput);
      }
      assertNotNull(timerSetState.read());
      assertTrue(timerSetState.read());
    }
  }

  private static class StatefulWithEventTimeTimerDoFn<KeyT, ValueT>
      extends AbstractStatefulWithTimer<KeyT, ValueT, Void> {

    private StatefulWithEventTimeTimerDoFn(Coder<KeyT> keyCoder) {
      super(TimeDomain.EVENT_TIME, keyCoder, null);
    }
  }

  private static class StatefulWithProcessingTimeTimerDoFn<KeyT, ValueT, OutputT>
      extends AbstractStatefulWithTimer<KeyT, ValueT, OutputT> {

    private StatefulWithProcessingTimeTimerDoFn(
        Coder<KeyT> keyCoder, @Nullable OutputT shouldBeOutput) {
      super(TimeDomain.PROCESSING_TIME, keyCoder, shouldBeOutput);
    }
  }

  private static class StatefulWithProcessingTimeTimerForWithValidateSparseKey<
          KeyT, ValueT, OutputT>
      extends AbstractStatefulWithTimer<KeyT, ValueT, OutputT> {

    private StatefulWithProcessingTimeTimerForWithValidateSparseKey(Coder<KeyT> keyCoder) {
      super(TimeDomain.PROCESSING_TIME, keyCoder, null);
    }

    @Override
    public void onTimer(
        OnTimerContext context,
        ValueState<Boolean> timerSetState,
        ValueState<KeyT> currentKeyState,
        Timer timer) {
      final KeyT currentKey = currentKeyState.read();
      context.output((OutputT) currentKey);
    }
  }

  private static class StatefulDoFn extends DoFn<KV<Integer, Integer>, KV<Integer, Integer>> {

    @StateId("test-state")
    private final StateSpec<ValueState<Integer>> testState = StateSpecs.value();

    @ProcessElement
    public void process(
        @Element KV<Integer, Integer> element,
        @StateId("test-state") ValueState<Integer> testState,
        OutputReceiver<KV<Integer, Integer>> output) {
      final Integer value = element.getValue();
      final Integer currentState = firstNonNull(testState.read(), 0);
      final Integer newState = currentState + value;
      testState.write(newState);

      final KV<Integer, Integer> result = KV.of(element.getKey(), newState);
      output.output(result);
    }
  }

  @Category(StreamingTest.class)
  @Test
  public void shouldRejectEventTimeTimer() {
    p.apply(createStreamingSource(p))
        .apply(ParDo.of(new StatefulWithEventTimeTimerDoFn<>(VarIntCoder.of())));

    final IllegalStateException exception = assertThrows(IllegalStateException.class, p::run);

    assertEquals(
        TimeDomain.EVENT_TIME
            + " not yet supported in streaming mode: "
            + StatefulWithEventTimeTimerDoFn.class.getName(),
        exception.getMessage());
  }

  @Category({StreamingTest.class, UsesProcessingTimeTimers.class})
  @Test
  public void shouldTriggerProcessingTimeTimer() {
    final String shouldBeOutput = "some-result";
    final PCollection<String> result =
        p.apply(createStreamingSource(p))
            .apply(
                ParDo.of(
                    new StatefulWithProcessingTimeTimerDoFn<>(VarIntCoder.of(), shouldBeOutput)))
            .setCoder(StringUtf8Coder.of());

    PAssert.that(result)
        .satisfies(
            (Iterable<String> iter) -> {
              assertFalse(Iterables.isEmpty(iter));
              for (String timerOutput : iter) {
                assertEquals(shouldBeOutput, timerOutput);
              }
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Category({StreamingTest.class, UsesProcessingTimeTimers.class})
  @Test
  public void shouldTriggerProcessingTimeTimerWithSparseKey() {
    final int sparseKey = 3;
    KvCoder<Integer, Integer> coder = KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
    Instant instant = new Instant(0);
    CreateStream<KV<Integer, Integer>> sparseStream =
        CreateStream.of(coder, batchDuration(p)).advanceWatermarkForNextBatch(instant);

    sparseStream =
        sparseStream.nextBatch(
            TimestampedValue.of(KV.of(sparseKey, 0), instant.plus(Duration.millis(1000L))));

    final PCollection<KV<Integer, Integer>> sparsePCollection =
        p.apply("Create Sparse Key Stream", sparseStream);

    final PCollection<KV<Integer, Integer>> nonSparsePCollection =
        p.apply("Create Non Sparse Key Stream", createStreamingSource(p, 3));

    final PCollection<Integer> result =
        sparsePCollection
            .apply(Flatten.with(nonSparsePCollection))
            .apply(
                ParDo.of(
                    new StatefulWithProcessingTimeTimerForWithValidateSparseKey<
                        /*KeyT*/ Integer, /*ValueT*/ Integer, /*OutputT*/ Integer>(
                        VarIntCoder.of())))
            .setCoder(VarIntCoder.of());

    PAssert.that(result).containsInAnyOrder(1, 2, sparseKey);

    p.run().waitUntilFinish();
  }

  @Category(StreamingTest.class)
  @Test
  public void shouldProcessGlobalWidowStatefulParDo() {
    final PCollection<KV<Integer, Integer>> result =
        p.apply(createStreamingSource(p)).apply(ParDo.of(new StatefulDoFn()));

    PAssert.that(result)
        .containsInAnyOrder(
            // key 1
            KV.of(1, 1), // 1
            KV.of(1, 3), // 1 + 2
            KV.of(1, 6), // 3 + 3
            // key 2
            KV.of(2, 4), // 4
            KV.of(2, 9), // 4 + 5
            KV.of(2, 15)); // 9 + 6

    p.run().waitUntilFinish();
  }

  @Category(StreamingTest.class)
  @Test
  public void shouldProcessWindowedStatefulParDo() {
    final PCollection<KV<Integer, Integer>> result =
        p.apply(createStreamingSource(p, 2))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
            .apply(ParDo.of(new StatefulDoFn()));

    PAssert.that(result)
        .containsInAnyOrder(
            // Windowed Key 1
            KV.of(1, 1), // 1
            KV.of(1, 3), // 1 + 2
            KV.of(1, 6), // 3 + 3

            // Windowed Key 2
            KV.of(2, 4), // 4
            KV.of(2, 9), // 4 + 5
            KV.of(2, 15), // 9 + 6

            // Windowed Key 1
            KV.of(1, 7), // 7
            KV.of(1, 15), // 7 + 8
            KV.of(1, 24), // 15 + 9

            // Windowed Key 2
            KV.of(2, 10), // 10
            KV.of(2, 21), // 10 + 11
            KV.of(2, 33) // 21 + 12
            );

    p.run().waitUntilFinish();
  }

  private Duration batchDuration(Pipeline pipeline) {
    return Duration.millis(
        pipeline.getOptions().as(SparkPipelineOptions.class).getBatchIntervalMillis());
  }
}
