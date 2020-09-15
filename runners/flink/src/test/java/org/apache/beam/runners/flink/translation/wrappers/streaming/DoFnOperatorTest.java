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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.LRUMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PCollectionViewTesting;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

/** Tests for {@link DoFnOperator}. */
@RunWith(JUnit4.class)
public class DoFnOperatorTest {

  // views and windows for testing side inputs
  private static final long WINDOW_MSECS_1 = 100;
  private static final long WINDOW_MSECS_2 = 500;
  private PCollectionView<Iterable<String>> view1;
  private PCollectionView<Iterable<String>> view2;

  private int numStartBundleCalled = 0;

  @Before
  public void setUp() {
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    view1 =
        pc.apply(Window.into(FixedWindows.of(new Duration(WINDOW_MSECS_1))))
            .apply(View.asIterable());
    view2 =
        pc.apply(Window.into(FixedWindows.of(new Duration(WINDOW_MSECS_2))))
            .apply(View.asIterable());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleOutput() throws Exception {

    Coder<WindowedValue<String>> coder = WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<String, String> doFnOperator =
        new DoFnOperator<>(
            new IdentityDoFn<>(),
            "stepName",
            coder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            null,
            null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("Hello")));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow("Hello")));

    testHarness.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiOutputOutput() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> coder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> mainOutput = new TupleTag<>("main-output");
    TupleTag<String> additionalOutput1 = new TupleTag<>("output-1");
    TupleTag<String> additionalOutput2 = new TupleTag<>("output-2");
    ImmutableMap<TupleTag<?>, OutputTag<?>> tagsToOutputTags =
        ImmutableMap.<TupleTag<?>, OutputTag<?>>builder()
            .put(
                additionalOutput1,
                new OutputTag<WindowedValue<String>>(additionalOutput1.getId()) {})
            .put(
                additionalOutput2,
                new OutputTag<WindowedValue<String>>(additionalOutput2.getId()) {})
            .build();
    ImmutableMap<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders =
        ImmutableMap.<TupleTag<?>, Coder<WindowedValue<?>>>builder()
            .put(mainOutput, (Coder) coder)
            .put(additionalOutput1, coder)
            .put(additionalOutput2, coder)
            .build();
    ImmutableMap<TupleTag<?>, Integer> tagsToIds =
        ImmutableMap.<TupleTag<?>, Integer>builder()
            .put(mainOutput, 0)
            .put(additionalOutput1, 1)
            .put(additionalOutput2, 2)
            .build();

    DoFnOperator<String, String> doFnOperator =
        new DoFnOperator<>(
            new MultiOutputDoFn(additionalOutput1, additionalOutput2),
            "stepName",
            coder,
            Collections.emptyMap(),
            mainOutput,
            ImmutableList.of(additionalOutput1, additionalOutput2),
            new DoFnOperator.MultiOutputOutputManagerFactory(
                mainOutput, tagsToOutputTags, tagsToCoders, tagsToIds),
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            null,
            null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("one")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("two")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("hello")));

    assertThat(
        this.stripStreamRecord(testHarness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow("got: hello")));

    assertThat(
        this.stripStreamRecord(testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput1))),
        contains(
            WindowedValue.valueInGlobalWindow("extra: one"),
            WindowedValue.valueInGlobalWindow("got: hello")));

    assertThat(
        this.stripStreamRecord(testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput2))),
        contains(
            WindowedValue.valueInGlobalWindow("extra: two"),
            WindowedValue.valueInGlobalWindow("got: hello")));

    testHarness.close();
  }

  /**
   * This test specifically verifies that we correctly map Flink watermarks to Beam watermarks. In
   * Beam, a watermark {@code T} guarantees there will not be elements with a timestamp {@code < T}
   * in the future. In Flink, a watermark {@code T} guarantees there will not be elements with a
   * timestamp {@code <= T} in the future. We have to make sure to take this into account when
   * firing timers.
   *
   * <p>This not test the timer API in general or processing-time timers because there are generic
   * tests for this in {@code ParDoTest}.
   */
  @Test
  public void testWatermarkContract() throws Exception {

    final Instant timerTimestamp = new Instant(1000);
    final Instant timerOutputTimestamp = timerTimestamp.minus(1);
    final String eventTimeMessage = "Event timer fired: ";
    final String processingTimeMessage = "Processing timer fired";

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10_000)));

    final String eventTimerId = "eventTimer";
    final String eventTimerId2 = "eventTimer2";
    final String processingTimerId = "processingTimer";
    DoFn<Integer, String> fn =
        new DoFn<Integer, String>() {

          @TimerId(eventTimerId)
          private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @TimerId(eventTimerId2)
          private final TimerSpec eventTimer2 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @TimerId(processingTimerId)
          private final TimerSpec processingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

          @ProcessElement
          public void processElement(
              ProcessContext context,
              @TimerId(eventTimerId) Timer eventTimer,
              @TimerId(eventTimerId2) Timer eventTimerWithOutputTimestamp,
              @TimerId(processingTimerId) Timer processingTimer) {
            eventTimer.set(timerTimestamp);
            eventTimerWithOutputTimestamp
                .withOutputTimestamp(timerOutputTimestamp)
                .set(timerTimestamp);
            processingTimer.offset(Duration.millis(timerTimestamp.getMillis())).setRelative();
          }

          @OnTimer(eventTimerId)
          public void onEventTime(OnTimerContext context) {
            assertEquals(
                "Timer timestamp must match set timestamp.", timerTimestamp, context.timestamp());
            context.outputWithTimestamp(eventTimeMessage + eventTimerId, context.timestamp());
          }

          @OnTimer(eventTimerId2)
          public void onEventTime2(OnTimerContext context) {
            assertEquals(
                "Timer timestamp must match set timestamp.",
                timerTimestamp,
                context.fireTimestamp());
            context.output(eventTimeMessage + eventTimerId2);
          }

          @OnTimer(processingTimerId)
          public void onProcessingTime(OnTimerContext context) {
            assertEquals(
                // Timestamps in processing timer context are defined to be the input watermark
                // See SimpleDoFnRunner#onTimer
                "Timer timestamp must match current input watermark",
                timerTimestamp.plus(1),
                context.timestamp());
            context.outputWithTimestamp(processingTimeMessage, context.timestamp());
          }
        };

    VarIntCoder keyCoder = VarIntCoder.of();
    WindowedValue.FullWindowedValueCoder<Integer> inputCoder =
        WindowedValue.getFullCoder(keyCoder, windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<String> outputCoder =
        WindowedValue.getFullCoder(
            StringUtf8Coder.of(), windowingStrategy.getWindowFn().windowCoder());

    KeySelector<WindowedValue<Integer>, ByteBuffer> keySelector =
        e -> FlinkKeyUtils.encodeKey(e.getValue(), keyCoder);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<Integer, String> doFnOperator =
        new DoFnOperator<>(
            fn,
            "stepName",
            inputCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, outputCoder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            keyCoder, /* key coder */
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<String>> testHarness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            doFnOperator,
            keySelector,
            new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));

    testHarness.setup(new CoderTypeSerializer<>(outputCoder));

    testHarness.open();

    testHarness.processWatermark(0);
    testHarness.setProcessingTime(0);

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10_000));

    // this should register the two timers above
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(13, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(stripStreamRecordFromWindowedValue(testHarness.getOutput()), emptyIterable());

    // this does not yet fire the timers (in vanilla Flink it would)
    testHarness.processWatermark(timerTimestamp.getMillis());
    testHarness.setProcessingTime(timerTimestamp.getMillis());

    assertThat(stripStreamRecordFromWindowedValue(testHarness.getOutput()), emptyIterable());
    assertThat(
        doFnOperator.keyedStateInternals.minWatermarkHoldMs(),
        is(timerOutputTimestamp.getMillis()));

    // this must fire the event timers
    testHarness.processWatermark(timerTimestamp.getMillis() + 1);

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        containsInAnyOrder(
            WindowedValue.of(
                eventTimeMessage + eventTimerId, timerTimestamp, window1, PaneInfo.NO_FIRING),
            WindowedValue.of(
                eventTimeMessage + eventTimerId2,
                timerTimestamp.minus(1),
                window1,
                PaneInfo.NO_FIRING)));

    testHarness.getOutput().clear();

    // this must fire the processing timer
    testHarness.setProcessingTime(timerTimestamp.getMillis() + 1);

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                // Timestamps in processing timer context are defined to be the input watermark
                // See SimpleDoFnRunner#onTimer
                processingTimeMessage, timerTimestamp.plus(1), window1, PaneInfo.NO_FIRING)));

    testHarness.close();
  }

  @Test
  public void testLateDroppingForStatefulFn() throws Exception {

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10)));

    DoFn<Integer, String> fn =
        new DoFn<Integer, String>() {

          @StateId("state")
          private final StateSpec<ValueState<String>> stateSpec =
              StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void processElement(ProcessContext context) {
            context.output(context.element().toString());
          }
        };

    VarIntCoder keyCoder = VarIntCoder.of();
    Coder<WindowedValue<Integer>> inputCoder =
        WindowedValue.getFullCoder(keyCoder, windowingStrategy.getWindowFn().windowCoder());
    Coder<WindowedValue<String>> outputCoder =
        WindowedValue.getFullCoder(
            StringUtf8Coder.of(), windowingStrategy.getWindowFn().windowCoder());

    KeySelector<WindowedValue<Integer>, ByteBuffer> keySelector =
        e -> FlinkKeyUtils.encodeKey(e.getValue(), keyCoder);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<Integer, String> doFnOperator =
        new DoFnOperator<>(
            fn,
            "stepName",
            inputCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, outputCoder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            keyCoder, /* key coder */
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<String>> testHarness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            doFnOperator,
            keySelector,
            new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));

    testHarness.open();

    testHarness.processWatermark(0);

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10));

    // this should not be late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(13, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.of("13", new Instant(0), window1, PaneInfo.NO_FIRING)));

    testHarness.getOutput().clear();

    testHarness.processWatermark(9);

    // this should still not be considered late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(17, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.of("17", new Instant(0), window1, PaneInfo.NO_FIRING)));

    testHarness.getOutput().clear();

    testHarness.processWatermark(10);

    // this should now be considered late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(17, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(stripStreamRecordFromWindowedValue(testHarness.getOutput()), emptyIterable());

    testHarness.close();
  }

  @Test
  public void testStateGCForStatefulFn() throws Exception {

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10))).withAllowedLateness(Duration.ZERO);
    final int offset = 5000;
    final int timerOutput = 4093;

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>>
        testHarness =
            getHarness(
                windowingStrategy,
                offset,
                (window) -> new Instant(window.maxTimestamp()),
                timerOutput);

    testHarness.open();

    testHarness.processWatermark(0);

    assertEquals(0, testHarness.numKeyedStateEntries());

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10));

    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(KV.of("key1", 5), new Instant(1), window1, PaneInfo.NO_FIRING)));

    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(KV.of("key2", 7), new Instant(3), window1, PaneInfo.NO_FIRING)));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                KV.of("key1", 5 + offset), new Instant(1), window1, PaneInfo.NO_FIRING),
            WindowedValue.of(
                KV.of("key2", 7 + offset), new Instant(3), window1, PaneInfo.NO_FIRING)));

    // 2 entries for the elements and 2 for the pending timers
    assertEquals(4, testHarness.numKeyedStateEntries());

    testHarness.getOutput().clear();

    // this should trigger both the window.maxTimestamp() timer and the GC timer
    // this tests that the GC timer fires after the user timer
    // we have to add 1 here because Flink timers fire when watermark >= timestamp while Beam
    // timers fire when watermark > timestamp
    testHarness.processWatermark(
        window1
                .maxTimestamp()
                .plus(windowingStrategy.getAllowedLateness())
                .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
                .getMillis()
            + 1);

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                KV.of("key1", timerOutput), new Instant(9), window1, PaneInfo.NO_FIRING),
            WindowedValue.of(
                KV.of("key2", timerOutput), new Instant(9), window1, PaneInfo.NO_FIRING)));

    testHarness.close();
  }

  @Test
  public void testGCForGlobalWindow() throws Exception {
    WindowingStrategy<Object, GlobalWindow> windowingStrategy = WindowingStrategy.globalDefault();

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>>
        testHarness = getHarness(windowingStrategy, 5000, (window) -> new Instant(50), 4092);

    testHarness.open();

    testHarness.processWatermark(0);

    // ensure the state was garbage collected and the pending timers have been removed
    assertEquals(0, testHarness.numKeyedStateEntries());

    // Check global window cleanup via final watermark, _without_ cleanup timers
    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(
                KV.of("key1", 5), new Instant(23), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));
    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(
                KV.of("key2", 6), new Instant(42), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));

    // timers set by the transform
    assertThat(testHarness.numEventTimeTimers(), is(2));
    // state has been written which needs to be cleaned up (includes timers)
    assertThat(testHarness.numKeyedStateEntries(), is(4));

    // Fire timers set
    testHarness.processWatermark(51);
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(testHarness.numKeyedStateEntries(), is(2));

    // Should not trigger garbage collection yet
    testHarness.processWatermark(GlobalWindow.INSTANCE.maxTimestamp().plus(1).getMillis());
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(testHarness.numKeyedStateEntries(), is(2));

    // Cleanup due to end of global window
    testHarness.processWatermark(GlobalWindow.INSTANCE.maxTimestamp().plus(2).getMillis());
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(testHarness.numKeyedStateEntries(), is(0));

    // Any new state will also be cleaned up on close
    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(
                KV.of("key2", 6), new Instant(42), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));

    // Close sends Flink's max watermark and will cleanup again
    testHarness.close();
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(testHarness.numKeyedStateEntries(), is(0));
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>>
      getHarness(
          WindowingStrategy windowingStrategy,
          int elementOffset,
          Function<BoundedWindow, Instant> timerTimestamp,
          int timerOutput)
          throws Exception {
    final String timerId = "boo";
    final String stateId = "dazzle";

    DoFn<KV<String, Integer>, KV<String, Integer>> fn =
        new DoFn<KV<String, Integer>, KV<String, Integer>>() {

          @TimerId(timerId)
          private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @StateId(stateId)
          private final StateSpec<ValueState<String>> stateSpec =
              StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void processElement(
              ProcessContext context,
              @TimerId(timerId) Timer timer,
              @StateId(stateId) ValueState<String> state,
              BoundedWindow window) {
            timer.set(timerTimestamp.apply(window));
            state.write(context.element().getKey());
            context.output(
                KV.of(context.element().getKey(), context.element().getValue() + elementOffset));
          }

          @OnTimer(timerId)
          public void onTimer(OnTimerContext context, @StateId(stateId) ValueState<String> state) {
            context.output(KV.of(state.read(), timerOutput));
          }
        };

    WindowedValue.FullWindowedValueCoder<KV<String, Integer>> coder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            windowingStrategy.getWindowFn().windowCoder());

    TupleTag<KV<String, Integer>> outputTag = new TupleTag<>("main-output");

    KeySelector<WindowedValue<KV<String, Integer>>, ByteBuffer> keySelector =
        e -> FlinkKeyUtils.encodeKey(e.getValue().getKey(), StringUtf8Coder.of());

    DoFnOperator<KV<String, Integer>, KV<String, Integer>> doFnOperator =
        new DoFnOperator<>(
            fn,
            "stepName",
            coder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            StringUtf8Coder.of(), /* key coder */
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                doFnOperator,
                keySelector,
                new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));
    return testHarness;
  }

  @Test
  public void testNormalParDoSideInputs() throws Exception {
    testSideInputs(false);
  }

  @Test
  public void testKeyedParDoSideInputs() throws Exception {
    testSideInputs(true);
  }

  void testSideInputs(boolean keyed) throws Exception {

    Coder<WindowedValue<String>> coder = WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
        ImmutableMap.<Integer, PCollectionView<?>>builder().put(1, view1).put(2, view2).build();

    Coder<String> keyCoder = StringUtf8Coder.of();
    ;
    KeySelector<WindowedValue<String>, ByteBuffer> keySelector = null;
    if (keyed) {
      keySelector = value -> FlinkKeyUtils.encodeKey(value.getValue(), keyCoder);
    }

    DoFnOperator<String, String> doFnOperator =
        new DoFnOperator<>(
            new IdentityDoFn<>(),
            "stepName",
            coder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
            WindowingStrategy.of(FixedWindows.of(Duration.millis(100))),
            sideInputMapping, /* side-input mapping */
            ImmutableList.of(view1, view2), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            keyed ? keyCoder : null,
            keyed ? keySelector : null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    TwoInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue, WindowedValue<String>>
        testHarness = new TwoInputStreamOperatorTestHarness<>(doFnOperator);

    if (keyed) {
      // we use a dummy key for the second input since it is considered to be broadcast
      testHarness =
          new KeyedTwoInputStreamOperatorTestHarness<>(
              doFnOperator,
              keySelector,
              null,
              new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));
    }

    testHarness.open();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(500));

    // test the keep of sideInputs events
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view1.getPipeline().getOptions(), View.asIterable(), "hello", "ciao"),
                    new Instant(0),
                    firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view2.getPipeline().getOptions(), View.asIterable(), "foo", "bar"),
                    new Instant(0),
                    secondWindow))));

    // push in a regular elements
    WindowedValue<String> helloElement = valueInWindow("Hello", new Instant(0), firstWindow);
    WindowedValue<String> worldElement = valueInWindow("World", new Instant(1000), firstWindow);
    testHarness.processElement1(new StreamRecord<>(helloElement));
    testHarness.processElement1(new StreamRecord<>(worldElement));

    // test the keep of pushed-back events
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view1.getPipeline().getOptions(), View.asIterable(), "hello", "ciao"),
                    new Instant(1000),
                    firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view2.getPipeline().getOptions(), View.asIterable(), "foo", "bar"),
                    new Instant(1000),
                    secondWindow))));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(helloElement, worldElement));

    testHarness.close();
  }

  @Test
  public void testStateRestore() throws Exception {
    DoFn<KV<String, Long>, KV<String, Long>> filterElementsEqualToCountFn =
        new DoFn<KV<String, Long>, KV<String, Long>>() {

          @StateId("counter")
          private final StateSpec<ValueState<Long>> counterSpec =
              StateSpecs.value(VarLongCoder.of());

          @ProcessElement
          public void processElement(
              ProcessContext context, @StateId("counter") ValueState<Long> count) {
            long currentCount = Optional.ofNullable(count.read()).orElse(0L);
            currentCount = currentCount + 1;
            count.write(currentCount);

            KV<String, Long> currentElement = context.element();
            if (currentCount == currentElement.getValue()) {
              context.output(currentElement);
            }
          }
        };

    WindowingStrategy<Object, GlobalWindow> windowingStrategy = WindowingStrategy.globalDefault();

    TupleTag<KV<String, Long>> outputTag = new TupleTag<>("main-output");

    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    KvToByteBufferKeySelector keySelector = new KvToByteBufferKeySelector<>(keyCoder);
    KvCoder<String, Long> coder = KvCoder.of(keyCoder, VarLongCoder.of());

    FullWindowedValueCoder<KV<String, Long>> kvCoder =
        WindowedValue.getFullCoder(coder, windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<ByteBuffer> keyCoderInfo =
        new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of());

    OneInputStreamOperatorTestHarness<
            WindowedValue<KV<String, Long>>, WindowedValue<KV<String, Long>>>
        testHarness =
            createTestHarness(
                windowingStrategy,
                filterElementsEqualToCountFn,
                kvCoder,
                kvCoder,
                keyCoder,
                outputTag,
                keyCoderInfo,
                keySelector);
    testHarness.open();

    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 100L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 100L))));

    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
    testHarness.close();

    testHarness =
        createTestHarness(
            windowingStrategy,
            filterElementsEqualToCountFn,
            kvCoder,
            kvCoder,
            keyCoder,
            outputTag,
            keyCoderInfo,
            keySelector);
    testHarness.initializeState(snapshot);
    testHarness.open();

    // after restore: counter = 2
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 100L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 4L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 5L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("a", 100L))));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow(KV.of("a", 4L)),
            WindowedValue.valueInGlobalWindow(KV.of("a", 5L))));

    testHarness.close();
  }

  @Test
  public void nonKeyedParDoSideInputCheckpointing() throws Exception {
    sideInputCheckpointing(
        () -> {
          Coder<WindowedValue<String>> coder =
              WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
          TupleTag<String> outputTag = new TupleTag<>("main-output");

          ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
              ImmutableMap.<Integer, PCollectionView<?>>builder()
                  .put(1, view1)
                  .put(2, view2)
                  .build();

          DoFnOperator<String, String> doFnOperator =
              new DoFnOperator<>(
                  new IdentityDoFn<>(),
                  "stepName",
                  coder,
                  Collections.emptyMap(),
                  outputTag,
                  Collections.emptyList(),
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
                  WindowingStrategy.globalDefault(),
                  sideInputMapping, /* side-input mapping */
                  ImmutableList.of(view1, view2), /* side inputs */
                  PipelineOptionsFactory.as(FlinkPipelineOptions.class),
                  null,
                  null,
                  DoFnSchemaInformation.create(),
                  Collections.emptyMap());

          return new TwoInputStreamOperatorTestHarness<>(doFnOperator);
        });
  }

  @Test
  public void keyedParDoSideInputCheckpointing() throws Exception {
    sideInputCheckpointing(
        () -> {
          StringUtf8Coder keyCoder = StringUtf8Coder.of();
          Coder<WindowedValue<String>> coder =
              WindowedValue.getFullCoder(keyCoder, IntervalWindow.getCoder());
          TupleTag<String> outputTag = new TupleTag<>("main-output");

          KeySelector<WindowedValue<String>, ByteBuffer> keySelector =
              e -> FlinkKeyUtils.encodeKey(e.getValue(), keyCoder);

          ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
              ImmutableMap.<Integer, PCollectionView<?>>builder()
                  .put(1, view1)
                  .put(2, view2)
                  .build();

          DoFnOperator<String, String> doFnOperator =
              new DoFnOperator<>(
                  new IdentityDoFn<>(),
                  "stepName",
                  coder,
                  Collections.emptyMap(),
                  outputTag,
                  Collections.emptyList(),
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
                  WindowingStrategy.of(FixedWindows.of(Duration.millis(100))),
                  sideInputMapping, /* side-input mapping */
                  ImmutableList.of(view1, view2), /* side inputs */
                  PipelineOptionsFactory.as(FlinkPipelineOptions.class),
                  keyCoder,
                  keySelector,
                  DoFnSchemaInformation.create(),
                  Collections.emptyMap());

          return new KeyedTwoInputStreamOperatorTestHarness<>(
              doFnOperator,
              keySelector,
              // we use a dummy key for the second input since it is considered to be broadcast
              null,
              new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));
        });
  }

  void sideInputCheckpointing(
      TestHarnessFactory<
              TwoInputStreamOperatorTestHarness<
                  WindowedValue<String>, RawUnionValue, WindowedValue<String>>>
          harnessFactory)
      throws Exception {

    TwoInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue, WindowedValue<String>>
        testHarness = harnessFactory.create();

    testHarness.open();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(500));

    // push in some side inputs for both windows
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view1.getPipeline().getOptions(), View.asIterable(), "hello", "ciao"),
                    new Instant(0),
                    firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view2.getPipeline().getOptions(), View.asIterable(), "foo", "bar"),
                    new Instant(0),
                    secondWindow))));

    // snapshot state, throw away the operator, then restore and verify that we still match
    // main-input elements to the side-inputs that we sent earlier
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    testHarness = harnessFactory.create();

    testHarness.initializeState(snapshot);
    testHarness.open();

    // push in main-input elements
    WindowedValue<String> helloElement = valueInWindow("Hello", new Instant(0), firstWindow);
    WindowedValue<String> worldElement = valueInWindow("World", new Instant(1000), firstWindow);
    testHarness.processElement1(new StreamRecord<>(helloElement));
    testHarness.processElement1(new StreamRecord<>(worldElement));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(helloElement, worldElement));

    testHarness.close();
  }

  @Test
  public void nonKeyedParDoPushbackDataCheckpointing() throws Exception {
    pushbackDataCheckpointing(
        () -> {
          Coder<WindowedValue<String>> coder =
              WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

          TupleTag<String> outputTag = new TupleTag<>("main-output");

          ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
              ImmutableMap.<Integer, PCollectionView<?>>builder()
                  .put(1, view1)
                  .put(2, view2)
                  .build();

          DoFnOperator<String, String> doFnOperator =
              new DoFnOperator<>(
                  new IdentityDoFn<>(),
                  "stepName",
                  coder,
                  Collections.emptyMap(),
                  outputTag,
                  Collections.emptyList(),
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
                  WindowingStrategy.of(FixedWindows.of(Duration.millis(100))),
                  sideInputMapping, /* side-input mapping */
                  ImmutableList.of(view1, view2), /* side inputs */
                  PipelineOptionsFactory.as(FlinkPipelineOptions.class),
                  null,
                  null,
                  DoFnSchemaInformation.create(),
                  Collections.emptyMap());

          return new TwoInputStreamOperatorTestHarness<>(doFnOperator);
        });
  }

  @Test
  public void keyedParDoPushbackDataCheckpointing() throws Exception {
    pushbackDataCheckpointing(
        () -> {
          StringUtf8Coder keyCoder = StringUtf8Coder.of();
          Coder<WindowedValue<String>> coder =
              WindowedValue.getFullCoder(keyCoder, IntervalWindow.getCoder());

          TupleTag<String> outputTag = new TupleTag<>("main-output");

          KeySelector<WindowedValue<String>, ByteBuffer> keySelector =
              e -> FlinkKeyUtils.encodeKey(e.getValue(), keyCoder);

          ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
              ImmutableMap.<Integer, PCollectionView<?>>builder()
                  .put(1, view1)
                  .put(2, view2)
                  .build();

          DoFnOperator<String, String> doFnOperator =
              new DoFnOperator<>(
                  new IdentityDoFn<>(),
                  "stepName",
                  coder,
                  Collections.emptyMap(),
                  outputTag,
                  Collections.emptyList(),
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, coder),
                  WindowingStrategy.of(FixedWindows.of(Duration.millis(100))),
                  sideInputMapping, /* side-input mapping */
                  ImmutableList.of(view1, view2), /* side inputs */
                  PipelineOptionsFactory.as(FlinkPipelineOptions.class),
                  keyCoder,
                  keySelector,
                  DoFnSchemaInformation.create(),
                  Collections.emptyMap());

          return new KeyedTwoInputStreamOperatorTestHarness<>(
              doFnOperator,
              keySelector,
              // we use a dummy key for the second input since it is considered to be broadcast
              null,
              new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));
        });
  }

  void pushbackDataCheckpointing(
      TestHarnessFactory<
              TwoInputStreamOperatorTestHarness<
                  WindowedValue<String>, RawUnionValue, WindowedValue<String>>>
          harnessFactory)
      throws Exception {

    TwoInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue, WindowedValue<String>>
        testHarness = harnessFactory.create();

    testHarness.open();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(500));

    // push in main-input elements
    WindowedValue<String> helloElement = valueInWindow("Hello", new Instant(0), firstWindow);
    WindowedValue<String> worldElement = valueInWindow("World", new Instant(1000), firstWindow);
    testHarness.processElement1(new StreamRecord<>(helloElement));
    testHarness.processElement1(new StreamRecord<>(worldElement));

    // snapshot state, throw away the operator, then restore and verify that we still match
    // main-input elements to the side-inputs that we sent earlier
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    testHarness = harnessFactory.create();

    testHarness.initializeState(snapshot);
    testHarness.open();

    // push in some side inputs for both windows
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view1.getPipeline().getOptions(), View.asIterable(), "hello", "ciao"),
                    new Instant(0),
                    firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(
                    PCollectionViewTesting.materializeValuesFor(
                        view2.getPipeline().getOptions(), View.asIterable(), "foo", "bar"),
                    new Instant(0),
                    secondWindow))));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        containsInAnyOrder(helloElement, worldElement));

    testHarness.close();
  }

  @Test
  public void testTimersRestore() throws Exception {
    final Instant timerTimestamp = new Instant(1000);
    final String outputMessage = "Timer fired";

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10_000)));

    DoFn<Integer, String> fn =
        new DoFn<Integer, String>() {
          private static final String EVENT_TIMER_ID = "eventTimer";

          @TimerId(EVENT_TIMER_ID)
          private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void processElement(ProcessContext context, @TimerId(EVENT_TIMER_ID) Timer timer) {
            timer.set(timerTimestamp);
          }

          @OnTimer(EVENT_TIMER_ID)
          public void onEventTime(OnTimerContext context) {
            assertEquals(
                "Timer timestamp must match set timestamp.", timerTimestamp, context.timestamp());
            context.outputWithTimestamp(outputMessage, context.timestamp());
          }
        };

    VarIntCoder keyCoder = VarIntCoder.of();
    WindowedValue.FullWindowedValueCoder<Integer> inputCoder =
        WindowedValue.getFullCoder(keyCoder, windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<String> outputCoder =
        WindowedValue.getFullCoder(
            StringUtf8Coder.of(), windowingStrategy.getWindowFn().windowCoder());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    final CoderTypeSerializer<WindowedValue<String>> outputSerializer =
        new CoderTypeSerializer<>(outputCoder);
    CoderTypeInformation<ByteBuffer> keyCoderInfo =
        new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of());
    KeySelector<WindowedValue<Integer>, ByteBuffer> keySelector =
        e -> FlinkKeyUtils.encodeKey(e.getValue(), keyCoder);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<String>> testHarness =
        createTestHarness(
            windowingStrategy,
            fn,
            inputCoder,
            outputCoder,
            keyCoder,
            outputTag,
            keyCoderInfo,
            keySelector);

    testHarness.setup(outputSerializer);

    testHarness.open();

    testHarness.processWatermark(0);

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10_000));

    // this should register a timer
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(13, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(stripStreamRecordFromWindowedValue(testHarness.getOutput()), emptyIterable());

    // snapshot and restore
    final OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
    testHarness.close();

    testHarness =
        createTestHarness(
            windowingStrategy,
            fn,
            inputCoder,
            outputCoder,
            VarIntCoder.of(),
            outputTag,
            keyCoderInfo,
            keySelector);
    testHarness.setup(outputSerializer);
    testHarness.initializeState(snapshot);
    testHarness.open();

    // this must fire the timer
    testHarness.processWatermark(timerTimestamp.getMillis() + 1);

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                outputMessage, new Instant(timerTimestamp), window1, PaneInfo.NO_FIRING)));

    testHarness.close();
  }

  private <K, InT, OutT>
      OneInputStreamOperatorTestHarness<WindowedValue<InT>, WindowedValue<OutT>> createTestHarness(
          WindowingStrategy<Object, ?> windowingStrategy,
          DoFn<InT, OutT> fn,
          FullWindowedValueCoder<InT> inputCoder,
          FullWindowedValueCoder<OutT> outputCoder,
          Coder<?> keyCoder,
          TupleTag<OutT> outputTag,
          TypeInformation<K> keyCoderInfo,
          KeySelector<WindowedValue<InT>, K> keySelector)
          throws Exception {
    DoFnOperator<InT, OutT> doFnOperator =
        new DoFnOperator<>(
            fn,
            "stepName",
            inputCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(outputTag, outputCoder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            keyCoder /* key coder */,
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    return new KeyedOneInputStreamOperatorTestHarness<>(doFnOperator, keySelector, keyCoderInfo);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBundle() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> outputTag = new TupleTag<>("main-output");
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(2L);
    options.setMaxBundleTimeMills(10L);

    IdentityDoFn<String> doFn =
        new IdentityDoFn<String>() {
          @FinishBundle
          public void finishBundle(FinishBundleContext context) {
            context.output(
                "finishBundle", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    DoFnOperator<String, String> doFnOperator =
        new DoFnOperator<>(
            doFn,
            "stepName",
            windowedValueCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            options,
            null,
            null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("a")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("b")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("c")));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("a"),
            WindowedValue.valueInGlobalWindow("b"),
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("c")));

    // draw a snapshot
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    // Finish bundle element will be buffered as part of finishing a bundle in snapshot()
    PushedBackElementsHandler<KV<Integer, WindowedValue<?>>> pushedBackElementsHandler =
        doFnOperator.outputManager.pushedBackElementsHandler;
    assertThat(pushedBackElementsHandler, instanceOf(NonKeyedPushedBackElementsHandler.class));
    List<KV<Integer, WindowedValue<?>>> bufferedElements =
        pushedBackElementsHandler.getElements().collect(Collectors.toList());
    assertThat(
        bufferedElements, contains(KV.of(0, WindowedValue.valueInGlobalWindow("finishBundle"))));

    testHarness.close();

    DoFnOperator<String, String> newDoFnOperator =
        new DoFnOperator<>(
            doFn,
            "stepName",
            windowedValueCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            options,
            null,
            null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> newHarness =
        new OneInputStreamOperatorTestHarness<>(newDoFnOperator);

    // restore snapshot
    newHarness.initializeState(snapshot);

    newHarness.open();

    // startBundle will output the buffered elements.
    newHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("d")));

    // check finishBundle by timeout
    newHarness.setProcessingTime(10);

    assertThat(
        stripStreamRecordFromWindowedValue(newHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("d"),
            WindowedValue.valueInGlobalWindow("finishBundle")));

    // No bundle will be created when sending the MAX watermark
    // (unless pushed back items are emitted)
    newHarness.close();

    assertThat(
        stripStreamRecordFromWindowedValue(newHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("d"),
            WindowedValue.valueInGlobalWindow("finishBundle")));

    // close() will also call dispose(), but call again to verify no new bundle
    // is created afterwards
    newDoFnOperator.dispose();

    assertThat(
        stripStreamRecordFromWindowedValue(newHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("d"),
            WindowedValue.valueInGlobalWindow("finishBundle")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBundleKeyed() throws Exception {

    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    KvToByteBufferKeySelector keySelector = new KvToByteBufferKeySelector<>(keyCoder);
    KvCoder<String, String> kvCoder = KvCoder.of(keyCoder, StringUtf8Coder.of());
    WindowedValue.ValueOnlyWindowedValueCoder<KV<String, String>> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(kvCoder);

    TupleTag<String> outputTag = new TupleTag<>("main-output");
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(2L);
    options.setMaxBundleTimeMills(10L);

    DoFn<KV<String, String>, String> doFn =
        new DoFn<KV<String, String>, String>() {
          @ProcessElement
          public void processElement(ProcessContext ctx) {
            // Change output type of element to test that we do not depend on the input keying
            ctx.output(ctx.element().getValue());
          }

          @FinishBundle
          public void finishBundle(FinishBundleContext context) {
            context.output(
                "finishBundle", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(kvCoder.getValueCoder(), GlobalWindow.Coder.INSTANCE));

    DoFnOperator<KV<String, String>, KV<String, String>> doFnOperator =
        new DoFnOperator(
            doFn,
            "stepName",
            windowedValueCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            options,
            keyCoder,
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, String>>, WindowedValue<String>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                doFnOperator, keySelector, keySelector.getProducedType());

    testHarness.open();

    testHarness.processElement(
        new StreamRecord(WindowedValue.valueInGlobalWindow(KV.of("key", "a"))));
    testHarness.processElement(
        new StreamRecord(WindowedValue.valueInGlobalWindow(KV.of("key", "b"))));
    testHarness.processElement(
        new StreamRecord(WindowedValue.valueInGlobalWindow(KV.of("key", "c"))));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("a"),
            WindowedValue.valueInGlobalWindow("b"),
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("c")));

    // Take a snapshot
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    // Finish bundle element will be buffered as part of finishing a bundle in snapshot()
    PushedBackElementsHandler<KV<Integer, WindowedValue<?>>> pushedBackElementsHandler =
        doFnOperator.outputManager.pushedBackElementsHandler;
    assertThat(pushedBackElementsHandler, instanceOf(NonKeyedPushedBackElementsHandler.class));
    List<KV<Integer, WindowedValue<?>>> bufferedElements =
        pushedBackElementsHandler.getElements().collect(Collectors.toList());
    assertThat(
        bufferedElements, contains(KV.of(0, WindowedValue.valueInGlobalWindow("finishBundle"))));

    testHarness.close();

    doFnOperator =
        new DoFnOperator(
            doFn,
            "stepName",
            windowedValueCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            options,
            keyCoder,
            keySelector,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    testHarness =
        new KeyedOneInputStreamOperatorTestHarness(
            doFnOperator, keySelector, keySelector.getProducedType());

    // Restore snapshot
    testHarness.initializeState(snapshot);

    testHarness.open();

    // startBundle will output the buffered elements.
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("key", "d"))));

    // check finishBundle by timeout
    testHarness.setProcessingTime(10);

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            // The first finishBundle is restored from the checkpoint
            WindowedValue.valueInGlobalWindow("finishBundle"),
            WindowedValue.valueInGlobalWindow("d"),
            WindowedValue.valueInGlobalWindow("finishBundle")));

    testHarness.close();
  }

  @Test
  public void testCheckpointBufferingWithMultipleBundles() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(10L);
    options.setCheckpointingInterval(1L);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    StringUtf8Coder coder = StringUtf8Coder.of();
    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(coder);

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    @SuppressWarnings("unchecked")
    Supplier<DoFnOperator<String, String>> doFnOperatorSupplier =
        () ->
            new DoFnOperator<>(
                new IdentityDoFn(),
                "stepName",
                windowedValueCoder,
                Collections.emptyMap(),
                outputTag,
                Collections.emptyList(),
                outputManagerFactory,
                WindowingStrategy.globalDefault(),
                new HashMap<>(), /* side-input mapping */
                Collections.emptyList(), /* side inputs */
                options,
                null,
                null,
                DoFnSchemaInformation.create(),
                Collections.emptyMap());

    DoFnOperator<String, String> doFnOperator = doFnOperatorSupplier.get();
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    // start a bundle
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow("regular element")));

    // This callback will be executed in the snapshotState function in the course of
    // finishing the currently active bundle. Everything emitted in the callback should
    // be buffered and not sent downstream.
    doFnOperator.setBundleFinishedCallback(
        () -> {
          try {
            // Clear this early for the test here because we want to finish the bundle from within
            // the callback which would otherwise cause an infinitive recursion
            doFnOperator.setBundleFinishedCallback(null);
            testHarness.processElement(
                new StreamRecord<>(WindowedValue.valueInGlobalWindow("trigger another bundle")));
            doFnOperator.invokeFinishBundle();
            testHarness.processElement(
                new StreamRecord<>(
                    WindowedValue.valueInGlobalWindow(
                        "check that the previous element is not flushed")));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    // Check that we have only the element which was emitted before the snapshot
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow("regular element")));

    // Check that we would flush the buffered elements when continuing to run
    testHarness.processWatermark(Long.MAX_VALUE);
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("regular element"),
            WindowedValue.valueInGlobalWindow("trigger another bundle"),
            WindowedValue.valueInGlobalWindow("check that the previous element is not flushed")));

    testHarness.close();

    // Check that we would flush the buffered elements when restoring from a checkpoint
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness2 =
        new OneInputStreamOperatorTestHarness<>(doFnOperatorSupplier.get());

    testHarness2.initializeState(snapshot);
    testHarness2.open();

    testHarness2.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow("after restore")));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness2.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("trigger another bundle"),
            WindowedValue.valueInGlobalWindow("check that the previous element is not flushed"),
            WindowedValue.valueInGlobalWindow("after restore")));
  }

  @Test
  public void testExactlyOnceBuffering() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(2L);
    options.setCheckpointingInterval(1L);

    TupleTag<String> outputTag = new TupleTag<>("main-output");
    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    numStartBundleCalled = 0;
    DoFn<String, String> doFn =
        new DoFn<String, String>() {
          @StartBundle
          public void startBundle(StartBundleContext context) {
            numStartBundleCalled += 1;
          }

          @ProcessElement
          // Use RequiresStableInput to force buffering elements
          @RequiresStableInput
          public void processElement(ProcessContext context) {
            context.output(context.element());
          }

          @FinishBundle
          public void finishBundle(FinishBundleContext context) {
            context.output(
                "finishBundle", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    Supplier<DoFnOperator<String, String>> doFnOperatorSupplier =
        () ->
            new DoFnOperator<>(
                doFn,
                "stepName",
                windowedValueCoder,
                Collections.emptyMap(),
                outputTag,
                Collections.emptyList(),
                outputManagerFactory,
                WindowingStrategy.globalDefault(),
                new HashMap<>(), /* side-input mapping */
                Collections.emptyList(), /* side inputs */
                options,
                null,
                null,
                DoFnSchemaInformation.create(),
                Collections.emptyMap());

    DoFnOperator<String, String> doFnOperator = doFnOperatorSupplier.get();
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("a")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("b")));

    assertThat(Iterables.size(testHarness.getOutput()), is(0));
    assertThat(numStartBundleCalled, is(0));

    // create a backup and then
    OperatorSubtaskState backup = testHarness.snapshot(0, 0);
    doFnOperator.notifyCheckpointComplete(0L);

    assertThat(numStartBundleCalled, is(1));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("a"),
            WindowedValue.valueInGlobalWindow("b"),
            WindowedValue.valueInGlobalWindow("finishBundle")));

    doFnOperator = doFnOperatorSupplier.get();
    testHarness = new OneInputStreamOperatorTestHarness<>(doFnOperator);

    // restore from the snapshot
    testHarness.initializeState(backup);
    testHarness.open();

    doFnOperator.notifyCheckpointComplete(0L);

    assertThat(numStartBundleCalled, is(2));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("a"),
            WindowedValue.valueInGlobalWindow("b"),
            WindowedValue.valueInGlobalWindow("finishBundle")));

    // repeat to see if elements are evicted
    doFnOperator.notifyCheckpointComplete(1L);

    assertThat(numStartBundleCalled, is(2));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow("a"),
            WindowedValue.valueInGlobalWindow("b"),
            WindowedValue.valueInGlobalWindow("finishBundle")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExactlyOnceBufferingKeyed() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(2L);
    options.setCheckpointingInterval(1L);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    KvToByteBufferKeySelector keySelector = new KvToByteBufferKeySelector<>(keyCoder);
    KvCoder<String, String> kvCoder = KvCoder.of(keyCoder, StringUtf8Coder.of());
    WindowedValue.ValueOnlyWindowedValueCoder<KV<String, String>> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(kvCoder);

    DoFn<KV<String, String>, KV<String, String>> doFn =
        new DoFn<KV<String, String>, KV<String, String>>() {
          @StartBundle
          public void startBundle(StartBundleContext context) {
            numStartBundleCalled++;
          }

          @ProcessElement
          // Use RequiresStableInput to force buffering elements
          @RequiresStableInput
          public void processElement(ProcessContext context) {
            context.output(context.element());
          }

          @FinishBundle
          public void finishBundle(FinishBundleContext context) {
            context.output(
                KV.of("key3", "finishBundle"),
                BoundedWindow.TIMESTAMP_MIN_VALUE,
                GlobalWindow.INSTANCE);
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    Supplier<DoFnOperator<KV<String, String>, KV<String, String>>> doFnOperatorSupplier =
        () ->
            new DoFnOperator(
                doFn,
                "stepName",
                windowedValueCoder,
                Collections.emptyMap(),
                outputTag,
                Collections.emptyList(),
                outputManagerFactory,
                WindowingStrategy.globalDefault(),
                new HashMap<>(), /* side-input mapping */
                Collections.emptyList(), /* side inputs */
                options,
                keyCoder,
                keySelector,
                DoFnSchemaInformation.create(),
                Collections.emptyMap());

    DoFnOperator<KV<String, String>, KV<String, String>> doFnOperator = doFnOperatorSupplier.get();
    OneInputStreamOperatorTestHarness<
            WindowedValue<KV<String, String>>, WindowedValue<KV<String, String>>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                doFnOperator, keySelector, keySelector.getProducedType());

    testHarness.open();

    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("key", "a"))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("key", "b"))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("key2", "c"))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow(KV.of("key2", "d"))));

    assertThat(Iterables.size(testHarness.getOutput()), is(0));

    OperatorSubtaskState backup = testHarness.snapshot(0, 0);
    doFnOperator.notifyCheckpointComplete(0L);

    assertThat(numStartBundleCalled, is(1));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow(KV.of("key", "a")),
            WindowedValue.valueInGlobalWindow(KV.of("key", "b")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "c")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "d")),
            WindowedValue.valueInGlobalWindow(KV.of("key3", "finishBundle"))));

    doFnOperator = doFnOperatorSupplier.get();
    testHarness =
        new KeyedOneInputStreamOperatorTestHarness(
            doFnOperator, keySelector, keySelector.getProducedType());

    // restore from the snapshot
    testHarness.initializeState(backup);
    testHarness.open();

    doFnOperator.notifyCheckpointComplete(0L);

    assertThat(numStartBundleCalled, is(2));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow(KV.of("key", "a")),
            WindowedValue.valueInGlobalWindow(KV.of("key", "b")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "c")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "d")),
            WindowedValue.valueInGlobalWindow(KV.of("key3", "finishBundle"))));

    // repeat to see if elements are evicted
    doFnOperator.notifyCheckpointComplete(1L);

    assertThat(numStartBundleCalled, is(2));
    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.valueInGlobalWindow(KV.of("key", "a")),
            WindowedValue.valueInGlobalWindow(KV.of("key", "b")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "c")),
            WindowedValue.valueInGlobalWindow(KV.of("key2", "d")),
            WindowedValue.valueInGlobalWindow(KV.of("key3", "finishBundle"))));
  }

  @Test(expected = IllegalStateException.class)
  public void testFailOnRequiresStableInputAndDisabledCheckpointing() {
    TupleTag<String> outputTag = new TupleTag<>("main-output");

    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    KvToByteBufferKeySelector keySelector = new KvToByteBufferKeySelector<>(keyCoder);
    KvCoder<String, String> kvCoder = KvCoder.of(keyCoder, StringUtf8Coder.of());
    WindowedValue.ValueOnlyWindowedValueCoder<KV<String, String>> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(kvCoder);

    DoFn<String, String> doFn =
        new DoFn<String, String>() {
          @ProcessElement
          // Use RequiresStableInput to force buffering elements
          @RequiresStableInput
          public void processElement(ProcessContext context) {
            context.output(context.element());
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    // should make the DoFnOperator creation fail
    options.setCheckpointingInterval(-1L);
    new DoFnOperator(
        doFn,
        "stepName",
        windowedValueCoder,
        Collections.emptyMap(),
        outputTag,
        Collections.emptyList(),
        outputManagerFactory,
        WindowingStrategy.globalDefault(),
        new HashMap<>(), /* side-input mapping */
        Collections.emptyList(), /* side inputs */
        options,
        keyCoder,
        keySelector,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());
  }

  @Test
  public void testBundleProcessingExceptionIsFatalDuringCheckpointing() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setMaxBundleSize(10L);
    options.setCheckpointingInterval(1L);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    StringUtf8Coder coder = StringUtf8Coder.of();
    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(coder);

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    @SuppressWarnings("unchecked")
    DoFnOperator doFnOperator =
        new DoFnOperator<>(
            new IdentityDoFn() {
              @FinishBundle
              public void finishBundle() {
                throw new RuntimeException("something went wrong here");
              }
            },
            "stepName",
            windowedValueCoder,
            Collections.emptyMap(),
            outputTag,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            options,
            null,
            null,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    @SuppressWarnings("unchecked")
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    // start a bundle
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.valueInGlobalWindow("regular element")));

    // Make sure we throw Error, not a regular Exception.
    // A regular exception would just cause the checkpoint to fail.
    assertThrows(Error.class, () -> testHarness.snapshot(0, 0));
  }

  @Test
  public void testAccumulatorRegistrationOnOperatorClose() throws Exception {
    DoFnOperator doFnOperator = getOperatorForCleanupInspection();
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    String metricContainerFieldName = "flinkMetricContainer";
    FlinkMetricContainer monitoredContainer =
        Mockito.spy(
            (FlinkMetricContainer)
                Whitebox.getInternalState(doFnOperator, metricContainerFieldName));
    Whitebox.setInternalState(doFnOperator, metricContainerFieldName, monitoredContainer);

    // Closes and disposes the operator
    testHarness.close();
    // Ensure that dispose has the metrics code
    doFnOperator.dispose();
    Mockito.verify(monitoredContainer, Mockito.times(2)).registerMetricsForPipelineResult();
  }

  /**
   * Ensures Jackson cache is cleaned to get rid of any references to the Flink Classloader. See
   * https://jira.apache.org/jira/browse/BEAM-6460
   */
  @Test
  public void testRemoveCachedClassReferences() throws Exception {
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(getOperatorForCleanupInspection());

    LRUMap typeCache =
        (LRUMap) Whitebox.getInternalState(TypeFactory.defaultInstance(), "_typeCache");
    assertThat(typeCache.size(), greaterThan(0));
    testHarness.open();
    testHarness.close();
    assertThat(typeCache.size(), is(0));
  }

  private static DoFnOperator getOperatorForCleanupInspection() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setParallelism(4);

    TupleTag<String> outputTag = new TupleTag<>("main-output");
    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());
    IdentityDoFn<String> doFn =
        new IdentityDoFn<String>() {
          @FinishBundle
          public void finishBundle(FinishBundleContext context) {
            context.output(
                "finishBundle", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
          }
        };

    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    return new DoFnOperator<>(
        doFn,
        "stepName",
        windowedValueCoder,
        Collections.emptyMap(),
        outputTag,
        Collections.emptyList(),
        outputManagerFactory,
        WindowingStrategy.globalDefault(),
        new HashMap<>(), /* side-input mapping */
        Collections.emptyList(), /* side inputs */
        options,
        null,
        null,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());
  }

  private Iterable<WindowedValue<String>> stripStreamRecord(Iterable<?> input) {
    return FluentIterable.from(input)
        .filter(o -> o instanceof StreamRecord)
        .transform(
            new Function<Object, WindowedValue<String>>() {
              @Nullable
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public WindowedValue<String> apply(@Nullable Object o) {
                if (o instanceof StreamRecord) {
                  return (WindowedValue<String>) ((StreamRecord) o).getValue();
                }
                throw new RuntimeException("unreachable");
              }
            });
  }

  private static class MultiOutputDoFn extends DoFn<String, String> {
    private TupleTag<String> additionalOutput1;
    private TupleTag<String> additionalOutput2;

    public MultiOutputDoFn(TupleTag<String> additionalOutput1, TupleTag<String> additionalOutput2) {
      this.additionalOutput1 = additionalOutput1;
      this.additionalOutput2 = additionalOutput2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if ("one".equals(c.element())) {
        c.output(additionalOutput1, "extra: one");
      } else if ("two".equals(c.element())) {
        c.output(additionalOutput2, "extra: two");
      } else {
        c.output("got: " + c.element());
        c.output(additionalOutput1, "got: " + c.element());
        c.output(additionalOutput2, "got: " + c.element());
      }
    }
  }

  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private WindowedValue<Iterable<?>> valuesInWindow(
      Iterable<?> values, Instant timestamp, BoundedWindow window) {
    return (WindowedValue) WindowedValue.of(values, timestamp, window, PaneInfo.NO_FIRING);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T> WindowedValue<T> valueInWindow(T value, Instant timestamp, BoundedWindow window) {
    return WindowedValue.of(value, timestamp, window, PaneInfo.NO_FIRING);
  }

  private interface TestHarnessFactory<T> {
    T create() throws Exception;
  }
}
