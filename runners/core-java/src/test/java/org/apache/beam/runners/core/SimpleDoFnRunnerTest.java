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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Collections;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link SimpleDoFnRunner}. */
@RunWith(JUnit4.class)
public class SimpleDoFnRunnerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Mock StepContext mockStepContext;

  @Mock TimerInternals mockTimerInternals;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockStepContext.timerInternals()).thenReturn(mockTimerInternals);
  }

  @Test
  public void testProcessElementExceptionsWrappedAsUserCodeException() {
    ThrowingDoFn fn = new ThrowingDoFn();
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            null,
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.processElement(WindowedValue.valueInGlobalWindow("anyValue"));
  }

  @Test
  public void testOnTimerExceptionsWrappedAsUserCodeException() {
    ThrowingDoFn fn = new ThrowingDoFn();
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            null,
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.onTimer(
        ThrowingDoFn.TIMER_ID, GlobalWindow.INSTANCE, new Instant(0), TimeDomain.EVENT_TIME);
  }

  /**
   * Tests that a users call to set a timer gets properly dispatched to the timer internals. From
   * there on, it is the duty of the runner & step context to set it in whatever way is right for
   * that runner.
   */
  @Test
  public void testTimerSet() {
    WindowFn<?, GlobalWindow> windowFn = new GlobalWindows();
    DoFnWithTimers<GlobalWindow> fn = new DoFnWithTimers<>(windowFn.windowCoder());
    DoFnRunner<KV<String, String>, TimerData> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            null,
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    // Setting the timer needs the current time, as it is set relative
    Instant currentTime = new Instant(42);
    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(currentTime);
    when(mockTimerInternals.currentProcessingTime()).thenReturn(currentTime);

    runner.processElement(WindowedValue.valueInGlobalWindow(KV.of("anyKey", "anyValue")));

    verify(mockTimerInternals)
        .setTimer(
            StateNamespaces.window(new GlobalWindows().windowCoder(), GlobalWindow.INSTANCE),
            DoFnWithTimers.EVENT_TIMER_ID,
            currentTime.plus(DoFnWithTimers.TIMER_OFFSET),
            TimeDomain.EVENT_TIME);

    verify(mockTimerInternals)
        .setTimer(
            StateNamespaces.window(new GlobalWindows().windowCoder(), GlobalWindow.INSTANCE),
            DoFnWithTimers.PROCESSING_TIMER_ID,
            currentTime.plus(DoFnWithTimers.TIMER_OFFSET),
            TimeDomain.PROCESSING_TIME);
  }

  @Test
  public void testStartBundleExceptionsWrappedAsUserCodeException() {
    ThrowingDoFn fn = new ThrowingDoFn();
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            null,
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.startBundle();
  }

  @Test
  public void testFinishBundleExceptionsWrappedAsUserCodeException() {
    ThrowingDoFn fn = new ThrowingDoFn();
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            null,
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.finishBundle();
  }

  /**
   * Tests that {@link SimpleDoFnRunner#onTimer} properly dispatches to the underlying {@link DoFn}
   * on appropriate time domains.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testOnTimerCalledWithGlobalWindow() {

    // TIMESTAMP_MIN_VALUE is initial value for processing time used done by TestClock
    Instant currentProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    Instant currentEventTime = new Instant(42);

    TestStream<KV<String, String>> testStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(currentEventTime)
            .addElements(TimestampedValue.of(KV.of("anyKey", "anyValue"), new Instant(99)))
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkToInfinity();

    WindowFn<?, GlobalWindow> windowFn = new GlobalWindows();
    DoFnWithTimers<GlobalWindow> fn = new DoFnWithTimers<>(windowFn.windowCoder());

    PCollection<TimerData> output =
        pipeline
            .apply(testStream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(fn))
            .setCoder(TimerInternals.TimerDataCoder.of(windowFn.windowCoder()));

    PAssert.that(output)
        .containsInAnyOrder(
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), GlobalWindow.INSTANCE),
                currentProcessingTime.plus(DoFnWithTimers.TIMER_OFFSET).plus(1),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), GlobalWindow.INSTANCE),
                currentEventTime.plus(DoFnWithTimers.TIMER_OFFSET),
                TimeDomain.EVENT_TIME));

    pipeline.run();
  }

  /**
   * Tests that {@link SimpleDoFnRunner#onTimer} properly dispatches to the underlying {@link DoFn}
   * on appropriate time domains. With {@link IntervalWindow}, we check behavior of emitted events
   * when time is inside and outside of window boundaries.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testOnTimerCalledWithIntervalWindow() {

    // TIMESTAMP_MIN_VALUE is initial value for processing time used done by TestClock
    Instant baseTime = new Instant(0);

    Duration windowDuration = Duration.standardHours(1);
    Duration windowLateness = Duration.standardMinutes(1);
    IntervalWindow window = new IntervalWindow(baseTime, windowDuration);
    FixedWindows windowFn = FixedWindows.of(windowDuration);
    DoFnWithTimers<IntervalWindow> fn = new DoFnWithTimers<>(windowFn.windowCoder());

    TimestampedValue<KV<String, String>> event =
        TimestampedValue.of(KV.of("anyKey", "anyValue"), window.start());

    TestStream<KV<String, String>> testStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            // watermark in window, processing time far behind
            .advanceWatermarkTo(window.start())
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkTo(window.start().plus(DoFnWithTimers.TIMER_OFFSET).plus(1))

            // watermark and processing time within window
            .advanceProcessingTime(
                Duration.millis(Math.abs(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis())))
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkTo(
                window.start().plus(2 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(2))

            // watermark in window, processing time ahead of window.end() but within lateness
            .advanceProcessingTime(windowDuration)
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkTo(
                window.start().plus(3 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(3))

            // watermark in window, processing time  is out of window's allowed lateness
            .advanceProcessingTime(Duration.millis(100 * windowLateness.getMillis()))
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkTo(
                window.start().plus(4 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(4))

            // watermark is ahead of window.end() but we are not too late
            .advanceWatermarkTo(window.end().plus(1))
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkTo(window.end().plus(DoFnWithTimers.TIMER_OFFSET).plus(2))

            // all too late => only these 2 onTimer events are dropped, all above gets emitted
            .advanceWatermarkTo(window.end().plus(windowLateness).plus(1))
            .addElements(event)
            .advanceProcessingTime(DoFnWithTimers.TIMER_OFFSET.plus(1))
            .advanceWatermarkToInfinity();

    PCollection<TimerData> output =
        pipeline
            .apply(testStream)
            .apply(
                Window.<KV<String, String>>into(windowFn)
                    .withAllowedLateness(windowLateness)
                    .discardingFiredPanes())
            .apply(ParDo.of(fn))
            .setCoder(TimerInternals.TimerDataCoder.of(windowFn.windowCoder()));

    PAssert.that(output)
        .containsInAnyOrder(
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                BoundedWindow.TIMESTAMP_MIN_VALUE.plus(DoFnWithTimers.TIMER_OFFSET).plus(1),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                baseTime.plus(DoFnWithTimers.TIMER_OFFSET),
                TimeDomain.EVENT_TIME),
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                baseTime.plus(2 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(2),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                baseTime.plus(2 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(1),
                TimeDomain.EVENT_TIME),
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                window.end().plus(3 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(3),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                baseTime.plus(3 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(2),
                TimeDomain.EVENT_TIME),
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                window
                    .end()
                    .plus(100 * windowLateness.getMillis())
                    .plus(4 * DoFnWithTimers.TIMER_OFFSET.getMillis())
                    .plus(4),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                baseTime.plus(4 * DoFnWithTimers.TIMER_OFFSET.getMillis()).plus(3),
                TimeDomain.EVENT_TIME),
            TimerData.of(
                DoFnWithTimers.PROCESSING_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                window
                    .end()
                    .plus(100 * windowLateness.getMillis())
                    .plus(5 * DoFnWithTimers.TIMER_OFFSET.getMillis())
                    .plus(5),
                TimeDomain.PROCESSING_TIME),
            TimerData.of(
                DoFnWithTimers.EVENT_TIMER_ID,
                StateNamespaces.window(windowFn.windowCoder(), window),
                window.end().plus(DoFnWithTimers.TIMER_OFFSET).plus(1),
                TimeDomain.EVENT_TIME));

    pipeline.run();
  }

  /**
   * Demonstrates that attempting to output an element before the timestamp of the current element
   * with zero {@link DoFn#getAllowedTimestampSkew() allowed timestamp skew} throws.
   */
  @Test
  public void testBackwardsInTimeNoSkew() {
    SkewingDoFn fn = new SkewingDoFn(Duration.ZERO);
    DoFnRunner<Duration, Duration> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            new TupleTag<>(),
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    runner.startBundle();
    // An element output at the current timestamp is fine.
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(Duration.ZERO, new Instant(0)));
    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("must be no earlier");
    thrown.expectMessage(
        String.format("timestamp of the current input (%s)", new Instant(0).toString()));
    thrown.expectMessage(
        String.format(
            "the allowed skew (%s)", PeriodFormat.getDefault().print(Duration.ZERO.toPeriod())));
    // An element output before (current time - skew) is forbidden
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(Duration.millis(1L), new Instant(0)));
  }

  /**
   * Demonstrates that attempting to output an element before the timestamp of the current element
   * plus the value of {@link DoFn#getAllowedTimestampSkew()} throws, but between that value and the
   * current timestamp succeeds.
   */
  @Test
  public void testSkew() {
    SkewingDoFn fn = new SkewingDoFn(Duration.standardMinutes(10L));
    DoFnRunner<Duration, Duration> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            new TupleTag<>(),
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    runner.startBundle();
    // Outputting between "now" and "now - allowed skew" succeeds.
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(Duration.standardMinutes(5L), new Instant(0)));
    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("must be no earlier");
    thrown.expectMessage(
        String.format("timestamp of the current input (%s)", new Instant(0).toString()));
    thrown.expectMessage(
        String.format(
            "the allowed skew (%s)",
            PeriodFormat.getDefault().print(Duration.standardMinutes(10L).toPeriod())));
    // Outputting before "now - allowed skew" fails.
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(Duration.standardHours(1L), new Instant(0)));
  }

  /**
   * Demonstrates that attempting to output an element with a timestamp before the current one
   * always succeeds when {@link DoFn#getAllowedTimestampSkew()} is equal to {@link Long#MAX_VALUE}
   * milliseconds.
   */
  @Test
  public void testInfiniteSkew() {
    SkewingDoFn fn = new SkewingDoFn(Duration.millis(Long.MAX_VALUE));
    DoFnRunner<Duration, Duration> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            new TupleTag<>(),
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()));

    runner.startBundle();
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(Duration.millis(1L), new Instant(0)));
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(
            Duration.millis(1L), BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))));
    runner.processElement(
        WindowedValue.timestampedValueInGlobalWindow(
            // This is the maximum amount a timestamp in beam can move (from the maximum timestamp
            // to the minimum timestamp).
            Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis())
                .minus(Duration.millis(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis())),
            BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  static class ThrowingDoFn extends DoFn<String, String> {
    final Exception exceptionToThrow = new UnsupportedOperationException("Expected exception");

    static final String TIMER_ID = "throwingTimerId";

    @TimerId(TIMER_ID)
    private static final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StartBundle
    public void startBundle() throws Exception {
      throw exceptionToThrow;
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      throw exceptionToThrow;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      throw exceptionToThrow;
    }

    @OnTimer(TIMER_ID)
    public void onTimer(OnTimerContext context) throws Exception {
      throw exceptionToThrow;
    }
  }

  private static class DoFnWithTimers<W extends BoundedWindow>
      extends DoFn<KV<String, String>, TimerData> {
    static final String EVENT_TIMER_ID = "testEventTimerId";
    static final String PROCESSING_TIMER_ID = "testProcessingTimerId";

    static final Duration TIMER_OFFSET = Duration.millis(100);

    private final Coder<W> windowCoder;

    DoFnWithTimers(Coder<W> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @TimerId(EVENT_TIMER_ID)
    private static final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(PROCESSING_TIMER_ID)
    private static final TimerSpec processingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(
        ProcessContext context,
        @TimerId(EVENT_TIMER_ID) Timer eventTimer,
        @TimerId(PROCESSING_TIMER_ID) Timer processingTimer) {
      eventTimer.offset(TIMER_OFFSET).setRelative();
      processingTimer.offset(TIMER_OFFSET).setRelative();
    }

    @OnTimer(EVENT_TIMER_ID)
    public void onEventTimer(OnTimerContext context) {
      context.output(
          TimerData.of(
              DoFnWithTimers.EVENT_TIMER_ID,
              StateNamespaces.window(windowCoder, (W) context.window()),
              context.timestamp(),
              context.timeDomain()));
    }

    @OnTimer(PROCESSING_TIMER_ID)
    public void onProcessingTimer(OnTimerContext context) {
      context.output(
          TimerData.of(
              DoFnWithTimers.PROCESSING_TIMER_ID,
              StateNamespaces.window(windowCoder, (W) context.window()),
              context.timestamp(),
              context.timeDomain()));
    }
  }

  /**
   * A {@link DoFn} that outputs elements with timestamp equal to the input timestamp minus the
   * input element.
   */
  private static class SkewingDoFn extends DoFn<Duration, Duration> {
    private final Duration allowedSkew;

    private SkewingDoFn(Duration allowedSkew) {
      this.allowedSkew = allowedSkew;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.outputWithTimestamp(context.element(), context.timestamp().minus(context.element()));
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedSkew;
    }
  }

  private static class ListOutputManager implements OutputManager {
    private ListMultimap<TupleTag<?>, WindowedValue<?>> outputs = ArrayListMultimap.create();

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }
}
