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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link SimpleDoFnRunner}. */
@RunWith(JUnit4.class)
public class SimpleDoFnRunnerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.onTimer(
        TimerDeclaration.PREFIX + ThrowingDoFn.TIMER_ID,
        "",
        null,
        GlobalWindow.INSTANCE,
        new Instant(0),
        new Instant(0),
        TimeDomain.EVENT_TIME);
  }

  /**
   * Tests that a users call to set a timer gets properly dispatched to the timer internals. From
   * there on, it is the duty of the runner & step context to set it in whatever way is right for
   * that runner.
   */
  @Test
  public void testTimerSet() {
    WindowFn<?, ?> windowFn = new GlobalWindows();
    DoFnWithTimers<GlobalWindow> fn = new DoFnWithTimers(windowFn.windowCoder());
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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    // Setting the timer needs the current time, as it is set relative
    Instant currentTime = new Instant(42);
    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(currentTime);

    runner.processElement(WindowedValue.valueInGlobalWindow("anyValue"));

    verify(mockTimerInternals)
        .setTimer(
            StateNamespaces.window(new GlobalWindows().windowCoder(), GlobalWindow.INSTANCE),
            TimerDeclaration.PREFIX + DoFnWithTimers.TIMER_ID,
            "",
            currentTime.plus(DoFnWithTimers.TIMER_OFFSET),
            currentTime.plus(DoFnWithTimers.TIMER_OFFSET),
            TimeDomain.EVENT_TIME);
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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.finishBundle();
  }

  /**
   * Tests that {@link SimpleDoFnRunner#onTimer} properly dispatches to the underlying {@link DoFn}.
   */
  @Test
  public void testOnTimerCalled() {
    WindowFn<?, GlobalWindow> windowFn = new GlobalWindows();
    DoFnWithTimers<GlobalWindow> fn = new DoFnWithTimers(windowFn.windowCoder());
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
            WindowingStrategy.of(windowFn),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    Instant currentTime = new Instant(42);
    Duration offset = Duration.millis(37);

    // Mocking is not easily compatible with annotation analysis, so we manually record
    // the method call.
    runner.onTimer(
        TimerDeclaration.PREFIX + DoFnWithTimers.TIMER_ID,
        "",
        null,
        GlobalWindow.INSTANCE,
        currentTime.plus(offset),
        currentTime.plus(offset),
        TimeDomain.EVENT_TIME);

    assertThat(
        fn.onTimerInvocations,
        contains(
            TimerData.of(
                DoFnWithTimers.TIMER_ID,
                "",
                StateNamespaces.window(windowFn.windowCoder(), GlobalWindow.INSTANCE),
                currentTime.plus(offset),
                currentTime.plus(offset),
                TimeDomain.EVENT_TIME)));
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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

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
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

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

  private static class DoFnWithTimers<W extends BoundedWindow> extends DoFn<String, String> {
    static final String TIMER_ID = "testTimerId";

    static final Duration TIMER_OFFSET = Duration.millis(100);

    private final Coder<W> windowCoder;

    // Mutable
    List<TimerData> onTimerInvocations;

    DoFnWithTimers(Coder<W> windowCoder) {
      this.windowCoder = windowCoder;
      this.onTimerInvocations = new ArrayList<>();
    }

    @TimerId(TIMER_ID)
    private static final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(ProcessContext context, @TimerId(TIMER_ID) Timer timer) {
      timer.offset(TIMER_OFFSET).setRelative();
    }

    @OnTimer(TIMER_ID)
    public void onTimer(OnTimerContext context) {
      onTimerInvocations.add(
          TimerData.of(
              DoFnWithTimers.TIMER_ID,
              StateNamespaces.window(windowCoder, (W) context.window()),
              context.fireTimestamp(),
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
