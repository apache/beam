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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
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
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class SimpleDoFnRunnerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public final transient TestPipeline p = TestPipeline.create();

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

    runner.processElement(WindowedValues.valueInGlobalWindow("anyValue"));
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
        TimeDomain.EVENT_TIME,
        CausedByDrain.NORMAL);
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

    runner.processElement(WindowedValues.valueInGlobalWindow("anyValue"));

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
        TimeDomain.EVENT_TIME,
        CausedByDrain.CAUSED_BY_DRAIN);

    assertThat(
        fn.onTimerInvocations,
        contains(
            TimerData.of(
                DoFnWithTimers.TIMER_ID,
                "",
                StateNamespaces.window(windowFn.windowCoder(), GlobalWindow.INSTANCE),
                currentTime.plus(offset),
                currentTime.plus(offset),
                TimeDomain.EVENT_TIME,
                CausedByDrain.CAUSED_BY_DRAIN)));
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
        WindowedValues.timestampedValueInGlobalWindow(Duration.ZERO, new Instant(0)));
    Exception exception =
        assertThrows(
            UserCodeException.class,
            () -> {
              // An element output before (current time - skew) is forbidden
              runner.processElement(
                  WindowedValues.timestampedValueInGlobalWindow(
                      Duration.millis(1L), new Instant(0)));
            });

    assertThat(exception.getCause(), isA(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(),
        allOf(
            containsString("must be no earlier"),
            containsString(
                String.format(
                    "timestamp of the current input or timer (%s)", new Instant(0).toString())),
            containsString(
                String.format(
                    "the allowed skew (%s)",
                    PeriodFormat.getDefault().print(Duration.ZERO.toPeriod())))));
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
        WindowedValues.timestampedValueInGlobalWindow(
            Duration.standardMinutes(5L), new Instant(0)));

    Exception exception =
        assertThrows(
            UserCodeException.class,
            () -> {
              // Outputting before "now - allowed skew" fails.
              runner.processElement(
                  WindowedValues.timestampedValueInGlobalWindow(
                      Duration.standardHours(1L), new Instant(0)));
            });

    assertThat(exception.getCause(), isA(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(),
        allOf(
            containsString("must be no earlier"),
            containsString(
                String.format(
                    "timestamp of the current input or timer (%s)", new Instant(0).toString())),
            containsString(
                String.format(
                    "the allowed skew (%s)",
                    PeriodFormat.getDefault().print(Duration.standardMinutes(10L).toPeriod())))));
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
        WindowedValues.timestampedValueInGlobalWindow(Duration.millis(1L), new Instant(0)));
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(
            Duration.millis(1L), BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))));
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(
            // This is the maximum amount a timestamp in beam can move (from the maximum timestamp
            // to the minimum timestamp).
            Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis())
                .minus(Duration.millis(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis())),
            BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  /**
   * Demonstrates that attempting to set a timer with an output timestamp before the timestamp of
   * the current element with zero {@link DoFn#getAllowedTimestampSkew() allowed timestamp skew}
   * throws.
   */
  @Test
  public void testTimerBackwardsInTimeNoSkew() {
    TimerSkewDoFn fn = new TimerSkewDoFn(Duration.ZERO);
    DoFnRunner<KV<String, Duration>, Duration> runner =
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
    // A timer with output timestamp at the current timestamp is fine.
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(KV.of("1", Duration.ZERO), new Instant(0)));

    Exception exception =
        assertThrows(
            UserCodeException.class,
            () -> {
              // A timer with output timestamp before (current time - skew) is forbidden
              runner.processElement(
                  WindowedValues.timestampedValueInGlobalWindow(
                      KV.of("2", Duration.millis(1L)), new Instant(0)));
            });

    assertThat(exception.getCause(), isA(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(),
        allOf(
            containsString("Cannot output timer with"),
            containsString(
                String.format("output timestamp %s", new Instant(0).minus(Duration.millis(1L)))),
            containsString(
                String.format(
                    "allowed skew (%s)",
                    PeriodFormat.getDefault().print(Duration.ZERO.toPeriod())))));
  }

  /**
   * Demonstrates that attempting to have a timer with output timestamp before the timestamp of the
   * current element plus the value of {@link DoFn#getAllowedTimestampSkew()} throws, but between
   * that value and the current timestamp succeeds.
   */
  @Test
  public void testTimerSkew() {
    TimerSkewDoFn fn = new TimerSkewDoFn(Duration.standardMinutes(10L));
    DoFnRunner<KV<String, Duration>, Duration> runner =
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
    // Timer with output timestamp between "now" and "now - allowed skew" succeeds.
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(
            KV.of("1", Duration.standardMinutes(5L)), new Instant(0)));

    Exception exception =
        assertThrows(
            UserCodeException.class,
            () -> {
              // A timer with output timestamp before (current time - skew) is forbidden
              runner.processElement(
                  WindowedValues.timestampedValueInGlobalWindow(
                      KV.of("2", Duration.standardHours(1L)), new Instant(0)));
            });

    assertThat(exception.getCause(), isA(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(),
        allOf(
            containsString("Cannot output timer with"),
            containsString(
                String.format(
                    "output timestamp %s", new Instant(0).minus(Duration.standardHours(1L)))),
            containsString(
                String.format(
                    "allowed skew (%s)",
                    PeriodFormat.getDefault().print(Duration.standardMinutes(10L).toPeriod())))));
  }

  /**
   * Demonstrates that attempting to output an element with a timestamp before the current one
   * always succeeds when {@link DoFn#getAllowedTimestampSkew()} is equal to {@link Long#MAX_VALUE}
   * milliseconds.
   */
  @Test
  public void testTimerInfiniteSkew() {
    TimerSkewDoFn fn = new TimerSkewDoFn(Duration.millis(Long.MAX_VALUE));
    DoFnRunner<KV<String, Duration>, Duration> runner =
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
        WindowedValues.timestampedValueInGlobalWindow(
            KV.of("1", Duration.millis(1L)), new Instant(0)));
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(
            KV.of("2", Duration.millis(1L)),
            BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))));
    runner.processElement(
        WindowedValues.timestampedValueInGlobalWindow(
            KV.of(
                "3",
                // This is the maximum amount a timestamp in beam can move (from the maximum
                // timestamp
                // to the minimum timestamp).
                Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis())
                    .minus(Duration.millis(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()))),
            BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void testOnTimerAllowedSkew() {
    TimerOutputSkewingDoFn fn = new TimerOutputSkewingDoFn(Duration.millis(10), Duration.millis(5));
    DoFnRunner<KV<String, Duration>, Duration> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    runner.onTimer(
        TimerDeclaration.PREFIX + TimerOutputSkewingDoFn.TIMER_ID,
        "",
        null,
        GlobalWindow.INSTANCE,
        new Instant(0),
        new Instant(0),
        TimeDomain.EVENT_TIME,
        CausedByDrain.NORMAL);
  }

  @Test
  public void testOnTimerNoSkew() {
    TimerOutputSkewingDoFn fn = new TimerOutputSkewingDoFn(Duration.ZERO, Duration.millis(5));
    DoFnRunner<KV<String, Duration>, Duration> runner =
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

    Exception exception =
        assertThrows(
            UserCodeException.class,
            () -> {
              runner.onTimer(
                  TimerDeclaration.PREFIX + TimerOutputSkewingDoFn.TIMER_ID,
                  "",
                  null,
                  GlobalWindow.INSTANCE,
                  new Instant(0),
                  new Instant(0),
                  TimeDomain.EVENT_TIME,
                  CausedByDrain.NORMAL);
            });

    assertThat(exception.getCause(), isA(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(),
        allOf(
            containsString("must be no earlier"),
            containsString(
                String.format(
                    "timestamp of the current input or timer (%s)", new Instant(0).toString())),
            containsString(
                String.format(
                    "the allowed skew (%s)",
                    PeriodFormat.getDefault().print(Duration.ZERO.toPeriod())))));
  }

  @Test
  public void testProcessElementIsolatesOpenTelemetryContext() {
    ContextKey<String> testKey = ContextKey.named("beam-test-key");
    List<String> observedValues = new ArrayList<>();
    TupleTag<String> mainTag = new TupleTag<>("main");
    ListOutputManager outputManager = new ListOutputManager();
    DoFn<String, String> recordingFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String elem, OutputReceiver<String> out) {
            observedValues.add(Context.current().get(testKey));
            out.output(elem);
          }
        };

    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            recordingFn,
            NullSideInputReader.empty(),
            outputManager,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    // Build an untraced element under Context.root() -> normalized openTelemetryContext is null.
    WindowedValue<String> untraced;
    try (Scope ignored = Context.root().makeCurrent()) {
      untraced =
          WindowedValues.ofWithCurrentContext(WindowedValues.valueInGlobalWindow("in"), "untraced");
    }
    assertNull(untraced.getOpenTelemetryContext());

    Context ambientCtx = Context.root().with(testKey, "ambient");
    Context elemCtx = Context.root().with(testKey, "element");
    WindowedValue<String> traced =
        WindowedValues.of(
            "traced",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.NORMAL,
            elemCtx,
            ValueKind.INSERT);
    WindowedValue<String> explicitRoot =
        WindowedValues.of(
            "root",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.NORMAL,
            Context.root(),
            ValueKind.INSERT);

    // Executing an untraced element while ambientCtx is active must NOT leak ambientCtx.
    try (Scope ignored = ambientCtx.makeCurrent()) {
      runner.processElement(untraced);
      assertSame(ambientCtx, Context.current());

      runner.processElement(traced);
      assertSame(ambientCtx, Context.current());

      runner.processElement(explicitRoot);
      assertSame(ambientCtx, Context.current());
    }

    // Running explicit Context.root() with no attached thread context should also work cleanly.
    runner.processElement(explicitRoot);
    assertSame(Context.root(), Context.current());

    assertEquals(4, observedValues.size());
    assertNull(observedValues.get(0));
    assertEquals("element", observedValues.get(1));
    assertNull(observedValues.get(2));
    assertNull(observedValues.get(3));

    List<WindowedValue<?>> emitted = outputManager.outputs.get(mainTag);
    assertEquals(4, emitted.size());
    assertNull(emitted.get(0).getOpenTelemetryContext());
    assertSame(elemCtx, emitted.get(1).getOpenTelemetryContext());
    assertNull(emitted.get(2).getOpenTelemetryContext());
    assertNull(emitted.get(3).getOpenTelemetryContext());

    // ProcessContext.output() must capture a span started inside @ProcessElement even when
    // the input element had null openTelemetryContext (consistent with OutputReceiver.output).
    Context childCtx = Context.root().with(testKey, "in-dofn-span");
    ListOutputManager legacyOut = new ListOutputManager();
    DoFn<String, String> spanCreatingFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            try (Scope ignored = childCtx.makeCurrent()) {
              c.output("tagged-" + c.element());
            }
          }
        };
    DoFnRunner<String, String> legacyRunner =
        new SimpleDoFnRunner<>(
            null,
            spanCreatingFn,
            NullSideInputReader.empty(),
            legacyOut,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    legacyRunner.processElement(untraced);
    assertSame(childCtx, legacyOut.outputs.get(mainTag).get(0).getOpenTelemetryContext());
  }

  @Test
  public void testProcessElementOutputPreservesEmptyWindows() throws Exception {
    TupleTag<String> mainTag = new TupleTag<>("main");
    ListOutputManager outputManager = new ListOutputManager();
    DoFn<String, String> doFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output("out:" + c.element());
          }
        };
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            doFn,
            NullSideInputReader.empty(),
            outputManager,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(new GlobalWindows()),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    // 1. Custom empty-window WindowedValue (models Dataflow ValueInEmptyWindows).
    WindowedValue<String> emptyWrapper = new EmptyWindowedValue<>("v1", new Instant(1000L));
    runner.processElement(emptyWrapper);

    // 2. Legacy zero-window stream decoded via FullWindowedValueCoder.
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    org.apache.beam.sdk.coders.InstantCoder.of().encode(new Instant(2000L), baos);
    baos.write(new byte[] {0, 0, 0, 0}); // numWindows == 0
    org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder.INSTANCE.encode(
        PaneInfo.NO_FIRING, baos);
    org.apache.beam.sdk.coders.StringUtf8Coder.of().encode("v2", baos);
    WindowedValue<String> decodedZero =
        WindowedValues.getFullCoder(
                org.apache.beam.sdk.coders.StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE)
            .decode(new java.io.ByteArrayInputStream(baos.toByteArray()));
    Context activeCtx = Context.root().with(ContextKey.named("k"), "active");
    try (Scope ignored = activeCtx.makeCurrent()) {
      runner.processElement(decodedZero);
    }

    List<WindowedValue<?>> emitted = outputManager.outputs.get(mainTag);
    assertEquals(2, emitted.size());
    assertEquals("out:v1", emitted.get(0).getValue());
    assertEquals(0, emitted.get(0).getWindows().size());
    assertEquals("out:v2", emitted.get(1).getValue());
    assertEquals(0, emitted.get(1).getWindows().size());
  }

  private static final class EmptyWindowedValue<T> implements WindowedValue<T> {
    private final T value;
    private final Instant timestamp;

    EmptyWindowedValue(T value, Instant timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public Instant getTimestamp() {
      return timestamp;
    }

    @Override
    public java.util.Collection<? extends BoundedWindow> getWindows() {
      return Collections.emptyList();
    }

    @Override
    public PaneInfo getPaneInfo() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public String getRecordId() {
      return null;
    }

    @Override
    public Context getOpenTelemetryContext() {
      return null;
    }

    @Override
    public Long getRecordOffset() {
      return null;
    }

    @Override
    public CausedByDrain causedByDrain() {
      return CausedByDrain.NORMAL;
    }

    @Override
    public ValueKind getValueKind() {
      return ValueKind.INSERT;
    }

    @Override
    public Iterable<? extends WindowedValue<T>> explodeWindows() {
      return Collections.emptyList();
    }

    @Override
    public <OtherT> WindowedValue<OtherT> withValue(OtherT newValue) {
      return new EmptyWindowedValue<>(newValue, timestamp);
    }
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
              context.timeDomain(),
              context.causedByDrain()));
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

  /**
   * A {@link DoFn} that creates/sets a timer with an output timestamp equal to the input timestamp
   * minus the input element's value. Keys are ignored but required for timers.
   */
  private static class TimerSkewDoFn extends DoFn<KV<String, Duration>, Duration> {
    static final String TIMER_ID = "testTimerId";
    private final Duration allowedSkew;

    @TimerId(TIMER_ID)
    private static final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private TimerSkewDoFn(Duration allowedSkew) {
      this.allowedSkew = allowedSkew;
    }

    @ProcessElement
    public void processElement(ProcessContext context, @TimerId(TIMER_ID) Timer timer) {
      timer
          .withOutputTimestamp(context.timestamp().minus(context.element().getValue()))
          .set(new Instant(0));
    }

    @OnTimer(TIMER_ID)
    public void onTimer() {}

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedSkew;
    }
  }

  /**
   * A {@link DoFn} that creates/sets a timer with an output timestamp equal to the input timestamp
   * minus the input element's value. Keys are ignored but required for timers.
   */
  private static class TimerOutputSkewingDoFn extends DoFn<KV<String, Duration>, Duration> {
    static final String TIMER_ID = "testTimerId";
    private final Duration allowedSkew;
    private final Duration outputTimestampSkew;

    @TimerId(TIMER_ID)
    private static final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private TimerOutputSkewingDoFn(Duration allowedSkew, Duration outputTimestampSkew) {
      this.allowedSkew = allowedSkew;
      this.outputTimestampSkew = outputTimestampSkew;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {}

    @OnTimer(TIMER_ID)
    public void onTimer(OnTimerContext context) {
      Instant outputTimestamp = context.timestamp().minus(outputTimestampSkew);
      context.outputWithTimestamp(Duration.ZERO, outputTimestamp);
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedSkew;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReentrantProcessElementPreservesOuterContextAndMetadata() {
    TupleTag<String> mainTag = new TupleTag<>("main");
    IntervalWindow outerWin =
        new IntervalWindow(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    IntervalWindow innerWin =
        new IntervalWindow(Instant.ofEpochMilli(500), Instant.ofEpochMilli(600));
    PaneInfo outerPane = PaneInfo.createPane(true, false, Timing.EARLY, 1, 0);
    PaneInfo innerPane = PaneInfo.createPane(false, true, Timing.LATE, 2, 1);
    WindowedValue<String> outerElem =
        WindowedValues.of(
            "outer",
            Instant.ofEpochMilli(150),
            outerWin,
            outerPane,
            "outer-rec",
            11L,
            CausedByDrain.NORMAL,
            null,
            ValueKind.INSERT);
    WindowedValue<String> innerElem =
        WindowedValues.of(
            "inner",
            Instant.ofEpochMilli(550),
            innerWin,
            innerPane,
            "inner-rec",
            22L,
            CausedByDrain.CAUSED_BY_DRAIN,
            null,
            ValueKind.DELETE);

    List<WindowedValue<String>> observed = new ArrayList<>();
    List<DoFn<String, String>.ProcessContext> capturedContexts = new ArrayList<>();
    DoFn<String, String> echoDoFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(
              ProcessContext c, BoundedWindow window, OutputReceiver<String> out) {
            capturedContexts.add(c);
            String before = "before:" + c.element() + "@" + c.timestamp().getMillis();
            out.output(before);
            // Verify ProcessContext still sees its own element/window/metadata after reentry.
            assertEquals(c.element().equals("outer") ? outerWin : innerWin, window);
            String after = "after:" + c.element() + "@" + c.timestamp().getMillis();
            out.output(after);
          }
        };

    final DoFnRunner<String, String>[] runnerRef = new DoFnRunner[1];
    WindowedValueMultiReceiver reentrantOutputManager =
        new WindowedValueMultiReceiver() {
          @Override
          public <OutputT> void output(TupleTag<OutputT> tag, WindowedValue<OutputT> output) {
            WindowedValue<String> wv = (WindowedValue<String>) output;
            observed.add(wv);
            if ("before:outer@150".equals(wv.getValue())) {
              runnerRef[0].processElement(innerElem);
            }
          }
        };

    runnerRef[0] =
        new SimpleDoFnRunner<>(
            null,
            echoDoFn,
            NullSideInputReader.empty(),
            reentrantOutputManager,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(FixedWindows.of(Duration.millis(100))),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    runnerRef[0].startBundle();
    runnerRef[0].processElement(outerElem);
    runnerRef[0].finishBundle();

    assertEquals(4, observed.size());
    assertEquals("before:outer@150", observed.get(0).getValue());
    assertEquals(Instant.ofEpochMilli(150), observed.get(0).getTimestamp());
    assertThat(observed.get(0).getWindows(), contains(outerWin));
    assertEquals(outerPane, observed.get(0).getPaneInfo());
    assertEquals("outer-rec", observed.get(0).getRecordId());
    assertEquals(Long.valueOf(11L), observed.get(0).getRecordOffset());
    assertEquals(CausedByDrain.NORMAL, observed.get(0).causedByDrain());
    assertEquals(ValueKind.INSERT, observed.get(0).getValueKind());

    assertEquals("before:inner@550", observed.get(1).getValue());
    assertEquals(Instant.ofEpochMilli(550), observed.get(1).getTimestamp());
    assertThat(observed.get(1).getWindows(), contains(innerWin));
    assertEquals(innerPane, observed.get(1).getPaneInfo());
    assertEquals("inner-rec", observed.get(1).getRecordId());
    assertEquals(Long.valueOf(22L), observed.get(1).getRecordOffset());
    assertEquals(CausedByDrain.CAUSED_BY_DRAIN, observed.get(1).causedByDrain());
    assertEquals(ValueKind.DELETE, observed.get(1).getValueKind());

    assertEquals("after:inner@550", observed.get(2).getValue());
    assertEquals("after:outer@150", observed.get(3).getValue());
    assertEquals(Instant.ofEpochMilli(150), observed.get(3).getTimestamp());
    assertThat(observed.get(3).getWindows(), contains(outerWin));
    assertEquals(outerPane, observed.get(3).getPaneInfo());
    assertEquals("outer-rec", observed.get(3).getRecordId());
    assertEquals(Long.valueOf(11L), observed.get(3).getRecordOffset());
    assertEquals(CausedByDrain.NORMAL, observed.get(3).causedByDrain());
    assertEquals(ValueKind.INSERT, observed.get(3).getValueKind());

    // Both primary AND reentrant fallback contexts must fail fast once @ProcessElement exits.
    assertEquals(2, capturedContexts.size());
    for (DoFn<String, String>.ProcessContext ctx : capturedContexts) {
      assertThrows(IllegalStateException.class, ctx::element);
    }

    // Reentrant inner exception swallowed inside outputManager still leaves outer context intact.
    DoFn<String, String> throwOnInnerFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            if ("boom".equals(c.element())) {
              throw new IllegalStateException("inner boom");
            }
            c.output("pre:" + c.element());
            c.output("post:" + c.element());
          }
        };
    List<String> afterThrow = new ArrayList<>();
    final DoFnRunner<String, String>[] runner2 = new DoFnRunner[1];
    runner2[0] =
        new SimpleDoFnRunner<>(
            null,
            throwOnInnerFn,
            NullSideInputReader.empty(),
            new WindowedValueMultiReceiver() {
              @Override
              public <OutputT> void output(TupleTag<OutputT> tag, WindowedValue<OutputT> out) {
                String v = (String) out.getValue();
                afterThrow.add(v);
                if ("pre:outer".equals(v)) {
                  assertThrows(
                      UserCodeException.class,
                      () -> runner2[0].processElement(WindowedValues.valueInGlobalWindow("boom")));
                }
              }
            },
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    runner2[0].processElement(WindowedValues.valueInGlobalWindow("outer"));
    assertThat(afterThrow, contains("pre:outer", "post:outer"));
    // And after an outer exception, reusable context is cleanly reset for subsequent elements.
    assertThrows(
        UserCodeException.class,
        () -> runner2[0].processElement(WindowedValues.valueInGlobalWindow("boom")));
    afterThrow.clear();
    runner2[0].processElement(WindowedValues.valueInGlobalWindow("ok"));
    assertThat(afterThrow, contains("pre:ok", "post:ok"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiOutputReceiverBoundariesDuplicatesAndNonIdenticalTag() {
    TupleTag<String> mainTag = new TupleTag<>("main");
    TupleTag<String> mainCopy = new TupleTag<>("main");
    TupleTag<String> unknownTag = new TupleTag<>("unknown");

    // Verify 0, 1, 2, 8, and 9 extra tags (crossing MAX_INLINE_EXTRA_TAGS = 8 boundary), plus
    // duplicate mainTag and duplicate side tag inside additionalOutputTags.
    for (int extraCount : new int[] {0, 1, 2, 8, 9}) {
      List<TupleTag<?>> extraTags = new ArrayList<>();
      List<TupleTag<String>> extraCopies = new ArrayList<>();
      for (int i = 0; i < extraCount; i++) {
        extraTags.add(new TupleTag<String>("side-" + i));
        extraCopies.add(new TupleTag<>("side-" + i));
      }
      List<TupleTag<?>> withDuplicates = new ArrayList<>(extraTags);
      withDuplicates.add(mainTag); // duplicate main tag (like FlinkDoFnFunction)
      if (!extraTags.isEmpty()) {
        withDuplicates.add(extraTags.get(0));
      }

      ListOutputManager manager = new ListOutputManager();
      DoFn<String, String> multiDoFn =
          new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String elem, MultiOutputReceiver out) {
              out.get(mainTag).output(elem + ":main");
              out.get(mainCopy).output(elem + ":mainCopy");
              // Passing null routes to mainOutputTag, matching DoFnOutputReceivers / FnApi.
              out.get((TupleTag<String>) null).output(elem + ":nullToMain");
              for (int i = 0; i < extraTags.size(); i++) {
                TupleTag<String> exact = (TupleTag<String>) extraTags.get(i);
                TupleTag<String> copy = extraCopies.get(i);
                out.get(exact).output(elem + ":s" + i);
                out.get(copy).output(elem + ":s" + i + "c");
              }
              assertThrows(IllegalArgumentException.class, () -> out.get(unknownTag).output("bad"));
            }
          };

      DoFnRunner<String, String> runner =
          new SimpleDoFnRunner<>(
              null,
              multiDoFn,
              NullSideInputReader.empty(),
              manager,
              mainTag,
              withDuplicates,
              mockStepContext,
              null,
              Collections.emptyMap(),
              WindowingStrategy.globalDefault(),
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      runner.startBundle();
      // Process two sequential elements to hit both initial alias population and cached fast path.
      runner.processElement(WindowedValues.valueInGlobalWindow("e1"));
      runner.processElement(WindowedValues.valueInGlobalWindow("e2"));
      runner.finishBundle();

      assertThat(
          manager.outputs.get(mainTag),
          contains(
              WindowedValues.valueInGlobalWindow("e1:main"),
              WindowedValues.valueInGlobalWindow("e1:mainCopy"),
              WindowedValues.valueInGlobalWindow("e1:nullToMain"),
              WindowedValues.valueInGlobalWindow("e2:main"),
              WindowedValues.valueInGlobalWindow("e2:mainCopy"),
              WindowedValues.valueInGlobalWindow("e2:nullToMain")));
      for (int i = 0; i < extraTags.size(); i++) {
        assertThat(
            manager.outputs.get(extraTags.get(i)),
            contains(
                WindowedValues.valueInGlobalWindow("e1:s" + i),
                WindowedValues.valueInGlobalWindow("e1:s" + i + "c"),
                WindowedValues.valueInGlobalWindow("e2:s" + i),
                WindowedValues.valueInGlobalWindow("e2:s" + i + "c")));
      }
    }
  }

  @Test
  public void testNullTagValidationBeforeAndAfterSideOutput() {
    TupleTag<String> mainTag = new TupleTag<>("main");
    TupleTag<String> side1 = new TupleTag<>("side1");
    TupleTag<String> side2 = new TupleTag<>("side2");
    ListOutputManager manager = new ListOutputManager();

    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            // Null tag passed to outputWindowedValue BEFORE any valid side output must throw IAE.
            assertThrows(
                IllegalArgumentException.class,
                () -> c.outputWindowedValue(null, WindowedValues.valueInGlobalWindow("bad")));
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    c.outputWindowedValue(
                        null,
                        "bad",
                        BoundedWindow.TIMESTAMP_MIN_VALUE,
                        Collections.singletonList(GlobalWindow.INSTANCE),
                        PaneInfo.NO_FIRING));
            // Alternating valid side outputs (both exact and equal copies).
            c.output(side1, "s1");
            c.output(side2, "s2");
            c.output(new TupleTag<>("side1"), "s1copy");
            // Null tag AFTER valid side outputs must also throw IAE.
            assertThrows(
                IllegalArgumentException.class,
                () -> c.outputWindowedValue(null, WindowedValues.valueInGlobalWindow("bad2")));
          }

          @FinishBundle
          public void finishBundle(FinishBundleContext c) {
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    c.output(
                        null, "bad", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE));
          }
        };

    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            fn,
            NullSideInputReader.empty(),
            manager,
            mainTag,
            Arrays.asList(side1, side2),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    runner.startBundle();
    runner.processElement(WindowedValues.valueInGlobalWindow("x"));
    runner.finishBundle();
    assertEquals(2, manager.outputs.get(side1).size());
    assertEquals(1, manager.outputs.get(side2).size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullMainOutputTagAndNullAdditionalOutputTagSafety() {
    TupleTag<String> side0 = new TupleTag<>("side-0");
    TupleTag<String> unknown = new TupleTag<>("unknown");

    // ProcessContext.output(v) with null mainOutputTag must throw NPE (wrapped in
    // UserCodeException).
    DoFn<String, String> mainOutFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output("x");
          }
        };
    DoFnRunner<String, String> nullMainRunner =
        new SimpleDoFnRunner<>(
            null,
            mainOutFn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            null,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    UserCodeException uce =
        assertThrows(
            UserCodeException.class,
            () -> nullMainRunner.processElement(WindowedValues.valueInGlobalWindow("in")));
    assertThat(uce.getCause(), isA(NullPointerException.class));

    // CachedMultiOutputReceiver with mainOutputTag == null across 0, 1, 2, and 9 extra tags.
    for (int extraCount : new int[] {0, 1, 2, 9}) {
      List<TupleTag<?>> extras = new ArrayList<>();
      for (int i = 0; i < extraCount; i++) {
        extras.add(new TupleTag<String>("side-" + i));
      }
      ListOutputManager mgr = new ListOutputManager();
      DoFn<String, String> multiFn =
          new DoFn<String, String>() {
            @ProcessElement
            public void processElement(MultiOutputReceiver out) {
              if (extraCount > 0) {
                out.get(side0).output("ok");
              }
              assertThrows(
                  IllegalArgumentException.class, () -> out.get(unknown).output("unknown"));
            }
          };
      DoFnRunner<String, String> r =
          new SimpleDoFnRunner<>(
              null,
              multiFn,
              NullSideInputReader.empty(),
              mgr,
              null,
              extras,
              mockStepContext,
              null,
              Collections.emptyMap(),
              WindowingStrategy.globalDefault(),
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      r.processElement(WindowedValues.valueInGlobalWindow("in"));
      if (extraCount > 0) {
        assertEquals(1, mgr.outputs.get(side0).size());
      }
    }

    // additionalOutputTags containing a null tag with non-null mainOutputTag must not NPE on
    // slow-path lookup, and out.get(null) must still route to mainOutputTag.
    TupleTag<String> mainTag = new TupleTag<>("main");
    ListOutputManager mgr = new ListOutputManager();
    DoFn<String, String> nullExtraFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(MultiOutputReceiver out) {
            out.get((TupleTag<String>) null).output("routed-to-main");
            assertThrows(IllegalArgumentException.class, () -> out.get(unknown).output("bad"));
          }
        };
    DoFnRunner<String, String> r =
        new SimpleDoFnRunner<>(
            null,
            nullExtraFn,
            NullSideInputReader.empty(),
            mgr,
            mainTag,
            Arrays.asList(null, side0),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    r.processElement(WindowedValues.valueInGlobalWindow("in"));
    assertThat(
        mgr.outputs.get(mainTag), contains(WindowedValues.valueInGlobalWindow("routed-to-main")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRowReceiversAndDirectOutputReceiverOverloads() {
    Schema schema = Schema.builder().addStringField("s").build();
    SchemaCoder<Row> schemaCoder = SchemaCoder.of(schema);
    TupleTag<Row> mainTag = new TupleTag<>("main");
    TupleTag<Row> sideTag = new TupleTag<>("side");
    TupleTag<String> nonSchemaTag = new TupleTag<>("plain");
    Map<TupleTag<?>, Coder<?>> coders = new HashMap<>();
    coders.put(mainTag, schemaCoder);
    coders.put(sideTag, schemaCoder);
    coders.put(nonSchemaTag, StringUtf8Coder.of());

    List<DoFn.OutputReceiver<Row>> mainRowRefs = new ArrayList<>();
    List<DoFn.OutputReceiver<Row>> sideRowRefs = new ArrayList<>();
    DoFn<String, Row> rowFn =
        new DoFn<String, Row>() {
          @ProcessElement
          public void processElement(
              @Element String elem, OutputReceiver<Row> mainOut, MultiOutputReceiver multiOut) {
            Row row = Row.withSchema(schema).addValue(elem).build();
            mainOut.output(row);
            DoFn.OutputReceiver<Row> mr = multiOut.getRowReceiver(mainTag);
            DoFn.OutputReceiver<Row> sr = multiOut.getRowReceiver(sideTag);
            mainRowRefs.add(mr);
            sideRowRefs.add(sr);
            sr.output(row);
            assertThrows(IllegalStateException.class, () -> multiOut.getRowReceiver(nonSchemaTag));
            assertThrows(
                IllegalStateException.class,
                () -> multiOut.getRowReceiver(new TupleTag<Row>("missing")));
          }
        };
    ListOutputManager manager = new ListOutputManager();
    DoFnRunner<String, Row> runner =
        new SimpleDoFnRunner<>(
            null,
            rowFn,
            NullSideInputReader.empty(),
            manager,
            mainTag,
            Arrays.asList(sideTag, nonSchemaTag),
            mockStepContext,
            null,
            coders,
            WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10))),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    IntervalWindow w1 = new IntervalWindow(Instant.ofEpochMilli(0), Instant.ofEpochMilli(10_000));
    IntervalWindow w2 =
        new IntervalWindow(Instant.ofEpochMilli(10_000), Instant.ofEpochMilli(20_000));
    runner.processElement(
        WindowedValues.of("a", Instant.ofEpochMilli(100), w1, PaneInfo.NO_FIRING));
    runner.processElement(
        WindowedValues.of("b", Instant.ofEpochMilli(10_100), w2, PaneInfo.NO_FIRING));
    assertSame(mainRowRefs.get(0), mainRowRefs.get(1));
    assertSame(sideRowRefs.get(0), sideRowRefs.get(1));
    assertEquals(2, manager.outputs.get(mainTag).size());
    assertEquals(2, manager.outputs.get(sideTag).size());
    assertEquals(Instant.ofEpochMilli(100), manager.outputs.get(sideTag).get(0).getTimestamp());
    assertEquals(Instant.ofEpochMilli(10_100), manager.outputs.get(sideTag).get(1).getTimestamp());

    // DirectOutputReceiver outputWithTimestamp, 4-arg and 5-arg outputWindowedValue, builder().
    TupleTag<String> strTag = new TupleTag<>("str");
    ListOutputManager strMgr = new ListOutputManager();
    PaneInfo earlyPane = PaneInfo.createPane(true, false, Timing.EARLY);
    DoFn<String, String> overloadFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String elem, OutputReceiver<String> out) {
            out.outputWithTimestamp(elem + ":ts", Instant.ofEpochMilli(200));
            out.outputWindowedValue(
                elem + ":w4", Instant.ofEpochMilli(300), Collections.singletonList(w1), earlyPane);
            out.outputWindowedValue(
                elem + ":w5",
                Instant.ofEpochMilli(400),
                Collections.singletonList(w2),
                earlyPane,
                ValueKind.DELETE);
            out.builder(elem + ":bld").setTimestamp(Instant.ofEpochMilli(500)).output();
          }
        };
    DoFnRunner<String, String> strRunner =
        new SimpleDoFnRunner<>(
            null,
            overloadFn,
            NullSideInputReader.empty(),
            strMgr,
            strTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10))),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    strRunner.processElement(
        WindowedValues.of("v", Instant.ofEpochMilli(100), w1, PaneInfo.NO_FIRING));
    List<WindowedValue<?>> outs = strMgr.outputs.get(strTag);
    assertEquals(4, outs.size());
    assertEquals("v:ts", outs.get(0).getValue());
    assertEquals(Instant.ofEpochMilli(200), outs.get(0).getTimestamp());
    assertEquals("v:w4", outs.get(1).getValue());
    assertEquals(earlyPane, outs.get(1).getPaneInfo());
    assertEquals("v:w5", outs.get(2).getValue());
    assertEquals(ValueKind.DELETE, outs.get(2).getValueKind());
    assertThat(outs.get(2).getWindows(), contains(w2));
    assertEquals("v:bld", outs.get(3).getValue());
    assertEquals(Instant.ofEpochMilli(500), outs.get(3).getTimestamp());
  }

  @Test
  public void testCheckTimestampSkewBoundaryEdgeCases() {
    TupleTag<String> mainTag = new TupleTag<>("main");

    // skew = Long.MAX_VALUE with elemMillis = -1 (no overflow) and -2 (overflow -> MIN_VALUE).
    DoFn<String, String> maxSkewFn =
        new DoFn<String, String>() {
          @Override
          @SuppressWarnings("deprecation")
          public Duration getAllowedTimestampSkew() {
            return Duration.millis(Long.MAX_VALUE);
          }

          @ProcessElement
          public void processElement(OutputReceiver<String> out) {
            out.outputWithTimestamp("min", BoundedWindow.TIMESTAMP_MIN_VALUE);
          }
        };
    ListOutputManager mgr = new ListOutputManager();
    DoFnRunner<String, String> maxSkewRunner =
        new SimpleDoFnRunner<>(
            null,
            maxSkewFn,
            NullSideInputReader.empty(),
            mgr,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    maxSkewRunner.processElement(
        WindowedValues.timestampedValueInGlobalWindow("a", Instant.ofEpochMilli(-1)));
    maxSkewRunner.processElement(
        WindowedValues.timestampedValueInGlobalWindow("b", Instant.ofEpochMilli(-2)));
    assertEquals(2, mgr.outputs.get(mainTag).size());

    // Negative allowedTimestampSkew must reject same-timestamp DirectOutputReceiver.output(v).
    DoFn<String, String> negSkewFn =
        new DoFn<String, String>() {
          @Override
          @SuppressWarnings("deprecation")
          public Duration getAllowedTimestampSkew() {
            return Duration.millis(-10);
          }

          @ProcessElement
          public void processElement(OutputReceiver<String> out) {
            out.output("should-fail");
          }
        };
    DoFnRunner<String, String> negRunner =
        new SimpleDoFnRunner<>(
            null,
            negSkewFn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    UserCodeException negEx =
        assertThrows(
            UserCodeException.class,
            () ->
                negRunner.processElement(
                    WindowedValues.timestampedValueInGlobalWindow("x", Instant.ofEpochMilli(100))));
    assertThat(negEx.getCause(), isA(IllegalArgumentException.class));

    // Timestamp past TIMESTAMP_MAX_VALUE rejected by output() on DirectOutputReceiver.
    DoFn<String, String> normalFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(OutputReceiver<String> out) {
            out.output("past-max");
          }
        };
    DoFnRunner<String, String> normalRunner =
        new SimpleDoFnRunner<>(
            null,
            normalFn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    Instant tooLate = BoundedWindow.TIMESTAMP_MAX_VALUE.plus(Duration.millis(1));
    UserCodeException maxEx =
        assertThrows(
            UserCodeException.class,
            () ->
                normalRunner.processElement(
                    WindowedValues.timestampedValueInGlobalWindow("x", tooLate)));
    assertThat(maxEx.getCause(), isA(IllegalArgumentException.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExplodedWindowsResetStateNamespaceAndOpenTelemetryScope() {
    TupleTag<String> mainTag = new TupleTag<>("main");
    InMemoryStateInternals<String> stateInternals = InMemoryStateInternals.forKey("k");
    when(mockStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);

    DoFn<String, String> statefulFn =
        new DoFn<String, String>() {
          @StateId("s")
          private final StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void processElement(
              @Element String elem,
              BoundedWindow window,
              @StateId("s") ValueState<String> state,
              OutputReceiver<String> out) {
            String prev = state.read();
            String label = prev == null ? elem : prev + "+" + elem;
            state.write(label);
            out.output(window.maxTimestamp().getMillis() + ":" + label);
          }
        };
    ListOutputManager mgr = new ListOutputManager();
    DoFnRunner<String, String> runner =
        new SimpleDoFnRunner<>(
            null,
            statefulFn,
            NullSideInputReader.empty(),
            mgr,
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            ImmutableMap.of(),
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    IntervalWindow w1 = new IntervalWindow(Instant.ofEpochMilli(0), Instant.ofEpochMilli(10));
    IntervalWindow w2 = new IntervalWindow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(20));
    // Multi-window element explodes across w1 and w2 with independent state namespaces.
    runner.processElement(
        WindowedValues.of("a", Instant.ofEpochMilli(5), Arrays.asList(w1, w2), PaneInfo.NO_FIRING));
    // Subsequent single-window element hits singleWindowOrNull fast path.
    runner.processElement(WindowedValues.of("b", Instant.ofEpochMilli(6), w1, PaneInfo.NO_FIRING));

    List<WindowedValue<?>> outs = mgr.outputs.get(mainTag);
    assertEquals(3, outs.size());
    assertEquals("9:a", outs.get(0).getValue());
    assertEquals("19:a", outs.get(1).getValue());
    assertEquals("9:a+b", outs.get(2).getValue());

    // OpenTelemetry context switching: null, same current context, and distinct span context.
    Context ctxA =
        Context.root()
            .with(
                Span.wrap(
                    SpanContext.create(
                        "aa000000000000000000000000000001",
                        "aa00000000000001",
                        TraceFlags.getSampled(),
                        TraceState.getDefault())));
    Context ctxB =
        Context.root()
            .with(
                Span.wrap(
                    SpanContext.create(
                        "bb000000000000000000000000000002",
                        "bb00000000000002",
                        TraceFlags.getSampled(),
                        TraceState.getDefault())));
    List<Context> seenContexts = new ArrayList<>();
    DoFn<String, String> otelFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(OutputReceiver<String> out) {
            seenContexts.add(Context.current());
            out.output("ok");
          }
        };
    DoFnRunner<String, String> otelRunner =
        new SimpleDoFnRunner<>(
            null,
            otelFn,
            NullSideInputReader.empty(),
            new ListOutputManager(),
            mainTag,
            Collections.emptyList(),
            mockStepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    try (Scope s = ctxA.makeCurrent()) {
      otelRunner.processElement(
          WindowedValues.of(
              "same",
              BoundedWindow.TIMESTAMP_MIN_VALUE,
              GlobalWindow.INSTANCE,
              PaneInfo.NO_FIRING,
              null,
              null,
              CausedByDrain.NORMAL,
              ctxA,
              ValueKind.INSERT));
      otelRunner.processElement(
          WindowedValues.of(
              "switch",
              BoundedWindow.TIMESTAMP_MIN_VALUE,
              GlobalWindow.INSTANCE,
              PaneInfo.NO_FIRING,
              null,
              null,
              CausedByDrain.NORMAL,
              ctxB,
              ValueKind.INSERT));
      assertSame(ctxA, Context.current());
    }
    assertSame(ctxA, seenContexts.get(0));
    assertSame(ctxB, seenContexts.get(1));
  }

  private static class ListOutputManager implements WindowedValueMultiReceiver {
    private final ListMultimap<TupleTag<?>, WindowedValue<?>> outputs = ArrayListMultimap.create();

    @Override
    public <OutputT> void output(TupleTag<OutputT> tag, WindowedValue<OutputT> output) {
      outputs.put(tag, output);
    }
  }
}
