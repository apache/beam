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
package org.apache.beam.sdk;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.DevTestInterface;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.*;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ParDo. */
public class UserTimerTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  /** Shared base test base with setup/teardown helpers. */
  public abstract static class SharedTestBase {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule public transient ExpectedException thrown = ExpectedException.none();
  }

  private static class PrintingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(
        @Element String element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        OutputReceiver<String> receiver) {
      receiver.output(
          element + ":" + timestamp.getMillis() + ":" + window.maxTimestamp().getMillis());
    }
  }

  static class TestNoOutputDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(DoFn<Integer, String>.ProcessContext c) throws Exception {}
  }

  static class TestDoFn extends DoFn<Integer, String> {
    enum State {
      NOT_SET_UP,
      UNSTARTED,
      STARTED,
      PROCESSING,
      FINISHED
    }

    State state = State.NOT_SET_UP;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> additionalOutputTupleTags = new ArrayList<>();

    public TestDoFn() {}

    public TestDoFn(
        List<PCollectionView<Integer>> sideInputViews,
        List<TupleTag<String>> additionalOutputTupleTags) {
      this.sideInputViews.addAll(sideInputViews);
      this.additionalOutputTupleTags.addAll(additionalOutputTupleTags);
    }

    @Setup
    public void prepare() {
      assertEquals(State.NOT_SET_UP, state);
      state = State.UNSTARTED;
    }

    @StartBundle
    public void startBundle() {
      assertThat(state, anyOf(equalTo(State.UNSTARTED), equalTo(State.FINISHED)));

      state = State.STARTED;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element Integer element) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      outputToAllWithSideInputs(c, "processing: " + element);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.FINISHED;
      c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(
            additionalOutputTupleTag,
            additionalOutputTupleTag.getId() + ": " + "finished",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE);
      }
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(additionalOutputTupleTag, additionalOutputTupleTag.getId() + ": " + value);
      }
    }
  }

  static class TestStartBatchErrorDoFn extends DoFn<Integer, String> {
    @StartBundle
    public void startBundle() {
      throw new RuntimeException("test error in initialize");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // This has to be here.
    }
  }

  static class TestProcessElementErrorDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      throw new RuntimeException("test error in process");
    }
  }

  static class TestFinishBatchErrorDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // This has to be here.
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      throw new RuntimeException("test error in finalize");
    }
  }

  private static class StrangelyNamedDoer extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  static class TestOutputTimestampDoFn<T extends Number> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T value, OutputReceiver<T> r) {
      r.outputWithTimestamp(value, new Instant(value.longValue()));
    }
  }

  static class TestShiftTimestampDoFn<T extends Number> extends DoFn<T, T> {
    private Duration allowedTimestampSkew;
    private Duration durationToShift;

    public TestShiftTimestampDoFn(Duration allowedTimestampSkew, Duration durationToShift) {
      this.allowedTimestampSkew = allowedTimestampSkew;
      this.durationToShift = durationToShift;
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedTimestampSkew;
    }

    @ProcessElement
    public void processElement(
        @Element T value, @Timestamp Instant timestamp, OutputReceiver<T> r) {
      checkNotNull(timestamp);
      r.outputWithTimestamp(value, timestamp.plus(durationToShift));
    }
  }

  static class TestFormatTimestampDoFn<T extends Number> extends DoFn<T, String> {
    @ProcessElement
    public void processElement(
        @Element T element, @Timestamp Instant timestamp, OutputReceiver<String> r) {
      checkNotNull(timestamp);
      r.output("processing: " + element + ", timestamp: " + timestamp.getMillis());
    }
  }

  static class MultiFilter extends PTransform<PCollection<Integer>, PCollectionTuple> {

    private static final TupleTag<Integer> BY2 = new TupleTag<Integer>("by2") {};
    private static final TupleTag<Integer> BY3 = new TupleTag<Integer>("by3") {};

    @Override
    public PCollectionTuple expand(PCollection<Integer> input) {
      PCollection<Integer> by2 = input.apply("Filter2s", ParDo.of(new FilterFn(2)));
      PCollection<Integer> by3 = input.apply("Filter3s", ParDo.of(new FilterFn(3)));
      return PCollectionTuple.of(BY2, by2).and(BY3, by3);
    }

    static class FilterFn extends DoFn<Integer, Integer> {
      private final int divisor;

      FilterFn(int divisor) {
        this.divisor = divisor;
      }

      @ProcessElement
      public void processElement(@Element Integer element, OutputReceiver<Integer> r)
          throws Exception {
        if (element % divisor == 0) {
          r.output(element);
        }
      }
    }
  }

  /** Tests to validate ParDo timers. */
  @RunWith(JUnit4.class)
  public static class TimerTests extends SharedTestBase implements Serializable {
    @Test
    public void testTimerNotKeyed() {
      final String timerId = "foo";

      DoFn<String, Integer> fn =
          new DoFn<String, Integer>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext c, @TimerId(timerId) Timer timer) {}

            @OnTimer(timerId)
            public void onTimer() {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("timer");
      thrown.expectMessage("KvCoder");

      pipeline.apply(Create.of("hello", "goodbye", "hello again")).apply(ParDo.of(fn));
    }

    @Test
    public void testTimerNotDeterministic() {
      final String timerId = "foo";

      // DoubleCoder is not deterministic, so this should crash
      DoFn<KV<Double, String>, Integer> fn =
          new DoFn<KV<Double, String>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext c, @TimerId(timerId) Timer timer) {}

            @OnTimer(timerId)
            public void onTimer() {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("timer");
      thrown.expectMessage("deterministic");

      pipeline
          .apply(Create.of(KV.of(1.0, "hello"), KV.of(5.4, "goodbye"), KV.of(7.2, "hello again")))
          .apply(ParDo.of(fn));
    }

    /**
     * Tests that an event time timer fires and results in supplementary output.
     *
     * <p>This test relies on two properties:
     *
     * <ol>
     *   <li>A timer that is set on time should always get a chance to fire. For this to be true,
     *       timers per-key-and-window must be delivered in order so the timer is not wiped out
     *       until the window is expired by the runner.
     *   <li>A {@link Create} transform sends its elements on time, and later advances the watermark
     *       to infinity
     * </ol>
     *
     * <p>Note that {@link TestStream} is not applicable because it requires very special runner
     * hooks and is only supported by the direct runner.
     */
    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testEventTimeTimerBounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                r.output(42);
              }
            }
          };

      PCollection<Integer> output =
          pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    /**
     * Tests a GBK followed immediately by a {@link ParDo} that users timers. This checks a common
     * case where both GBK and the user code share a timer delivery bundle.
     */
    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testGbkFollowedByUserTimers() throws Exception {

      DoFn<KV<String, Iterable<Integer>>, Integer> fn =
          new DoFn<KV<String, Iterable<Integer>>, Integer>() {

            public static final String TIMER_ID = "foo";

            @TimerId(TIMER_ID)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(TIMER_ID) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(TIMER_ID)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                r.output(42);
              }
            }
          };

      PCollection<Integer> output =
          pipeline
              .apply(Create.of(KV.of("hello", 37)))
              .apply(GroupByKey.create())
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testEventTimeTimerAlignBounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
          new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<Integer, Instant>> r) {
              timer.align(Duration.standardSeconds(1)).offset(Duration.millis(1)).setRelative();
              r.output(KV.of(3, timestamp));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
              r.output(KV.of(42, timestamp));
            }
          };

      PCollection<KV<Integer, Instant>> output =
          pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3, BoundedWindow.TIMESTAMP_MIN_VALUE),
              KV.of(42, BoundedWindow.TIMESTAMP_MIN_VALUE.plus(1774)));
      pipeline.run();
    }

    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testTimerReceivedInOriginalWindow() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, BoundedWindow> fn =
          new DoFn<KV<String, Integer>, BoundedWindow>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
            }

            @OnTimer(timerId)
            public void onTimer(BoundedWindow window, OutputReceiver<BoundedWindow> r) {
              r.output(window);
            }

            @Override
            public TypeDescriptor<BoundedWindow> getOutputTypeDescriptor() {
              return (TypeDescriptor) TypeDescriptor.of(IntervalWindow.class);
            }
          };

      SlidingWindows windowing =
          SlidingWindows.of(Duration.standardMinutes(3)).every(Duration.standardMinutes(1));
      PCollection<BoundedWindow> output =
          pipeline
              .apply(Create.timestamped(TimestampedValue.of(KV.of("hello", 24), new Instant(0L))))
              .apply(Window.into(windowing))
              .apply(ParDo.of(fn));

      PAssert.that(output)
          .containsInAnyOrder(
              new IntervalWindow(new Instant(0), Duration.standardMinutes(3)),
              new IntervalWindow(
                  new Instant(0).minus(Duration.standardMinutes(1)), Duration.standardMinutes(3)),
              new IntervalWindow(
                  new Instant(0).minus(Duration.standardMinutes(2)), Duration.standardMinutes(3)));
      pipeline.run();
    }

    /**
     * Tests that an event time timer set absolutely for the last possible moment fires and results
     * in supplementary output. The test is otherwise identical to {@link
     * #testEventTimeTimerBounded()}.
     */
    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testEventTimeTimerAbsolute() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer, BoundedWindow window, OutputReceiver<Integer> r) {
              timer.set(window.maxTimestamp());
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<Integer> r) {
              r.output(42);
            }
          };

      PCollection<Integer> output =
          pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Ignore(
        "https://issues.apache.org/jira/browse/BEAM-2791, "
            + "https://issues.apache.org/jira/browse/BEAM-2535")
    @Test
    @Category({DevTestInterface.class, UsesStatefulParDo.class, UsesTimersInParDo.class})
    public void testEventTimeTimerLoop() {
      final String stateId = "count";
      final String timerId = "timer";
      final int loopCount = 5;

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec loopSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> countState,
                @TimerId(timerId) Timer loopTimer) {
              loopTimer.offset(Duration.millis(1)).setRelative();
            }

            @OnTimer(timerId)
            public void onLoopTimer(
                @StateId(stateId) ValueState<Integer> countState,
                @TimerId(timerId) Timer loopTimer,
                OutputReceiver<Integer> r) {
              int count = MoreObjects.firstNonNull(countState.read(), 0);
              if (count < loopCount) {
                r.output(count);
                countState.write(count + 1);
                loopTimer.offset(Duration.millis(1)).setRelative();
              }
            }
          };

      PCollection<Integer> output =
          pipeline.apply(Create.of(KV.of("hello", 42))).apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(0, 1, 2, 3, 4);
      pipeline.run();
    }

    /**
     * Tests that event time timers for multiple keys both fire. This particularly exercises
     * implementations that may GC in ways not simply governed by the watermark.
     */
    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testEventTimeTimerMultipleKeys() throws Exception {
      final String timerId = "foo";
      final String stateId = "sizzle";

      final int offset = 5000;
      final int timerOutput = 4093;

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
              timer.set(window.maxTimestamp());
              state.write(context.element().getKey());
              context.output(
                  KV.of(context.element().getKey(), context.element().getValue() + offset));
            }

            @OnTimer(timerId)
            public void onTimer(
                @StateId(stateId) ValueState<String> state, OutputReceiver<KV<String, Integer>> r) {
              r.output(KV.of(state.read(), timerOutput));
            }
          };

      // Enough keys that we exercise interesting code paths
      int numKeys = 50;
      List<KV<String, Integer>> input = new ArrayList<>();
      List<KV<String, Integer>> expectedOutput = new ArrayList<>();

      for (Integer key = 0; key < numKeys; ++key) {
        // Each key should have just one final output at GC time
        expectedOutput.add(KV.of(key.toString(), timerOutput));

        for (int i = 0; i < 15; ++i) {
          // Each input should be output with the offset added
          input.add(KV.of(key.toString(), i));
          expectedOutput.add(KV.of(key.toString(), i + offset));
        }
      }

      Collections.shuffle(input);

      PCollection<KV<String, Integer>> output =
          pipeline.apply(Create.of(input)).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(expectedOutput);
      pipeline.run();
    }

    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testAbsoluteProcessingTimeTimerRejected() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer) {
              try {
                timer.set(new Instant(0));
                fail("Should have failed due to processing time with absolute timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();
                List<String> expectedSubstrings =
                    Arrays.asList("relative timers", "processing time");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testOutOfBoundsEventTimeTimer() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                ProcessContext context, BoundedWindow window, @TimerId(timerId) Timer timer) {
              try {
                timer.set(window.maxTimestamp().plus(1L));
                fail("Should have failed due to processing time with absolute timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();
                List<String> expectedSubstrings = Arrays.asList("event time timer", "expiration");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testSimpleProcessingTimerTimer() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.PROCESSING_TIME)) {
                r.output(42);
              }
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .addElements(KV.of("hello", 37))
              .advanceProcessingTime(Duration.standardSeconds(2))
              .advanceWatermarkToInfinity();

      PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<Integer> r) {
              r.output(42);
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .advanceWatermarkTo(new Instant(0))
              .addElements(KV.of("hello", 37))
              .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(1)))
              .advanceWatermarkToInfinity();

      PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerAlignUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
          new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<Integer, Instant>> r) {
              timer.align(Duration.standardSeconds(1)).offset(Duration.millis(1)).setRelative();
              r.output(KV.of(3, timestamp));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
              r.output(KV.of(42, timestamp));
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .advanceWatermarkTo(new Instant(5))
              .addElements(KV.of("hello", 37))
              .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(1).plus(1)))
              .advanceWatermarkToInfinity();

      PCollection<KV<Integer, Instant>> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3, new Instant(5)),
              KV.of(42, new Instant(Duration.standardSeconds(1).minus(1).getMillis())));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerAlignAfterGcTimeUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
          new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              // This aligned time will exceed the END_OF_GLOBAL_WINDOW
              timer.align(Duration.standardDays(1)).setRelative();
              context.output(KV.of(3, context.timestamp()));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
              r.output(KV.of(42, timestamp));
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              // See GlobalWindow,
              // END_OF_GLOBAL_WINDOW is TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1))
              .advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1)))
              .addElements(KV.of("hello", 37))
              .advanceWatermarkToInfinity();

      PCollection<KV<Integer, Instant>> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1))),
              KV.of(42, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1))));
      pipeline.run();
    }

    /**
     * A test makes sure that a processing time timer should reset rather than creating duplicate
     * timers when a "set" method is called on it before it goes off.
     */
    @Test
    @Category({DevTestInterface.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testProcessingTimeTimerCanBeReset() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              context.output(context.element().getValue());
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<String> r) {
              r.output("timer_output");
            }
          };

      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .addElements(KV.of("key", "input1"))
              .addElements(KV.of("key", "input2"))
              .advanceProcessingTime(Duration.standardSeconds(2))
              .advanceWatermarkToInfinity();

      PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
      // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
      // "timer_output" is outputted. Therefore, the timer is overwritten.
      PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
      pipeline.run();
    }

    /**
     * A test makes sure that an event time timer should reset rather than creating duplicate timers
     * when a "set" method is called on it before it goes off.
     */
    @Test
    @Category({DevTestInterface.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerCanBeReset() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              context.output(context.element().getValue());
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<String> r) {
              r.output("timer_output");
            }
          };

      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .advanceWatermarkTo(new Instant(0))
              .addElements(KV.of("hello", "input1"))
              .addElements(KV.of("hello", "input2"))
              .advanceWatermarkToInfinity();

      PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
      // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
      // "timer_output" is outputted. Therefore, the timer is overwritten.
      PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
      pipeline.run();
    }

    @Test
    @Category({
      DevTestInterface.class,
      UsesTimersInParDo.class,
    })
    public void testPipelineOptionsParameterOnTimer() {
      final String timerId = "thisTimer";

      PCollection<String> results =
          pipeline
              .apply(Create.of(KV.of(0, 0)))
              .apply(
                  ParDo.of(
                      new DoFn<KV<Integer, Integer>, String>() {
                        @TimerId(timerId)
                        private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                        @ProcessElement
                        public void process(
                            ProcessContext c, BoundedWindow w, @TimerId(timerId) Timer timer) {
                          timer.set(w.maxTimestamp());
                        }

                        @OnTimer(timerId)
                        public void onTimer(OutputReceiver<String> r, PipelineOptions options) {
                          r.output(options.as(MyOptions.class).getFakeOption());
                        }
                      }));

      String testOptionValue = "not fake anymore";
      pipeline.getOptions().as(MyOptions.class).setFakeOption(testOptionValue);
      PAssert.that(results).containsInAnyOrder("not fake anymore");

      pipeline.run();
    }

    @Test
    @Category({DevTestInterface.class, UsesTestStream.class})
    public void duplicateTimerSetting() {
      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .addElements(KV.of("key1", "v1"))
              .advanceWatermarkToInfinity();

      PCollection<String> result = pipeline.apply(stream).apply(ParDo.of(new TwoTimerDoFn()));
      PAssert.that(result).containsInAnyOrder("It works");

      pipeline.run().waitUntilFinish();
    }
  }

  private static class FnWithSideInputs extends DoFn<String, String> {
    private final PCollectionView<Integer> view;

    private FnWithSideInputs(PCollectionView<Integer> view) {
      this.view = view;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element String element) {
      c.output(element + ":" + c.sideInput(view));
    }
  }

  private static class TestDummy {}

  private static class TestDummyCoder extends AtomicCoder<TestDummy> {
    private TestDummyCoder() {}

    private static final TestDummyCoder INSTANCE = new TestDummyCoder();

    @JsonCreator
    public static TestDummyCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(TestDummy value, OutputStream outStream)
        throws CoderException, IOException {}

    @Override
    public TestDummy decode(InputStream inStream) throws CoderException, IOException {
      return new TestDummy();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(TestDummy value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(TestDummy value, ElementByteSizeObserver observer)
        throws Exception {
      observer.update(0L);
    }

    @Override
    public void verifyDeterministic() {}
  }

  private static class TaggedOutputDummyFn extends DoFn<Integer, Integer> {
    private TupleTag<Integer> mainOutputTag;
    private TupleTag<TestDummy> dummyOutputTag;

    public TaggedOutputDummyFn(
        TupleTag<Integer> mainOutputTag, TupleTag<TestDummy> dummyOutputTag) {
      this.mainOutputTag = mainOutputTag;
      this.dummyOutputTag = dummyOutputTag;
    }

    @ProcessElement
    public void processElement(MultiOutputReceiver r) {
      r.get(mainOutputTag).output(1);
      r.get(dummyOutputTag).output(new TestDummy());
    }
  }

  private static class MainOutputDummyFn extends DoFn<Integer, TestDummy> {
    private TupleTag<TestDummy> mainOutputTag;
    private TupleTag<Integer> intOutputTag;

    public MainOutputDummyFn(TupleTag<TestDummy> mainOutputTag, TupleTag<Integer> intOutputTag) {
      this.mainOutputTag = mainOutputTag;
      this.intOutputTag = intOutputTag;
    }

    @ProcessElement
    public void processElement(MultiOutputReceiver r) {
      r.get(mainOutputTag).output(new TestDummy());
      r.get(intOutputTag).output(1);
    }
  }

  private static class MyInteger implements Comparable<MyInteger> {
    private final int value;

    MyInteger(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MyInteger)) {
        return false;
      }

      MyInteger myInteger = (MyInteger) o;

      return value == myInteger.value;
    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public int compareTo(MyInteger o) {
      return Integer.compare(this.getValue(), o.getValue());
    }

    @Override
    public String toString() {
      return "MyInteger{" + "value=" + value + '}';
    }
  }

  private static class MyIntegerCoder extends AtomicCoder<MyInteger> {
    private static final MyIntegerCoder INSTANCE = new MyIntegerCoder();

    private final VarIntCoder delegate = VarIntCoder.of();

    public static MyIntegerCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(MyInteger value, OutputStream outStream) throws CoderException, IOException {
      delegate.encode(value.getValue(), outStream);
    }

    @Override
    public MyInteger decode(InputStream inStream) throws CoderException, IOException {
      return new MyInteger(delegate.decode(inStream));
    }
  }

  /** PAssert "matcher" for expected output. */
  static class HasExpectedOutput
      implements SerializableFunction<Iterable<String>, Void>, Serializable {
    private final List<Integer> inputs;
    private final List<Integer> sideInputs;
    private final String additionalOutput;

    public static HasExpectedOutput forInput(List<Integer> inputs) {
      return new HasExpectedOutput(new ArrayList<>(inputs), new ArrayList<>(), "");
    }

    private HasExpectedOutput(
        List<Integer> inputs, List<Integer> sideInputs, String additionalOutput) {
      this.inputs = inputs;
      this.sideInputs = sideInputs;
      this.additionalOutput = additionalOutput;
    }

    public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
      return new HasExpectedOutput(inputs, Arrays.asList(sideInputValues), additionalOutput);
    }

    public HasExpectedOutput fromOutput(TupleTag<String> outputTag) {
      return fromOutput(outputTag.getId());
    }

    public HasExpectedOutput fromOutput(String outputId) {
      return new HasExpectedOutput(inputs, sideInputs, outputId);
    }

    @Override
    public Void apply(Iterable<String> outputs) {
      List<String> processeds = new ArrayList<>();
      List<String> finisheds = new ArrayList<>();
      for (String output : outputs) {
        if (output.contains("finished")) {
          finisheds.add(output);
        } else {
          processeds.add(output);
        }
      }

      String sideInputsSuffix;
      if (sideInputs.isEmpty()) {
        sideInputsSuffix = "";
      } else {
        sideInputsSuffix = ": " + sideInputs;
      }

      String additionalOutputPrefix;
      if (additionalOutput.isEmpty()) {
        additionalOutputPrefix = "";
      } else {
        additionalOutputPrefix = additionalOutput + ": ";
      }

      List<String> expectedProcesseds = new ArrayList<>();
      for (Integer input : inputs) {
        expectedProcesseds.add(additionalOutputPrefix + "processing: " + input + sideInputsSuffix);
      }
      String[] expectedProcessedsArray =
          expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
      assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));

      for (String finished : finisheds) {
        assertEquals(additionalOutputPrefix + "finished", finished);
      }

      return null;
    }
  }

  private static class Checker implements SerializableFunction<Iterable<String>, Void> {
    @Override
    public Void apply(Iterable<String> input) {
      boolean foundElement = false;
      boolean foundFinish = false;
      for (String str : input) {
        if ("elem:1:1".equals(str)) {
          if (foundElement) {
            throw new AssertionError("Received duplicate element");
          }
          foundElement = true;
        } else if ("finish:3:3".equals(str)) {
          foundFinish = true;
        } else {
          throw new AssertionError("Got unexpected value: " + str);
        }
      }
      if (!foundElement) {
        throw new AssertionError("Missing \"elem:1:1\"");
      }
      if (!foundFinish) {
        throw new AssertionError("Missing \"finish:3:3\"");
      }

      return null;
    }
  }

  /** A {@link PipelineOptions} subclass for testing passing to a {@link DoFn}. */
  public interface MyOptions extends PipelineOptions {
    @Default.String("fake option")
    String getFakeOption();

    void setFakeOption(String value);
  }

  private static class TwoTimerDoFn extends DoFn<KV<String, String>, String> {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(ProcessContext c, @TimerId("timer") Timer timer) {
      timer.offset(Duration.standardMinutes(10)).setRelative();
      timer.offset(Duration.standardMinutes(30)).setRelative();
    }

    @OnTimer("timer")
    public void onTimer(OutputReceiver<String> r, @TimerId("timer") Timer timer) {
      r.output("It works");
    }
  }
}
