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

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittableParDoViaKeyedWorkItems.ProcessFn}. */
@RunWith(JUnit4.class)
public class SplittableParDoProcessFnTest {
  private static final int MAX_OUTPUTS_PER_BUNDLE = 10000;
  private static final Duration MAX_BUNDLE_DURATION = Duration.standardSeconds(5);
  @Rule public final ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

  // ----------------- Tests for whether the transform sets boundedness correctly --------------
  private static class SomeRestriction
      implements Serializable, HasDefaultTracker<SomeRestriction, SomeRestrictionTracker> {
    @Override
    public SomeRestrictionTracker newTracker() {
      return new SomeRestrictionTracker(this);
    }
  }

  private static class SomeRestrictionTracker extends RestrictionTracker<SomeRestriction, Void> {
    private final SomeRestriction someRestriction;

    public SomeRestrictionTracker(SomeRestriction someRestriction) {
      this.someRestriction = someRestriction;
    }

    @Override
    public boolean tryClaim(Void position) {
      return true;
    }

    @Override
    public SomeRestriction currentRestriction() {
      return someRestriction;
    }

    @Override
    public SplitResult<SomeRestriction> trySplit(double fractionOfRemainder) {
      return SplitResult.of(null, someRestriction);
    }

    @Override
    public void checkDone() {}

    @Override
    public RestrictionTracker.IsBounded isBounded() {
      return RestrictionTracker.IsBounded.BOUNDED;
    }
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  /**
   * A helper for testing {@link ProcessFn} on 1 element (but possibly over multiple {@link
   * DoFn.ProcessElement} calls).
   */
  private static class ProcessFnTester<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      implements AutoCloseable {
    private final DoFnTester<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> tester;
    private Instant currentProcessingTime;

    private InMemoryTimerInternals timerInternals;
    private TestInMemoryStateInternals<String> stateInternals;

    ProcessFnTester(
        Instant currentProcessingTime,
        final DoFn<InputT, OutputT> fn,
        Coder<InputT> inputCoder,
        Coder<RestrictionT> restrictionCoder,
        Coder<WatermarkEstimatorStateT> watermarkEstimatorStateCoder,
        int maxOutputsPerBundle,
        Duration maxBundleDuration)
        throws Exception {
      // The exact windowing strategy doesn't matter in this test, but it should be able to
      // encode IntervalWindow's because that's what all tests here use.
      WindowingStrategy<InputT, BoundedWindow> windowingStrategy =
          (WindowingStrategy) WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(1)));
      final ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
          processFn =
              new ProcessFn<>(
                  fn,
                  inputCoder,
                  restrictionCoder,
                  watermarkEstimatorStateCoder,
                  windowingStrategy);
      this.tester = DoFnTester.of(processFn);
      this.timerInternals = new InMemoryTimerInternals();
      this.stateInternals = new TestInMemoryStateInternals<>("dummy");
      processFn.setStateInternalsFactory(key -> stateInternals);
      processFn.setTimerInternalsFactory(key -> timerInternals);
      processFn.setProcessElementInvoker(
          new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
              fn,
              tester.getPipelineOptions(),
              new OutputWindowedValueToDoFnTester<>(tester),
              new SideInputReader() {
                @Override
                public <T> T get(PCollectionView<T> view, BoundedWindow window) {
                  throw new NoSuchElementException();
                }

                @Override
                public <T> boolean contains(PCollectionView<T> view) {
                  return false;
                }

                @Override
                public boolean isEmpty() {
                  return true;
                }
              },
              Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
              maxOutputsPerBundle,
              maxBundleDuration));
      // Do not clone since ProcessFn references non-serializable DoFnTester itself
      // through the state/timer/output callbacks.
      this.tester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);
      this.tester.startBundle();
      timerInternals.advanceProcessingTime(currentProcessingTime);

      this.currentProcessingTime = currentProcessingTime;
    }

    @Override
    public void close() throws Exception {
      tester.close();
    }

    /** Performs a seed {@link DoFn.ProcessElement} call feeding the element and restriction. */
    void startElement(InputT element, RestrictionT restriction) throws Exception {
      startElement(
          WindowedValue.of(
              KV.of(element, restriction),
              currentProcessingTime,
              GlobalWindow.INSTANCE,
              PaneInfo.ON_TIME_AND_ONLY_FIRING));
    }

    void startElement(WindowedValue<KV<InputT, RestrictionT>> windowedValue) throws Exception {
      tester.processElement(
          KeyedWorkItems.elementsWorkItem(
              "key".getBytes(StandardCharsets.UTF_8), Collections.singletonList(windowedValue)));
    }

    /**
     * Advances processing time by a given duration and, if any timers fired, performs a non-seed
     * {@link DoFn.ProcessElement} call, feeding it the timers.
     */
    boolean advanceProcessingTimeBy(Duration duration) throws Exception {
      currentProcessingTime = currentProcessingTime.plus(duration);
      timerInternals.advanceProcessingTime(currentProcessingTime);

      List<TimerInternals.TimerData> timers = new ArrayList<>();
      TimerInternals.TimerData nextTimer;
      while ((nextTimer = timerInternals.removeNextProcessingTimer()) != null) {
        timers.add(nextTimer);
      }
      if (timers.isEmpty()) {
        return false;
      }
      tester.processElement(
          KeyedWorkItems.timersWorkItem("key".getBytes(StandardCharsets.UTF_8), timers));
      return true;
    }

    List<TimestampedValue<OutputT>> peekOutputElementsInWindow(BoundedWindow window) {
      return tester.peekOutputElementsInWindow(window);
    }

    List<OutputT> takeOutputElements() {
      return tester.takeOutputElements();
    }

    public Instant getWatermarkHold() {
      return stateInternals.earliestWatermarkHold();
    }
  }

  private static class OutputWindowedValueToDoFnTester<OutputT>
      implements OutputWindowedValue<OutputT> {
    private final DoFnTester<?, OutputT> tester;

    private OutputWindowedValueToDoFnTester(DoFnTester<?, OutputT> tester) {
      this.tester = tester;
    }

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      outputWindowedValue(tester.getMainOutputTag(), output, timestamp, windows, pane);
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      for (BoundedWindow window : windows) {
        tester.getMutableOutput(tag).add(ValueInSingleWindow.of(output, timestamp, window, pane));
      }
    }
  }

  /** A simple splittable {@link DoFn} that's actually monolithic. */
  private static class ToStringFn extends DoFn<Integer, String> {
    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<SomeRestriction, Void> tracker) {
      checkState(tracker.tryClaim(null));
      c.output(c.element().toString() + "a");
      c.output(c.element().toString() + "b");
      c.output(c.element().toString() + "c");
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element Integer elem) {
      return new SomeRestriction();
    }
  }

  @Test
  public void testTrivialProcessFnPropagatesOutputWindowAndTimestamp() throws Exception {
    // Tests that ProcessFn correctly propagates the window and timestamp of the element
    // inside the KeyedWorkItem.
    // The underlying DoFn is actually monolithic, so this doesn't test splitting.
    DoFn<Integer, String> fn = new ToStringFn();

    Instant base = Instant.now();

    IntervalWindow w =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(1)), base.plus(Duration.standardMinutes(1)));

    ProcessFnTester<Integer, String, SomeRestriction, Void, Void> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(SomeRestriction.class),
            VoidCoder.of(),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION);
    tester.startElement(
        WindowedValue.of(
            KV.of(42, new SomeRestriction()),
            base,
            Collections.singletonList(w),
            PaneInfo.ON_TIME_AND_ONLY_FIRING));

    assertEquals(
        Arrays.asList(
            TimestampedValue.of("42a", base),
            TimestampedValue.of("42b", base),
            TimestampedValue.of("42c", base)),
        tester.peekOutputElementsInWindow(w));
  }

  private static class WatermarkUpdateFn extends DoFn<Instant, String> {
    @ProcessElement
    public void process(
        ProcessContext c,
        RestrictionTracker<OffsetRange, Long> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator) {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        watermarkEstimator.setWatermark(c.element().plus(Duration.standardSeconds(i)));
        c.output(String.valueOf(i));
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element Instant elem) {
      throw new IllegalStateException("Expected to be supplied explicitly in this test");
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
      return new OffsetRangeTracker(range);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return GlobalWindow.TIMESTAMP_MIN_VALUE;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new WatermarkEstimators.Manual(watermarkEstimatorState);
    }
  }

  @Test
  public void testUpdatesWatermark() throws Exception {
    DoFn<Instant, String> fn = new WatermarkUpdateFn();
    Instant base = Instant.now();

    ProcessFnTester<Instant, String, OffsetRange, Long, Instant> tester =
        new ProcessFnTester<>(
            base,
            fn,
            InstantCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            InstantCoder.of(),
            3,
            MAX_BUNDLE_DURATION);

    tester.startElement(base, new OffsetRange(0, 8));
    assertThat(tester.takeOutputElements(), hasItems("0", "1", "2"));
    assertEquals(base.plus(Duration.standardSeconds(2)), tester.getWatermarkHold());

    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), hasItems("3", "4", "5"));
    assertEquals(base.plus(Duration.standardSeconds(5)), tester.getWatermarkHold());

    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), hasItems("6", "7"));
    assertEquals(null, tester.getWatermarkHold());
  }

  /** A simple splittable {@link DoFn} that outputs the given element every 5 seconds forever. */
  private static class SelfInitiatedResumeFn extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<SomeRestriction, Void> tracker) {
      checkState(tracker.tryClaim(null));
      c.output(c.element().toString());
      return resume().withResumeDelay(Duration.standardSeconds(5));
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element Integer elem) {
      return new SomeRestriction();
    }
  }

  @Test
  public void testResumeSetsTimer() throws Exception {
    DoFn<Integer, String> fn = new SelfInitiatedResumeFn();
    Instant base = Instant.now();
    dateTimeProvider.setDateTimeFixed(base.getMillis());
    ProcessFnTester<Integer, String, SomeRestriction, Void, Void> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(SomeRestriction.class),
            VoidCoder.of(),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION);

    tester.startElement(42, new SomeRestriction());
    assertThat(tester.takeOutputElements(), contains("42"));

    // Should resume after 5 seconds: advancing by 3 seconds should have no effect.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertTrue(tester.takeOutputElements().isEmpty());

    // 6 seconds should be enough  should invoke the fn again.
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertThat(tester.takeOutputElements(), contains("42"));

    // Should again resume after 5 seconds: advancing by 3 seconds should again have no effect.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertTrue(tester.takeOutputElements().isEmpty());

    // 6 seconds should again be enough.
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertThat(tester.takeOutputElements(), contains("42"));
  }

  /** A splittable {@link DoFn} that generates the sequence [init, init + total). */
  private static class CounterFn extends DoFn<Integer, String> {
    private final int numOutputsPerCall;

    public CounterFn(int numOutputsPerCall) {
      this.numOutputsPerCall = numOutputsPerCall;
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        c.output(String.valueOf(c.element() + i));
        if (numIterations == numOutputsPerCall - 1) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element Integer elem) {
      throw new UnsupportedOperationException("Expected to be supplied explicitly in this test");
    }
  }

  @Test
  public void testResumeCarriesOverState() throws Exception {
    DoFn<Integer, String> fn = new CounterFn(1);
    Instant base = Instant.now();
    dateTimeProvider.setDateTimeFixed(base.getMillis());
    ProcessFnTester<Integer, String, OffsetRange, Long, Void> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            VoidCoder.of(),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION);

    tester.startElement(42, new OffsetRange(0, 3));
    assertThat(tester.takeOutputElements(), contains("42"));
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), contains("43"));
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), contains("44"));
    // Should not resume the null residual.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    // After outputting all 3 items, should not output anything more.
    assertEquals(0, tester.takeOutputElements().size());
    // Should also not ask to resume.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
  }

  @Test
  public void testCheckpointsAfterNumOutputs() throws Exception {
    int max = 100;
    DoFn<Integer, String> fn = new CounterFn(Integer.MAX_VALUE);
    Instant base = Instant.now();
    int baseIndex = 42;

    ProcessFnTester<Integer, String, OffsetRange, Long, Void> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            VoidCoder.of(),
            max,
            MAX_BUNDLE_DURATION);

    List<String> elements;

    // Create an fn that attempts to 2x output more than checkpointing allows.
    tester.startElement(baseIndex, new OffsetRange(0, 2 * max + max / 2));
    elements = tester.takeOutputElements();
    assertEquals(max, elements.size());
    // Should output the range [0, max)
    assertThat(elements, hasItem(String.valueOf(baseIndex)));
    assertThat(elements, hasItem(String.valueOf(baseIndex + max - 1)));

    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    elements = tester.takeOutputElements();
    assertEquals(max, elements.size());
    // Should output the range [max, 2*max)
    assertThat(elements, hasItem(String.valueOf(baseIndex + max)));
    assertThat(elements, hasItem(String.valueOf(baseIndex + 2 * max - 1)));

    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    elements = tester.takeOutputElements();
    assertEquals(max / 2, elements.size());
    // Should output the range [2*max, 2*max + max/2)
    assertThat(elements, hasItem(String.valueOf(baseIndex + 2 * max)));
    assertThat(elements, hasItem(String.valueOf(baseIndex + 2 * max + max / 2 - 1)));
    assertThat(elements, not(hasItem(String.valueOf(baseIndex + 2 * max + max / 2))));
  }

  @Test
  public void testCheckpointsAfterDuration() throws Exception {
    // Don't bound number of outputs.
    int max = Integer.MAX_VALUE;
    // But bound bundle duration - the bundle should terminate.
    Duration maxBundleDuration = Duration.standardSeconds(1);
    // Create an fn that attempts to 2x output more than checkpointing allows.
    DoFn<Integer, String> fn = new CounterFn(Integer.MAX_VALUE);
    Instant base = Instant.now();
    int baseIndex = 42;

    ProcessFnTester<Integer, String, OffsetRange, Long, Void> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            VoidCoder.of(),
            max,
            maxBundleDuration);

    List<String> elements;

    tester.startElement(baseIndex, new OffsetRange(0, Long.MAX_VALUE));
    // Bundle should terminate, and should do at least some processing.
    elements = tester.takeOutputElements();
    assertFalse(elements.isEmpty());
    // Bundle should have run for at least the requested duration.
    assertThat(
        Instant.now().getMillis() - base.getMillis(),
        greaterThanOrEqualTo(maxBundleDuration.getMillis()));
  }

  private static class LifecycleVerifyingFn extends DoFn<Integer, String> {
    private enum State {
      BEFORE_SETUP,
      OUTSIDE_BUNDLE,
      INSIDE_BUNDLE,
      TORN_DOWN
    }

    private State state = State.BEFORE_SETUP;

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<SomeRestriction, Void> tracker) {
      assertEquals(State.INSIDE_BUNDLE, state);
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element Integer element) {
      return new SomeRestriction();
    }

    @Setup
    public void setup() {
      assertEquals(State.BEFORE_SETUP, state);
      state = State.OUTSIDE_BUNDLE;
    }

    @Teardown
    public void tearDown() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.TORN_DOWN;
    }

    @StartBundle
    public void startBundle() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.INSIDE_BUNDLE;
    }

    @FinishBundle
    public void finishBundle() {
      assertEquals(State.INSIDE_BUNDLE, state);
      state = State.OUTSIDE_BUNDLE;
    }
  }

  @Test
  public void testInvokesLifecycleMethods() throws Exception {
    DoFn<Integer, String> fn = new LifecycleVerifyingFn();
    try (ProcessFnTester<Integer, String, SomeRestriction, Void, Void> tester =
        new ProcessFnTester<>(
            Instant.now(),
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(SomeRestriction.class),
            VoidCoder.of(),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION)) {
      tester.startElement(42, new SomeRestriction());
    }
  }
}
