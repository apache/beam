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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittableParDo}. */
@RunWith(JUnit4.class)
public class SplittableParDoTest {
  private static final int MAX_OUTPUTS_PER_BUNDLE = 10000;
  private static final Duration MAX_BUNDLE_DURATION = Duration.standardSeconds(5);

  // ----------------- Tests for whether the transform sets boundedness correctly --------------
  private static class SomeRestriction
      implements Serializable, HasDefaultTracker<SomeRestriction, SomeRestrictionTracker> {
    @Override
    public SomeRestrictionTracker newTracker() {
      return new SomeRestrictionTracker(this);
    }
  }

  private static class SomeRestrictionTracker implements RestrictionTracker<SomeRestriction> {
    private final SomeRestriction someRestriction;

    public SomeRestrictionTracker(SomeRestriction someRestriction) {
      this.someRestriction = someRestriction;
    }

    @Override
    public SomeRestriction currentRestriction() {
      return someRestriction;
    }

    @Override
    public SomeRestriction checkpoint() {
      return someRestriction;
    }

    @Override
    public void checkDone() {}
  }

  @BoundedPerElement
  private static class BoundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext context, SomeRestrictionTracker tracker) {}

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }
  }

  @UnboundedPerElement
  private static class UnboundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext context, SomeRestrictionTracker tracker) {}

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }
  }

  private static PCollection<Integer> makeUnboundedCollection(Pipeline pipeline) {
    return pipeline
        .apply("unbounded", Create.of(1, 2, 3))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
  }

  private static PCollection<Integer> makeBoundedCollection(Pipeline pipeline) {
    return pipeline
        .apply("bounded", Create.of(1, 2, 3))
        .setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
  }

  private static final TupleTag<String> MAIN_OUTPUT_TAG = new TupleTag<String>() {};

  private ParDo.MultiOutput<Integer, String> makeParDo(DoFn<Integer, String> fn) {
    return ParDo.of(fn).withOutputTags(MAIN_OUTPUT_TAG, TupleTagList.empty());
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBoundednessForBoundedFn() {
    pipeline.enableAbandonedNodeEnforcement(false);

    DoFn<Integer, String> boundedFn = new BoundedFakeFn();
    assertEquals(
        "Applying a bounded SDF to a bounded collection produces a bounded collection",
        PCollection.IsBounded.BOUNDED,
        makeBoundedCollection(pipeline)
            .apply("bounded to bounded", new SplittableParDo<>(makeParDo(boundedFn)))
            .get(MAIN_OUTPUT_TAG)
            .isBounded());
    assertEquals(
        "Applying a bounded SDF to an unbounded collection produces an unbounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeUnboundedCollection(pipeline)
            .apply("bounded to unbounded", new SplittableParDo<>(makeParDo(boundedFn)))
            .get(MAIN_OUTPUT_TAG)
            .isBounded());
  }

  @Test
  public void testBoundednessForUnboundedFn() {
    pipeline.enableAbandonedNodeEnforcement(false);

    DoFn<Integer, String> unboundedFn = new UnboundedFakeFn();
    assertEquals(
        "Applying an unbounded SDF to a bounded collection produces a bounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeBoundedCollection(pipeline)
            .apply("unbounded to bounded", new SplittableParDo<>(makeParDo(unboundedFn)))
            .get(MAIN_OUTPUT_TAG)
            .isBounded());
    assertEquals(
        "Applying an unbounded SDF to an unbounded collection produces an unbounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeUnboundedCollection(pipeline)
            .apply("unbounded to unbounded", new SplittableParDo<>(makeParDo(unboundedFn)))
            .get(MAIN_OUTPUT_TAG)
            .isBounded());
  }

  // ------------------------------- Tests for ProcessFn ---------------------------------

  /**
   * A helper for testing {@link SplittableParDo.ProcessFn} on 1 element (but possibly over multiple
   * {@link DoFn.ProcessElement} calls).
   */
  private static class ProcessFnTester<
          InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      implements AutoCloseable {
    private final DoFnTester<
            KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
        tester;
    private Instant currentProcessingTime;

    private InMemoryTimerInternals timerInternals;
    private TestInMemoryStateInternals<String> stateInternals;

    ProcessFnTester(
        Instant currentProcessingTime,
        final DoFn<InputT, OutputT> fn,
        Coder<InputT> inputCoder,
        Coder<RestrictionT> restrictionCoder,
        int maxOutputsPerBundle,
        Duration maxBundleDuration)
        throws Exception {
      // The exact windowing strategy doesn't matter in this test, but it should be able to
      // encode IntervalWindow's because that's what all tests here use.
      WindowingStrategy<InputT, BoundedWindow> windowingStrategy =
          (WindowingStrategy) WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(1)));
      final SplittableParDo.ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn =
          new SplittableParDo.ProcessFn<>(
              fn, inputCoder, restrictionCoder, windowingStrategy);
      this.tester = DoFnTester.of(processFn);
      this.timerInternals = new InMemoryTimerInternals();
      this.stateInternals = new TestInMemoryStateInternals<>("dummy");
      processFn.setStateInternalsFactory(
          new StateInternalsFactory<String>() {
            @Override
            public StateInternals stateInternalsForKey(String key) {
              return stateInternals;
            }
          });
      processFn.setTimerInternalsFactory(
          new TimerInternalsFactory<String>() {
            @Override
            public TimerInternals timerInternalsForKey(String key) {
              return timerInternals;
            }
          });
      processFn.setProcessElementInvoker(
          new OutputAndTimeBoundedSplittableProcessElementInvoker<
              InputT, OutputT, RestrictionT, TrackerT>(
              fn,
              tester.getPipelineOptions(),
              new OutputWindowedValueToDoFnTester<>(tester),
              new SideInputReader() {
                @Nullable
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
              ElementAndRestriction.of(element, restriction),
              currentProcessingTime,
              GlobalWindow.INSTANCE,
              PaneInfo.ON_TIME_AND_ONLY_FIRING));
    }

    void startElement(WindowedValue<ElementAndRestriction<InputT, RestrictionT>> windowedValue)
        throws Exception {
      tester.processElement(
          KeyedWorkItems.elementsWorkItem("key", Collections.singletonList(windowedValue)));
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
          KeyedWorkItems.<String, ElementAndRestriction<InputT, RestrictionT>>timersWorkItem(
              "key", timers));
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
    public void process(ProcessContext c, SomeRestrictionTracker tracker) {
      c.output(c.element().toString() + "a");
      c.output(c.element().toString() + "b");
      c.output(c.element().toString() + "c");
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer elem) {
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

    ProcessFnTester<Integer, String, SomeRestriction, SomeRestrictionTracker> tester =
        new ProcessFnTester<>(
            base,
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(SomeRestriction.class),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION);
    tester.startElement(
        WindowedValue.of(
            ElementAndRestriction.of(42, new SomeRestriction()),
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
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        c.updateWatermark(c.element().plus(Duration.standardSeconds(i)));
        c.output(String.valueOf(i));
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Instant elem) {
      throw new IllegalStateException("Expected to be supplied explicitly in this test");
    }

    @NewTracker
    public OffsetRangeTracker newTracker(OffsetRange range) {
      return new OffsetRangeTracker(range);
    }
  }

  @Test
  public void testUpdatesWatermark() throws Exception {
    DoFn<Instant, String> fn = new WatermarkUpdateFn();
    Instant base = Instant.now();

    ProcessFnTester<Instant, String, OffsetRange, OffsetRangeTracker> tester =
        new ProcessFnTester<>(
            base,
            fn,
            InstantCoder.of(),
            SerializableCoder.of(OffsetRange.class),
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

  /**
   * A splittable {@link DoFn} that generates the sequence [init, init + total).
   */
  private static class CounterFn extends DoFn<Integer, String> {
    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom();
          tracker.tryClaim(i); ++i) {
        c.output(String.valueOf(c.element() + i));
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Integer elem) {
      throw new UnsupportedOperationException("Expected to be supplied explicitly in this test");
    }
  }

  @Test
  public void testCheckpointsAfterNumOutputs() throws Exception {
    int max = 100;
    DoFn<Integer, String> fn = new CounterFn();
    Instant base = Instant.now();
    int baseIndex = 42;

    ProcessFnTester<Integer, String, OffsetRange, OffsetRangeTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(OffsetRange.class),
            max, MAX_BUNDLE_DURATION);

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
    assertThat(elements, not(hasItem((String.valueOf(baseIndex + 2 * max + max / 2)))));
  }

  @Test
  public void testCheckpointsAfterDuration() throws Exception {
    // Don't bound number of outputs.
    int max = Integer.MAX_VALUE;
    // But bound bundle duration - the bundle should terminate.
    Duration maxBundleDuration = Duration.standardSeconds(1);
    // Create an fn that attempts to 2x output more than checkpointing allows.
    DoFn<Integer, String> fn = new CounterFn();
    Instant base = Instant.now();
    int baseIndex = 42;

    ProcessFnTester<Integer, String, OffsetRange, OffsetRangeTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(OffsetRange.class),
            max, maxBundleDuration);

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
    public void process(ProcessContext c, SomeRestrictionTracker tracker) {
      assertEquals(State.INSIDE_BUNDLE, state);
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
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
    try (ProcessFnTester<Integer, String, SomeRestriction, SomeRestrictionTracker> tester =
        new ProcessFnTester<>(
            Instant.now(),
            fn,
            BigEndianIntegerCoder.of(),
            SerializableCoder.of(SomeRestriction.class),
            MAX_OUTPUTS_PER_BUNDLE,
            MAX_BUNDLE_DURATION)) {
      tester.startElement(42, new SomeRestriction());
    }
  }
}
