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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittableParDo}. */
@RunWith(JUnit4.class)
public class SplittableParDoTest {
  // ----------------- Tests for whether the transform sets boundedness correctly --------------
  private static class SomeRestriction implements Serializable {}

  private static class SomeRestrictionTracker implements RestrictionTracker<SomeRestriction> {
    private final SomeRestriction someRestriction = new SomeRestriction();

    @Override
    public SomeRestriction currentRestriction() {
      return someRestriction;
    }

    @Override
    public SomeRestriction checkpoint() {
      return someRestriction;
    }
  }

  private static class BoundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext context, SomeRestrictionTracker tracker) {}

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
      return null;
    }
  }

  private static class UnboundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context, SomeRestrictionTracker tracker) {
      return stop();
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
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

  @Test
  public void testBoundednessForBoundedFn() {
    Pipeline pipeline = TestPipeline.create();
    DoFn<Integer, String> boundedFn = new BoundedFakeFn();
    assertEquals(
        "Applying a bounded SDF to a bounded collection produces a bounded collection",
        PCollection.IsBounded.BOUNDED,
        makeBoundedCollection(pipeline)
            .apply("bounded to bounded", new SplittableParDo<>(boundedFn))
            .isBounded());
    assertEquals(
        "Applying a bounded SDF to an unbounded collection produces an unbounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeUnboundedCollection(pipeline)
            .apply("bounded to unbounded", new SplittableParDo<>(boundedFn))
            .isBounded());
  }

  @Test
  public void testBoundednessForUnboundedFn() {
    Pipeline pipeline = TestPipeline.create();
    DoFn<Integer, String> unboundedFn = new UnboundedFakeFn();
    assertEquals(
        "Applying an unbounded SDF to a bounded collection produces a bounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeBoundedCollection(pipeline)
            .apply("unbounded to bounded", new SplittableParDo<>(unboundedFn))
            .isBounded());
    assertEquals(
        "Applying an unbounded SDF to an unbounded collection produces an unbounded collection",
        PCollection.IsBounded.UNBOUNDED,
        makeUnboundedCollection(pipeline)
            .apply("unbounded to unbounded", new SplittableParDo<>(unboundedFn))
            .isBounded());
  }

  // ------------------------------- Tests for ProcessFn ---------------------------------

  /**
   * A helper for testing {@link SplittableParDo.ProcessFn} on 1 element (but possibly over multiple
   * {@link DoFn.ProcessElement} calls).
   */
  private static class ProcessFnTester<
      InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>> {
    private final DoFnTester<
            KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
        tester;
    private Instant currentProcessingTime;

    ProcessFnTester(
        Instant currentProcessingTime,
        DoFn<InputT, OutputT> fn,
        Coder<InputT> inputCoder,
        Coder<RestrictionT> restrictionCoder)
        throws Exception {
      SplittableParDo.ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn =
          new SplittableParDo.ProcessFn<>(
              fn, inputCoder, restrictionCoder, IntervalWindow.getCoder());
      this.tester = DoFnTester.of(processFn);
      this.tester.startBundle();
      this.tester.advanceProcessingTime(currentProcessingTime);

      this.currentProcessingTime = currentProcessingTime;
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
      tester.processElement(KeyedWorkItems.elementsWorkItem("key", Arrays.asList(windowedValue)));
    }

    /**
     * Advances processing time by a given duration and, if any timers fired, performs a non-seed
     * {@link DoFn.ProcessElement} call, feeding it the timers.
     */
    boolean advanceProcessingTimeBy(Duration duration) throws Exception {
      currentProcessingTime = currentProcessingTime.plus(duration);
      List<TimerInternals.TimerData> timers = tester.advanceProcessingTime(currentProcessingTime);
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

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
      return new SomeRestrictionTracker();
    }
  }

  @Test
  public void testTrivialProcessFnPropagatesOutputsWindowsAndTimestamp() throws Exception {
    // Tests that ProcessFn correctly propagates windows and timestamp of the element
    // inside the KeyedWorkItem.
    // The underlying DoFn is actually monolithic, so this doesn't test splitting.
    DoFn<Integer, String> fn = new ToStringFn();

    Instant base = Instant.now();
    ProcessFnTester<Integer, String, SomeRestriction, SomeRestrictionTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(SomeRestriction.class));

    IntervalWindow w1 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(1)), base.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(2)), base.plus(Duration.standardMinutes(2)));
    IntervalWindow w3 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(3)), base.plus(Duration.standardMinutes(3)));

    tester.startElement(
        WindowedValue.of(
            ElementAndRestriction.of(42, new SomeRestriction()),
            base,
            Arrays.asList(w1, w2, w3),
            PaneInfo.ON_TIME_AND_ONLY_FIRING));

    for (IntervalWindow w : new IntervalWindow[] {w1, w2, w3}) {
      assertEquals(
          Arrays.asList(
              TimestampedValue.of("42a", base),
              TimestampedValue.of("42b", base),
              TimestampedValue.of("42c", base)),
          tester.peekOutputElementsInWindow(w));
    }
  }

  /** A simple splittable {@link DoFn} that outputs the given element every 5 seconds forever. */
  private static class SelfInitiatedResumeFn extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, SomeRestrictionTracker tracker) {
      c.output(c.element().toString());
      return resume().withResumeDelay(Duration.standardSeconds(5)).withWatermark(c.timestamp());
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer elem) {
      return new SomeRestriction();
    }

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
      return new SomeRestrictionTracker();
    }
  }

  @Test
  public void testResumeSetsTimer() throws Exception {
    DoFn<Integer, String> fn = new SelfInitiatedResumeFn();
    Instant base = Instant.now();
    ProcessFnTester<Integer, String, SomeRestriction, SomeRestrictionTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(SomeRestriction.class));

    tester.startElement(42, new SomeRestriction());
    assertThat(tester.takeOutputElements(), contains("42"));

    // Should resume after 5 seconds: advancing by 3 seconds should have no effect.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertTrue(tester.takeOutputElements().isEmpty());

    // 6 seconds should be enough - should invoke the fn again.
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertThat(tester.takeOutputElements(), contains("42"));

    // Should again resume after 5 seconds: advancing by 3 seconds should again have no effect.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertTrue(tester.takeOutputElements().isEmpty());

    // 6 seconds should again be enough.
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(3)));
    assertThat(tester.takeOutputElements(), contains("42"));
  }

  private static class SomeCheckpoint implements Serializable {
    private int firstUnprocessedIndex;

    private SomeCheckpoint(int firstUnprocessedIndex) {
      this.firstUnprocessedIndex = firstUnprocessedIndex;
    }
  }

  private static class SomeCheckpointTracker implements RestrictionTracker<SomeCheckpoint> {
    private SomeCheckpoint current;
    private boolean isActive = true;

    private SomeCheckpointTracker(SomeCheckpoint current) {
      this.current = current;
    }

    @Override
    public SomeCheckpoint currentRestriction() {
      return current;
    }

    public boolean tryUpdateCheckpoint(int firstUnprocessedIndex) {
      if (!isActive) {
        return false;
      }
      current = new SomeCheckpoint(firstUnprocessedIndex);
      return true;
    }

    @Override
    public SomeCheckpoint checkpoint() {
      isActive = false;
      return current;
    }
  }

  /**
   * A splittable {@link DoFn} that generates the sequence [init, init + total) in batches of given
   * size.
   */
  private static class CounterFn extends DoFn<Integer, String> {
    private final int numTotalOutputs;
    private final int numOutputsPerCall;

    private CounterFn(int numTotalOutputs, int numOutputsPerCall) {
      this.numTotalOutputs = numTotalOutputs;
      this.numOutputsPerCall = numOutputsPerCall;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, SomeCheckpointTracker tracker) {
      int start = tracker.currentRestriction().firstUnprocessedIndex;
      for (int i = 0; i < numOutputsPerCall; ++i) {
        int index = start + i;
        if (!tracker.tryUpdateCheckpoint(index + 1)) {
          return resume();
        }
        if (index >= numTotalOutputs) {
          return stop();
        }
        c.output(String.valueOf(c.element() + index));
      }
      return resume();
    }

    @GetInitialRestriction
    public SomeCheckpoint getInitialRestriction(Integer elem) {
      throw new UnsupportedOperationException("Expected to be supplied explicitly in this test");
    }

    @NewTracker
    public SomeCheckpointTracker newTracker(SomeCheckpoint restriction) {
      return new SomeCheckpointTracker(restriction);
    }
  }

  @Test
  public void testResumeCarriesOverState() throws Exception {
    DoFn<Integer, String> fn = new CounterFn(3, 1);
    Instant base = Instant.now();
    ProcessFnTester<Integer, String, SomeCheckpoint, SomeCheckpointTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(SomeCheckpoint.class));

    tester.startElement(42, new SomeCheckpoint(0));
    assertThat(tester.takeOutputElements(), contains("42"));
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), contains("43"));
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    assertThat(tester.takeOutputElements(), contains("44"));
    assertTrue(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
    // After outputting all 3 items, should not output anything more.
    assertEquals(0, tester.takeOutputElements().size());
    // Should also not ask to resume.
    assertFalse(tester.advanceProcessingTimeBy(Duration.standardSeconds(1)));
  }

  @Test
  public void testReactsToCheckpoint() throws Exception {
    int max = SplittableParDo.ProcessFn.MAX_OUTPUTS_PER_BUNDLE;
    // Create an fn that attempts to 2x output more than checkpointing allows.
    DoFn<Integer, String> fn = new CounterFn(2 * max + max / 2, 2 * max);
    Instant base = Instant.now();
    int baseIndex = 42;

    ProcessFnTester<Integer, String, SomeCheckpoint, SomeCheckpointTracker> tester =
        new ProcessFnTester<>(
            base, fn, BigEndianIntegerCoder.of(), SerializableCoder.of(SomeCheckpoint.class));

    List<String> elements;

    tester.startElement(baseIndex, new SomeCheckpoint(0));
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
}
