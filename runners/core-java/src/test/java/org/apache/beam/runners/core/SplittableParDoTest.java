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

import static org.apache.beam.sdk.util.WindowedValue.of;
import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
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
import org.junit.experimental.categories.Category;

/** Tests for {@link SplittableParDo}. */
public class SplittableParDoTest {
  @DefaultCoder(SerializableCoder.class)
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
      return ProcessContinuation.stop();
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

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedness() {
    DoFn<Integer, String> boundedFn = new BoundedFakeFn();
    DoFn<Integer, String> unboundedFn = new UnboundedFakeFn();

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> boundedPC =
        pipeline
            .apply("bounded", Create.of(1, 2, 3))
            .setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
    PCollection<Integer> unboundedPC =
        pipeline
            .apply("unbounded", Create.of(1, 2, 3))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);

    assertEquals(
        PCollection.IsBounded.BOUNDED,
        boundedPC.apply("bounded to bounded", new SplittableParDo<>(boundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        unboundedPC.apply("bounded to unbounded", new SplittableParDo<>(boundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        boundedPC.apply("unbounded to bounded", new SplittableParDo<>(unboundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        unboundedPC
            .apply("unbounded to unbounded", new SplittableParDo<>(unboundedFn))
            .isBounded());
  }

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
    SplittableParDo.ProcessFn<Integer, String, SomeRestriction, SomeRestrictionTracker> processFn =
        new SplittableParDo.ProcessFn<>(fn, BigEndianIntegerCoder.of(), IntervalWindow.getCoder());

    Instant base = Instant.now();
    IntervalWindow w1 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(1)), base.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(2)), base.plus(Duration.standardMinutes(2)));
    IntervalWindow w3 =
        new IntervalWindow(
            base.minus(Duration.standardMinutes(3)), base.plus(Duration.standardMinutes(3)));
    List<IntervalWindow> windows = Arrays.asList(w1, w2, w3);
    KeyedWorkItem<String, ElementRestriction<Integer, SomeRestriction>> item =
        KeyedWorkItems.workItem(
            "key",
            Collections.<TimerInternals.TimerData>emptyList(),
            Arrays.asList(
                WindowedValue.of(
                    ElementRestriction.of(42, new SomeRestriction()),
                    base,
                    windows,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING)));

    DoFnTester<KeyedWorkItem<String, ElementRestriction<Integer, SomeRestriction>>, String> tester =
        DoFnTester.of(processFn);
    tester.processElement(item);
    for (IntervalWindow w : new IntervalWindow[] {w1, w2, w3}) {
      assertEquals(
          Arrays.asList(
              TimestampedValue.of("42a", base),
              TimestampedValue.of("42b", base),
              TimestampedValue.of("42c", base)),
          tester.peekOutputElementsInWindow(w));
    }
  }

  private static class SelfInitiatedResumeFn extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, SomeRestrictionTracker tracker) {
      c.output(c.element().toString());
      return ProcessContinuation.resume()
          .withResumeDelay(Duration.standardSeconds(5))
          .withFutureOutputWatermark(c.timestamp());
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
  public void testResumeSetsHoldAndTimer() throws Exception {
    DoFn<Integer, String> fn = new SelfInitiatedResumeFn();
    SplittableParDo.ProcessFn<Integer, String, SomeRestriction, SomeRestrictionTracker> processFn =
        new SplittableParDo.ProcessFn<>(fn, BigEndianIntegerCoder.of(), IntervalWindow.getCoder());
    Instant base = Instant.now();
    DoFnTester<KeyedWorkItem<String, ElementRestriction<Integer, SomeRestriction>>, String> tester =
        DoFnTester.of(processFn);
    tester.processElement(
        KeyedWorkItems.workItem(
            "key",
            Collections.<TimerInternals.TimerData>emptyList(),
            Arrays.asList(
                WindowedValue.of(
                    ElementRestriction.of(42, new SomeRestriction()),
                    base,
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING))));
  }
}
