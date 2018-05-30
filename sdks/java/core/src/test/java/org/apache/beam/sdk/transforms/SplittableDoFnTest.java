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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.testing.TestPipeline.testingPipelineOptions;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Ordering;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesSplittableParDo;
import org.apache.beam.sdk.testing.UsesSplittableParDoWithWindowedSideInputs;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for <a href="https://s.apache.org/splittable-do-fn>splittable</a> {@link DoFn} behavior.
 */
@RunWith(JUnit4.class)
public class SplittableDoFnTest implements Serializable {

  static class PairStringWithIndexToLength extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        c.output(KV.of(c.element(), (int) i));
        if (numIterations % 3 == 0) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(
        String element, OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }
  }

  private static PipelineOptions streamingTestPipelineOptions() {
    // Using testing options with streaming=true makes it possible to enable UsesSplittableParDo
    // tests in Dataflow runner, because as of writing, it can run Splittable DoFn only in
    // streaming mode.
    // This is a no-op for other runners currently (Direct runner doesn't care, and other
    // runners don't implement SDF at all yet).
    //
    // This is a workaround until https://issues.apache.org/jira/browse/BEAM-1620
    // is properly implemented and supports marking tests as streaming-only.
    //
    // https://issues.apache.org/jira/browse/BEAM-2483 specifically tracks the removal of the
    // current workaround.
    PipelineOptions options = testingPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    return options;
  }

  @Rule
  public final transient TestPipeline p = TestPipeline.fromOptions(streamingTestPipelineOptions());

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testPairWithIndexBasic() {

    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", 0),
                KV.of("bb", 0),
                KV.of("bb", 1),
                KV.of("ccccc", 0),
                KV.of("ccccc", 1),
                KV.of("ccccc", 2),
                KV.of("ccccc", 3),
                KV.of("ccccc", 4)));

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testPairWithIndexWindowedTimestamped() {
    // Tests that Splittable DoFn correctly propagates windowing strategy, windows and timestamps
    // of elements in the input collection.

    MutableDateTime mutableNow = Instant.now().toMutableDateTime();
    mutableNow.setMillisOfSecond(0);
    Instant now = mutableNow.toInstant();
    Instant nowP1 = now.plus(Duration.standardSeconds(1));
    Instant nowP2 = now.plus(Duration.standardSeconds(2));

    SlidingWindows windowFn =
        SlidingWindows.of(Duration.standardSeconds(5)).every(Duration.standardSeconds(1));
    PCollection<KV<String, Integer>> res =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of("a", now),
                    TimestampedValue.of("bb", nowP1),
                    TimestampedValue.of("ccccc", nowP2)))
            .apply(Window.into(windowFn))
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    assertEquals(windowFn, res.getWindowingStrategy().getWindowFn());

    PCollection<TimestampedValue<KV<String, Integer>>> timestamped = res.apply(Reify.timestamps());

    for (int i = 0; i < 4; ++i) {
      Instant base = now.minus(Duration.standardSeconds(i));
      IntervalWindow window = new IntervalWindow(base, base.plus(Duration.standardSeconds(5)));

      List<TimestampedValue<KV<String, Integer>>> expectedUnfiltered =
          Arrays.asList(
              TimestampedValue.of(KV.of("a", 0), now),
              TimestampedValue.of(KV.of("bb", 0), nowP1),
              TimestampedValue.of(KV.of("bb", 1), nowP1),
              TimestampedValue.of(KV.of("ccccc", 0), nowP2),
              TimestampedValue.of(KV.of("ccccc", 1), nowP2),
              TimestampedValue.of(KV.of("ccccc", 2), nowP2),
              TimestampedValue.of(KV.of("ccccc", 3), nowP2),
              TimestampedValue.of(KV.of("ccccc", 4), nowP2));

      List<TimestampedValue<KV<String, Integer>>> expected = new ArrayList<>();
      for (TimestampedValue<KV<String, Integer>> tv : expectedUnfiltered) {
        if (!window.start().isAfter(tv.getTimestamp())
            && !tv.getTimestamp().isAfter(window.maxTimestamp())) {
          expected.add(tv);
        }
      }
      assertFalse(expected.isEmpty());

      PAssert.that(timestamped).inWindow(window).containsInAnyOrder(expected);
    }
    p.run();
  }

  @BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlock extends DoFn<String, Integer> {
    private static final int MAX_INDEX = 98765;

    private final int numClaimsPerCall;

    private SDFWithMultipleOutputsPerBlock(int numClaimsPerCall) {
      this.numClaimsPerCall = numClaimsPerCall;
    }

    private static int snapToNextBlock(int index, int[] blockStarts) {
      for (int i = 1; i < blockStarts.length; ++i) {
        if (index > blockStarts[i - 1] && index <= blockStarts[i]) {
          return i;
        }
      }
      throw new IllegalStateException("Shouldn't get here");
    }

    @ProcessElement
    public ProcessContinuation processElement(ProcessContext c, OffsetRangeTracker tracker) {
      int[] blockStarts = {-1, 0, 12, 123, 1234, 12345, 34567, MAX_INDEX};
      int trueStart = snapToNextBlock((int) tracker.currentRestriction().getFrom(), blockStarts);
      for (int i = trueStart, numIterations = 1;
          tracker.tryClaim((long) blockStarts[i]);
          ++i, ++numIterations) {
        for (int index = blockStarts[i]; index < blockStarts[i + 1]; ++index) {
          c.output(index);
        }
        if (numIterations == numClaimsPerCall) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(String element) {
      return new OffsetRange(0, MAX_INDEX);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testOutputAfterCheckpoint() throws Exception {
    PCollection<Integer> outputs = p.apply(Create.of("foo"))
        .apply(ParDo.of(new SDFWithMultipleOutputsPerBlock(3)));
    PAssert.thatSingleton(outputs.apply(Count.globally()))
        .isEqualTo((long) SDFWithMultipleOutputsPerBlock.MAX_INDEX);
    p.run();
  }

  private static class SDFWithSideInput extends DoFn<Integer, String> {
    private final PCollectionView<String> sideInput;

    private SDFWithSideInput(PCollectionView<String> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      String side = c.sideInput(sideInput);
      c.output(side + ":" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Integer value) {
      return new OffsetRange(0, 1);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testSideInput() throws Exception {
    PCollectionView<String> sideInput =
        p.apply("side input", Create.of("foo")).apply(View.asSingleton());

    PCollection<String> res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(ParDo.of(new SDFWithSideInput(sideInput)).withSideInputs(sideInput));

    PAssert.that(res).containsInAnyOrder(Arrays.asList("foo:0", "foo:1", "foo:2"));

    p.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class
  })
  public void testWindowedSideInput() throws Exception {
    PCollection<Integer> mainInput =
        p.apply(
                "main",
                Create.timestamped(
                    TimestampedValue.of(0, new Instant(0)),
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(2)),
                    TimestampedValue.of(3, new Instant(3)),
                    TimestampedValue.of(4, new Instant(4)),
                    TimestampedValue.of(5, new Instant(5)),
                    TimestampedValue.of(6, new Instant(6)),
                    TimestampedValue.of(7, new Instant(7))))
            .apply("window 2", Window.into(FixedWindows.of(Duration.millis(2))));

    PCollectionView<String> sideInput =
        p.apply(
                "side",
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(0)),
                    TimestampedValue.of("b", new Instant(4))))
            .apply("window 4", Window.into(FixedWindows.of(Duration.millis(4))))
            .apply("singleton", View.asSingleton());

    PCollection<String> res =
        mainInput.apply(ParDo.of(new SDFWithSideInput(sideInput)).withSideInputs(sideInput));

    PAssert.that(res).containsInAnyOrder("a:0", "a:1", "a:2", "a:3", "b:4", "b:5", "b:6", "b:7");

    p.run();
  }

  @BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockAndSideInput
      extends DoFn<Integer, KV<String, Integer>> {
    private static final int MAX_INDEX = 98765;
    private final PCollectionView<String> sideInput;
    private final int numClaimsPerCall;

    public SDFWithMultipleOutputsPerBlockAndSideInput(
        PCollectionView<String> sideInput, int numClaimsPerCall) {
      this.sideInput = sideInput;
      this.numClaimsPerCall = numClaimsPerCall;
    }

    private static int snapToNextBlock(int index, int[] blockStarts) {
      for (int i = 1; i < blockStarts.length; ++i) {
        if (index > blockStarts[i - 1] && index <= blockStarts[i]) {
          return i;
        }
      }
      throw new IllegalStateException("Shouldn't get here");
    }

    @ProcessElement
    public ProcessContinuation processElement(ProcessContext c, OffsetRangeTracker tracker) {
      int[] blockStarts = {-1, 0, 12, 123, 1234, 12345, 34567, MAX_INDEX};
      int trueStart = snapToNextBlock((int) tracker.currentRestriction().getFrom(), blockStarts);
      for (int i = trueStart, numIterations = 1;
          tracker.tryClaim((long) blockStarts[i]);
          ++i, ++numIterations) {
        for (int index = blockStarts[i]; index < blockStarts[i + 1]; ++index) {
          c.output(KV.of(c.sideInput(sideInput) + ":" + c.element(), index));
        }
        if (numIterations == numClaimsPerCall) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(Integer element) {
      return new OffsetRange(0, MAX_INDEX);
    }
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class
  })
  public void testWindowedSideInputWithCheckpoints() throws Exception {
    PCollection<Integer> mainInput =
        p.apply(
                "main",
                Create.timestamped(
                    TimestampedValue.of(0, new Instant(0)),
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(2)),
                    TimestampedValue.of(3, new Instant(3))))
            .apply("window 1", Window.into(FixedWindows.of(Duration.millis(1))));

    PCollectionView<String> sideInput =
        p.apply(
                "side",
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(0)),
                    TimestampedValue.of("b", new Instant(2))))
            .apply("window 2", Window.into(FixedWindows.of(Duration.millis(2))))
            .apply("singleton", View.asSingleton());

    PCollection<KV<String, Integer>> res =
        mainInput.apply(
            ParDo.of(
                    new SDFWithMultipleOutputsPerBlockAndSideInput(
                        sideInput, 3 /* numClaimsPerCall */))
                .withSideInputs(sideInput));
    PCollection<KV<String, Iterable<Integer>>> grouped = res.apply(GroupByKey.create());

    PAssert.that(grouped.apply(Keys.create())).containsInAnyOrder("a:0", "a:1", "b:2", "b:3");
    PAssert.that(grouped)
        .satisfies(
            input -> {
              List<Integer> expected = new ArrayList<>();
              for (int i = 0; i < SDFWithMultipleOutputsPerBlockAndSideInput.MAX_INDEX; ++i) {
                expected.add(i);
              }
              for (KV<String, Iterable<Integer>> kv : input) {
                assertEquals(expected, Ordering.<Integer>natural().sortedCopy(kv.getValue()));
              }
              return null;
            });
    p.run();

    // TODO: also test coverage when some of the windows of the side input are not ready.
  }

  private static class SDFWithAdditionalOutput extends DoFn<Integer, String> {
    private final TupleTag<String> additionalOutput;

    private SDFWithAdditionalOutput(TupleTag<String> additionalOutput) {
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      c.output("main:" + c.element());
      c.output(additionalOutput, "additional:" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Integer value) {
      return new OffsetRange(0, 1);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testAdditionalOutput() throws Exception {
    TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
    TupleTag<String> additionalOutputTag = new TupleTag<String>("additional") {};

    PCollectionTuple res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(
                ParDo.of(new SDFWithAdditionalOutput(additionalOutputTag))
                    .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

    PAssert.that(res.get(mainOutputTag))
        .containsInAnyOrder(Arrays.asList("main:0", "main:1", "main:2"));
    PAssert.that(res.get(additionalOutputTag))
        .containsInAnyOrder(Arrays.asList("additional:0", "additional:1", "additional:2"));

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class, UsesTestStream.class})
  public void testLateData() throws Exception {

    Instant base = Instant.now();

    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements("aa")
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(5)))
            .addElements(TimestampedValue.of("bb", base.minus(Duration.standardHours(1))))
            .advanceProcessingTime(Duration.standardHours(1))
            .advanceWatermarkToInfinity();

    PCollection<String> input =
        p.apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardMinutes(1))
                    .discardingFiredPanes());

    PCollection<KV<String, Integer>> afterSDF =
        input
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PCollection<String> nonLate = afterSDF.apply(GroupByKey.create()).apply(Keys.create());

    // The splittable DoFn itself should not drop any data and act as pass-through.
    PAssert.that(afterSDF)
        .containsInAnyOrder(
            Arrays.asList(KV.of("aa", 0), KV.of("aa", 1), KV.of("bb", 0), KV.of("bb", 1)));

    // But it should preserve the windowing strategy of the data, including allowed lateness:
    // the follow-up GBK should drop the late data.
    assertEquals(afterSDF.getWindowingStrategy(), input.getWindowingStrategy());
    PAssert.that(nonLate).containsInAnyOrder("aa");

    p.run();
  }

  private static class SDFWithLifecycle extends DoFn<String, String> {
    private enum State {
      BEFORE_SETUP,
      OUTSIDE_BUNDLE,
      INSIDE_BUNDLE,
      TORN_DOWN
    }

    private State state = State.BEFORE_SETUP;

    @ProcessElement
    public void processElement(ProcessContext c, OffsetRangeTracker tracker) {
      assertEquals(State.INSIDE_BUNDLE, state);
      assertTrue(tracker.tryClaim(0L));
      c.output(c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String value) {
      return new OffsetRange(0, 1);
    }

    @Setup
    public void setUp() {
      assertEquals(State.BEFORE_SETUP, state);
      state = State.OUTSIDE_BUNDLE;
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

    @Teardown
    public void tearDown() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.TORN_DOWN;
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSplittableParDo.class})
  public void testLifecycleMethods() throws Exception {

    PCollection<String> res =
        p.apply(Create.of("a", "b", "c")).apply(ParDo.of(new SDFWithLifecycle()));

    PAssert.that(res).containsInAnyOrder("a", "b", "c");

    p.run();
  }

  // TODO (https://issues.apache.org/jira/browse/BEAM-988): Test that Splittable DoFn
  // emits output immediately (i.e. has a pass-through trigger) regardless of input's
  // windowing/triggering strategy.
}
