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

import static java.lang.Thread.sleep;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesBoundedSplittableParDo;
import org.apache.beam.sdk.testing.UsesBundleFinalizer;
import org.apache.beam.sdk.testing.UsesParDoLifecycle;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.UsesSplittableParDoWithWindowedSideInputs;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} behavior.
 */
@RunWith(JUnit4.class)
public class SplittableDoFnTest implements Serializable {

  static class PairStringWithIndexToLengthBase extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
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
    public OffsetRange getInitialRange(@Element String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(@Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }
  }

  @BoundedPerElement
  static class PairStringWithIndexToLengthBounded extends PairStringWithIndexToLengthBase {}

  @UnboundedPerElement
  static class PairStringWithIndexToLengthUnbounded extends PairStringWithIndexToLengthBase {}

  private static PairStringWithIndexToLengthBase pairStringWithIndexToLengthFn(IsBounded bounded) {
    return (bounded == IsBounded.BOUNDED)
        ? new PairStringWithIndexToLengthBounded()
        : new PairStringWithIndexToLengthUnbounded();
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class})
  public void testPairWithIndexBasicBounded() {
    testPairWithIndexBasic(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class})
  public void testPairWithIndexBasicUnbounded() {
    testPairWithIndexBasic(IsBounded.UNBOUNDED);
  }

  private void testPairWithIndexBasic(IsBounded bounded) {
    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(pairStringWithIndexToLengthFn(bounded)))
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
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class})
  public void testPairWithIndexWindowedTimestampedBounded() {
    testPairWithIndexWindowedTimestamped(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class})
  public void testPairWithIndexWindowedTimestampedUnbounded() {
    testPairWithIndexWindowedTimestamped(IsBounded.UNBOUNDED);
  }

  private void testPairWithIndexWindowedTimestamped(IsBounded bounded) {
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
            .apply(ParDo.of(pairStringWithIndexToLengthFn(bounded)))
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

  private static class SDFWithMultipleOutputsPerBlockBase extends DoFn<String, Integer> {
    private static final int MAX_INDEX = 98765;

    private final int numClaimsPerCall;

    private SDFWithMultipleOutputsPerBlockBase(int numClaimsPerCall) {
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
    public ProcessContinuation processElement(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
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
    public OffsetRange getInitialRange() {
      return new OffsetRange(0, MAX_INDEX);
    }
  }

  @BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockBounded
      extends SDFWithMultipleOutputsPerBlockBase {
    SDFWithMultipleOutputsPerBlockBounded(int numClaimsPerCall) {
      super(numClaimsPerCall);
    }
  }

  @UnboundedPerElement
  private static class SDFWithMultipleOutputsPerBlockUnbounded
      extends SDFWithMultipleOutputsPerBlockBase {
    SDFWithMultipleOutputsPerBlockUnbounded(int numClaimsPerCall) {
      super(numClaimsPerCall);
    }
  }

  private static SDFWithMultipleOutputsPerBlockBase sdfWithMultipleOutputsPerBlock(
      IsBounded bounded, int numClaimsPerCall) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithMultipleOutputsPerBlockBounded(numClaimsPerCall)
        : new SDFWithMultipleOutputsPerBlockUnbounded(numClaimsPerCall);
  }

  @Test
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class})
  public void testOutputAfterCheckpointBounded() {
    testOutputAfterCheckpoint(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class})
  public void testOutputAfterCheckpointUnbounded() {
    testOutputAfterCheckpoint(IsBounded.UNBOUNDED);
  }

  private void testOutputAfterCheckpoint(IsBounded bounded) {
    PCollection<Integer> outputs =
        p.apply(Create.of("foo"))
            .apply(ParDo.of(sdfWithMultipleOutputsPerBlock(bounded, 3)))
            .apply(Window.<Integer>configure().triggering(Never.ever()).discardingFiredPanes());
    PAssert.thatSingleton(outputs.apply(Count.globally()))
        .isEqualTo((long) SDFWithMultipleOutputsPerBlockBase.MAX_INDEX);
    p.run();
  }

  private static class SDFWithSideInputBase extends DoFn<Integer, String> {
    private final PCollectionView<String> sideInput;

    private SDFWithSideInputBase(PCollectionView<String> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      String side = c.sideInput(sideInput);
      c.output(side + ":" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, 1);
    }
  }

  @BoundedPerElement
  private static class SDFWithSideInputBounded extends SDFWithSideInputBase {
    private SDFWithSideInputBounded(PCollectionView<String> sideInput) {
      super(sideInput);
    }
  }

  @UnboundedPerElement
  private static class SDFWithSideInputUnbounded extends SDFWithSideInputBase {
    private SDFWithSideInputUnbounded(PCollectionView<String> sideInput) {
      super(sideInput);
    }
  }

  private static SDFWithSideInputBase sdfWithSideInput(
      IsBounded bounded, PCollectionView<String> sideInput) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithSideInputBounded(sideInput)
        : new SDFWithSideInputUnbounded(sideInput);
  }

  @Test
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class, UsesSideInputs.class})
  public void testSideInputBounded() {
    testSideInput(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class})
  public void testSideInputUnbounded() {
    testSideInput(IsBounded.UNBOUNDED);
  }

  private void testSideInput(IsBounded bounded) {
    PCollectionView<String> sideInput =
        p.apply("side input", Create.of("foo")).apply(View.asSingleton());

    PCollection<String> res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(ParDo.of(sdfWithSideInput(bounded, sideInput)).withSideInputs(sideInput));

    PAssert.that(res).containsInAnyOrder(Arrays.asList("foo:0", "foo:1", "foo:2"));

    p.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesBoundedSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class
  })
  public void testWindowedSideInputBounded() {
    testWindowedSideInput(IsBounded.BOUNDED);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesUnboundedSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class,
  })
  public void testWindowedSideInputUnbounded() {
    testWindowedSideInput(IsBounded.UNBOUNDED);
  }

  private void testWindowedSideInput(IsBounded bounded) {
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
        mainInput.apply(ParDo.of(sdfWithSideInput(bounded, sideInput)).withSideInputs(sideInput));

    PAssert.that(res).containsInAnyOrder("a:0", "a:1", "a:2", "a:3", "b:4", "b:5", "b:6", "b:7");

    p.run();
  }

  private static class SDFWithMultipleOutputsPerBlockAndSideInputBase
      extends DoFn<Integer, KV<String, Integer>> {
    private static final int MAX_INDEX = 98765;
    private final PCollectionView<String> sideInput;
    private final int numClaimsPerCall;

    SDFWithMultipleOutputsPerBlockAndSideInputBase(
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
    public ProcessContinuation processElement(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
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
    public OffsetRange getInitialRange() {
      return new OffsetRange(0, MAX_INDEX);
    }
  }

  @BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockAndSideInputBounded
      extends SDFWithMultipleOutputsPerBlockAndSideInputBase {
    private SDFWithMultipleOutputsPerBlockAndSideInputBounded(
        PCollectionView<String> sideInput, int numClaimsPerCall) {
      super(sideInput, numClaimsPerCall);
    }
  }

  @UnboundedPerElement
  private static class SDFWithMultipleOutputsPerBlockAndSideInputUnbounded
      extends SDFWithMultipleOutputsPerBlockAndSideInputBase {
    private SDFWithMultipleOutputsPerBlockAndSideInputUnbounded(
        PCollectionView<String> sideInput, int numClaimsPerCall) {
      super(sideInput, numClaimsPerCall);
    }
  }

  private static SDFWithMultipleOutputsPerBlockAndSideInputBase
      sdfWithMultipleOutputsPerBlockAndSideInput(
          IsBounded bounded, PCollectionView<String> sideInput, int numClaimsPerCall) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithMultipleOutputsPerBlockAndSideInputBounded(sideInput, numClaimsPerCall)
        : new SDFWithMultipleOutputsPerBlockAndSideInputUnbounded(sideInput, numClaimsPerCall);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesBoundedSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class
  })
  public void testWindowedSideInputWithCheckpointsBounded() {
    testWindowedSideInputWithCheckpoints(IsBounded.BOUNDED);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesUnboundedSplittableParDo.class,
    UsesSplittableParDoWithWindowedSideInputs.class,
  })
  public void testWindowedSideInputWithCheckpointsUnbounded() {
    testWindowedSideInputWithCheckpoints(IsBounded.UNBOUNDED);
  }

  private void testWindowedSideInputWithCheckpoints(IsBounded bounded) {
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
                    sdfWithMultipleOutputsPerBlockAndSideInput(
                        bounded, sideInput, 3 /* numClaimsPerCall */))
                .withSideInputs(sideInput));
    PCollection<KV<String, Iterable<Integer>>> grouped = res.apply(GroupByKey.create());

    PAssert.that(grouped.apply(Keys.create())).containsInAnyOrder("a:0", "a:1", "b:2", "b:3");
    PAssert.that(grouped)
        .satisfies(
            input -> {
              List<Integer> expected = new ArrayList<>();
              for (int i = 0; i < SDFWithMultipleOutputsPerBlockAndSideInputBase.MAX_INDEX; ++i) {
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

  private static class SDFWithAdditionalOutputBase extends DoFn<Integer, String> {
    private final TupleTag<String> additionalOutput;

    private SDFWithAdditionalOutputBase(TupleTag<String> additionalOutput) {
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      c.output("main:" + c.element());
      c.output(additionalOutput, "additional:" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, 1);
    }
  }

  @BoundedPerElement
  private static class SDFWithAdditionalOutputBounded extends SDFWithAdditionalOutputBase {
    private SDFWithAdditionalOutputBounded(TupleTag<String> additionalOutput) {
      super(additionalOutput);
    }
  }

  @UnboundedPerElement
  private static class SDFWithAdditionalOutputUnbounded extends SDFWithAdditionalOutputBase {
    private SDFWithAdditionalOutputUnbounded(TupleTag<String> additionalOutput) {
      super(additionalOutput);
    }
  }

  private static SDFWithAdditionalOutputBase sdfWithAdditionalOutput(
      IsBounded bounded, TupleTag<String> additionalOutput) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithAdditionalOutputBounded(additionalOutput)
        : new SDFWithAdditionalOutputUnbounded(additionalOutput);
  }

  @Test
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class})
  public void testAdditionalOutputBounded() {
    testAdditionalOutput(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class})
  public void testAdditionalOutputUnbounded() {
    testAdditionalOutput(IsBounded.UNBOUNDED);
  }

  private void testAdditionalOutput(IsBounded bounded) {
    TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
    TupleTag<String> additionalOutputTag = new TupleTag<String>("additional") {};

    PCollectionTuple res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(
                ParDo.of(sdfWithAdditionalOutput(bounded, additionalOutputTag))
                    .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

    PAssert.that(res.get(mainOutputTag))
        .containsInAnyOrder(Arrays.asList("main:0", "main:1", "main:2"));
    PAssert.that(res.get(additionalOutputTag))
        .containsInAnyOrder(Arrays.asList("additional:0", "additional:1", "additional:2"));

    p.run();
  }

  @Test(timeout = 15000L)
  @Ignore("https://issues.apache.org/jira/browse/BEAM-6354")
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class, UsesTestStream.class})
  public void testLateData() {

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
            .apply(ParDo.of(pairStringWithIndexToLengthFn(IsBounded.UNBOUNDED)))
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

  private static class SDFWithLifecycleBase extends DoFn<String, String> {
    private enum State {
      OUTSIDE_BUNDLE,
      INSIDE_BUNDLE,
      TORN_DOWN
    }

    private transient State state;

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      return new OffsetRange(0, 1);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      receiver.output(range);
    }

    @Setup
    public void setUp() {
      assertEquals(null, state);
      state = State.OUTSIDE_BUNDLE;
    }

    @StartBundle
    public void startBundle() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.INSIDE_BUNDLE;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      assertEquals(State.INSIDE_BUNDLE, state);
      assertTrue(tracker.tryClaim(0L));
      c.output(c.element());
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

  @BoundedPerElement
  private static class SDFWithLifecycleBounded extends SDFWithLifecycleBase {}

  @UnboundedPerElement
  private static class SDFWithLifecycleUnbounded extends SDFWithLifecycleBase {}

  private static SDFWithLifecycleBase sdfWithLifecycle(IsBounded bounded) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithLifecycleBounded()
        : new SDFWithLifecycleUnbounded();
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, UsesBoundedSplittableParDo.class})
  public void testLifecycleMethodsBounded() {
    testLifecycleMethods(IsBounded.BOUNDED);
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, UsesUnboundedSplittableParDo.class})
  public void testLifecycleMethodsUnbounded() {
    testLifecycleMethods(IsBounded.UNBOUNDED);
  }

  private void testLifecycleMethods(IsBounded bounded) {
    PCollection<String> res =
        p.apply(Create.of("a", "b", "c")).apply(ParDo.of(sdfWithLifecycle(bounded)));
    PAssert.that(res).containsInAnyOrder("a", "b", "c");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBoundedness() {
    // use TestPipeline.create() because we assert without p.run();
    Pipeline p = TestPipeline.create();
    PCollection<String> foo = p.apply(Create.of("foo"));
    {
      PCollection<String> res =
          foo.apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void process(RestrictionTracker<OffsetRange, Long> tracker) {
                      // Doesn't matter
                    }

                    @GetInitialRestriction
                    public OffsetRange getInitialRestriction() {
                      return new OffsetRange(0, 1);
                    }
                  }));
      assertEquals(PCollection.IsBounded.BOUNDED, res.isBounded());
    }
    {
      PCollection<String> res =
          foo.apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public ProcessContinuation process(
                        RestrictionTracker<OffsetRange, Long> tracker) {
                      return stop();
                    }

                    @GetInitialRestriction
                    public OffsetRange getInitialRestriction() {
                      return new OffsetRange(0, 1);
                    }
                  }));
      assertEquals(PCollection.IsBounded.UNBOUNDED, res.isBounded());
    }
  }

  /**
   * While the finalization callback hasn't been invoked, this DoFn will keep requesting
   * finalization, wait one second and then checkpoint upto MAX_ATTEMPTS amount of times. Once the
   * callback has been invoked, the DoFn will output the element and stop.
   */
  public static class BundleFinalizingSplittableDoFn extends DoFn<String, String> {
    private static final long MAX_ATTEMPTS = 3000;
    // We use the UUID to uniquely identify this DoFn in case this test is run with
    // other tests in the same JVM.
    private static final Map<UUID, AtomicBoolean> WAS_FINALIZED = new HashMap();
    private final UUID uuid = UUID.randomUUID();

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      // Use a modified OffsetRangeTracker with only support for checkpointing.
      return new OffsetRangeTracker(restriction) {
        @Override
        public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
          return super.trySplit(0);
        }
      };
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String element,
        OutputReceiver<String> receiver,
        RestrictionTracker<OffsetRange, Long> tracker,
        BundleFinalizer bundleFinalizer)
        throws InterruptedException {
      if (WAS_FINALIZED.computeIfAbsent(uuid, (unused) -> new AtomicBoolean()).get()) {
        tracker.tryClaim(tracker.currentRestriction().getFrom() + 1);
        receiver.output(element);
        // Claim beyond the end now that we know we have been finalized.
        tracker.tryClaim(Long.MAX_VALUE);
        return stop();
      }
      if (tracker.tryClaim(tracker.currentRestriction().getFrom() + 1)) {
        bundleFinalizer.afterBundleCommit(
            Instant.now().plus(Duration.standardSeconds(MAX_ATTEMPTS)),
            () -> WAS_FINALIZED.computeIfAbsent(uuid, (unused) -> new AtomicBoolean()).set(true));
        // We sleep here instead of setting a resume time since the resume time doesn't need to
        // be honored.
        sleep(100L);
        return resume();
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, MAX_ATTEMPTS);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesBoundedSplittableParDo.class, UsesBundleFinalizer.class})
  public void testBundleFinalizationOccursOnBoundedSplittableDoFn() throws Exception {
    @BoundedPerElement
    class BoundedBundleFinalizingSplittableDoFn extends BundleFinalizingSplittableDoFn {}
    PCollection<String> foo = p.apply(Create.of("foo"));
    PCollection<String> res = foo.apply(ParDo.of(new BoundedBundleFinalizingSplittableDoFn()));
    PAssert.that(res).containsInAnyOrder("foo");
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedSplittableParDo.class, UsesBundleFinalizer.class})
  public void testBundleFinalizationOccursOnUnboundedSplittableDoFn() throws Exception {
    @UnboundedPerElement
    class UnboundedBundleFinalizingSplittableDoFn extends BundleFinalizingSplittableDoFn {}
    PCollection<String> foo = p.apply(Create.of("foo"));
    PCollection<String> res = foo.apply(ParDo.of(new UnboundedBundleFinalizingSplittableDoFn()));
    PAssert.that(res).containsInAnyOrder("foo");
    p.run();
  }

  // TODO (https://issues.apache.org/jira/browse/BEAM-988): Test that Splittable DoFn
  // emits output immediately (i.e. has a pass-through trigger) regardless of input's
  // windowing/triggering strategy.
}
