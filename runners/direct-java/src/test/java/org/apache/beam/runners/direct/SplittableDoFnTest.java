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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for <a href="https://s.apache.org/splittable-do-fn>splittable</a> {@link DoFn} behavior
 * using the direct runner.
 *
 * <p>TODO: make this use @RunnableOnService.
 */
@RunWith(JUnit4.class)
public class SplittableDoFnTest {
  static class OffsetRange implements Serializable {
    public final int from;
    public final int to;

    OffsetRange(int from, int to) {
      this.from = from;
      this.to = to;
    }
  }

  private static class OffsetRangeTracker implements RestrictionTracker<OffsetRange> {
    private OffsetRange range;
    private Integer lastClaimedIndex = null;

    OffsetRangeTracker(OffsetRange range) {
      this.range = checkNotNull(range);
    }

    @Override
    public OffsetRange currentRestriction() {
      return range;
    }

    @Override
    public OffsetRange checkpoint() {
      if (lastClaimedIndex == null) {
        OffsetRange res = range;
        range = new OffsetRange(range.from, range.from);
        return res;
      }
      OffsetRange res = new OffsetRange(lastClaimedIndex + 1, range.to);
      this.range = new OffsetRange(range.from, lastClaimedIndex + 1);
      return res;
    }

    boolean tryClaim(int i) {
      checkState(lastClaimedIndex == null || i > lastClaimedIndex);
      if (i >= range.to) {
        return false;
      }
      lastClaimedIndex = i;
      return true;
    }
  }

  static class PairStringWithIndexToLength extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (int i = tracker.currentRestriction().from; tracker.tryClaim(i); ++i) {
        c.output(KV.of(c.element(), i));
        if (i % 3 == 0) {
          return ProcessContinuation.resume();
        }
      }
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(
        String element, OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.from, (range.from + range.to) / 2));
      receiver.output(new OffsetRange((range.from + range.to) / 2, range.to));
    }

    @NewTracker
    public OffsetRangeTracker newTracker(OffsetRange range) {
      return new OffsetRangeTracker(range);
    }
  }

  private static class ReifyTimestampsFn<T> extends DoFn<T, TimestampedValue<T>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(TimestampedValue.of(c.element(), c.timestamp()));
    }
  }

  @Ignore(
      "BEAM-801: SplittableParDo uses unsupported OldDoFn features that are not available in DoFn; "
          + "It must be implemented as a primitive.")
  @Test
  public void testPairWithIndexBasic() throws ClassNotFoundException {
    Pipeline p = TestPipeline.create();
    p.getOptions().setRunner(DirectRunner.class);
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

  @Ignore(
      "BEAM-801: SplittableParDo uses unsupported OldDoFn features that are not available in DoFn; "
          + "It must be implemented as a primitive.")
  @Test
  public void testPairWithIndexWindowedTimestamped() throws ClassNotFoundException {
    // Tests that Splittable DoFn correctly propagates windowing strategy, windows and timestamps
    // of elements in the input collection.
    Pipeline p = TestPipeline.create();
    p.getOptions().setRunner(DirectRunner.class);

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
            .apply(Window.<String>into(windowFn))
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    assertEquals(windowFn, res.getWindowingStrategy().getWindowFn());

    PCollection<TimestampedValue<KV<String, Integer>>> timestamped =
        res.apply("Reify timestamps", ParDo.of(new ReifyTimestampsFn<KV<String, Integer>>()));

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
}
