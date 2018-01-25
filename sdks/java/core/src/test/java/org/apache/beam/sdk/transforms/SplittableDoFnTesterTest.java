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

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.apache.beam.sdk.transforms.SplittableDoFnTester.NO_LIMIT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import java.util.Arrays;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SplittableDoFnTesterTest {
  private static class CountFn extends DoFn<Long, Long> {
    private final int numItersBeforeResume;

    private CountFn(int numItersBeforeResume) {
      this.numItersBeforeResume = numItersBeforeResume;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIters = 0; ; ++i, ++numIters) {
        if (numIters == numItersBeforeResume) {
          return resume();
        }
        if (!tracker.tryClaim(i)) {
          return stop();
        }
        c.output(i);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Long element) {
      return new OffsetRange(0, element);
    }
  }

  private static TimestampedValue<Long> tv(Instant timestamp, Long value) {
    return TimestampedValue.of(value, timestamp);
  }

  private static Multiset<TimestampedValue<Long>> tvs(Instant timestamp, long from, long to) {
    Multiset<TimestampedValue<Long>> res = LinkedHashMultiset.create();
    for (long i = from; i < to; ++i) {
      res.add(tv(timestamp, i));
    }
    return res;
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private PipelineOptions options = PipelineOptionsFactory.create();
  private Instant now = Instant.now();
  private ValueInSingleWindow<Long> element =
      ValueInSingleWindow.of(42L, now, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

  @Test
  public void testProcessElementVoluntary() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFn(3), element);

    SplittableDoFnTester.PrefixResult res =
        tester.processOnce(new OffsetRange(10, 20), NO_LIMIT);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 10, 13)));
    assertTrue(res.getContinuation().shouldResume());
    assertEquals(Arrays.asList(new OffsetRange(10, 13)), res.getCompleted());
    assertEquals(new OffsetRange(13, 20), res.getResidual());
  }

  @Test
  public void testProcessRepeatedlyUntilLimitVoluntary() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFn(3), element);

    SplittableDoFnTester.PrefixResult res =
        tester.processRepeatedlyUntilLimit(new OffsetRange(10, 20), NO_LIMIT);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 10, 20)));
    assertFalse(res.getContinuation().shouldResume());
  }

  @Test
  public void testProcessRepeatedlyUntilLimitNumOutputs() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFn(3), element);

    SplittableDoFnTester.PrefixResult res =
        tester.processRepeatedlyUntilLimit(new OffsetRange(10, 30), 7);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 10, 17)));
    assertFalse(res.getContinuation().shouldResume());

    res = tester.processRepeatedlyUntilLimit(res.getResidual(), NO_LIMIT);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 17, 30)));
  }

  @Test
  public void testProcessElementAndVerifyPrimarySuccess() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFn(3), element);

    SplittableDoFnTester.PrefixResult res =
        tester.processOnceAndVerifyReexecution(new OffsetRange(10, 30), NO_LIMIT);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 10, 13)));
    assertTrue(res.getContinuation().shouldResume());

    res = tester.processOnceAndVerifyReexecution(res.getResidual(), NO_LIMIT);
    assertThat(res.getOutputs().getMain(), equalTo(tvs(now, 13, 16)));
    assertTrue(res.getContinuation().shouldResume());
  }

  private static class BrokenOffsetRangeTracker implements RestrictionTracker<OffsetRange> {
    private OffsetRange range;
    private Long lastClaimedOffset = null;

    public BrokenOffsetRangeTracker(OffsetRange range) {
      this.range = range;
    }

    @Override
    public OffsetRange currentRestriction() {
      return range;
    }

    @Override
    public OffsetRange checkpoint() {
      OffsetRange res = new OffsetRange(lastClaimedOffset + 1, range.getTo());
      this.range =
          new OffsetRange(
              range.getFrom(), lastClaimedOffset /* incorrect: should be lastClaimedOffset + 1 */);
      return res;
    }

    public boolean tryClaim(long i) {
      if (i >= range.getTo()) {
        return false;
      }
      lastClaimedOffset = i;
      return true;
    }

    @Override
    public void checkDone() {}
  }

  private static class BrokenTrackerCountFn<T> extends DoFn<Long, Long> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, BrokenOffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); ; ++i) {
        if (!tracker.tryClaim(i)) {
          return stop();
        }
        c.output(i);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Long element) {
      return new OffsetRange(0, element);
    }

    @NewTracker
    public BrokenOffsetRangeTracker newTracker(OffsetRange range) {
      return new BrokenOffsetRangeTracker(range);
    }
  }

  @Test
  public void testProcessElementAndVerifyPrimaryBrokenTracker() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new BrokenTrackerCountFn<>(), element);

    expectedException.expectMessage("42" /* element */);
    expectedException.expectMessage("[10, 30)" /* restriction */);
    expectedException.expectMessage("[10, 14)" /* primary */);
    expectedException.expectMessage("14" /* missing element from re-executed primary */);
    tester.processOnceAndVerifyReexecution(new OffsetRange(10, 30), 5);
  }

  @Test
  public void testVerifyCheckpointConsistencySuccess() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFn(3), element);

    for (int leftNumOutputsLimit = 0; leftNumOutputsLimit < 20; ++leftNumOutputsLimit) {
      for (int j = 0; j < leftNumOutputsLimit; ++j) {
        tester.verifyCheckpointConsistency(
            new OffsetRange(10, Long.MAX_VALUE),
            j /* leftNumOutputsLimit */,
            leftNumOutputsLimit /* rightNumOutputsLimit */,
            3 /* extraNumOutputsLimit */);
      }
    }
  }

  private static class CountFnOutputsBeforeClaim extends DoFn<Long, Long> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); ; ++i) {
        // Deliberately wrong: should tryClaim() first.
        c.output(i);
        if (!tracker.tryClaim(i)) {
          return stop();
        }
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Long element) {
      return new OffsetRange(0, element);
    }
  }

  @Test
  public void testVerifyCheckpointConsistencyFnOutputsBeforeClaim() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFnOutputsBeforeClaim(), element);

    expectedException.expectMessage(
        "Checkpointing after 7 outputs causes divergence of first 16 outputs");
    expectedException.expectMessage(
        // Expected output
        "10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25");
    expectedException.expectMessage(
        // Actual output
        "10, 11, 12, 13, 14, 15, 16, 16, 17, 18, 19, 20, 21, 22, 23, 24");
    expectedException.expectMessage(
        "Elements that did not appear in actual after 3 extra outputs: [16]");
    tester.verifyCheckpointConsistency(
        new OffsetRange(10, Long.MAX_VALUE),
        7 /* leftNumOutputsLimit */,
        16 /* rightNumOutputsLimit */,
        3 /* extraNumOutputsLimit */);
  }

  private static class CountFnOutputsAfterFailedClaim extends DoFn<Long, Long> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); ; ++i) {
        if (!tracker.tryClaim(i)) {
          // Deliberately wrong: should not output here.
          c.output(i);
          return stop();
        }
        c.output(i);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Long element) {
      return new OffsetRange(0, element);
    }
  }

  @Test
  public void testVerifyCheckpointConsistencyFnOutputsAfterFailedClaim() {
    SplittableDoFnTester<Long, Long, OffsetRange> tester =
        new SplittableDoFnTester<>(options, new CountFnOutputsAfterFailedClaim(), element);

    expectedException.expectMessage(
        "Checkpointing after 7 outputs causes divergence of first 16 outputs");
    expectedException.expectMessage(
        // Expected output
        "10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26");
    expectedException.expectMessage(
        // Actual output
        "10, 11, 12, 13, 14, 15, 16, 17, 17, 18, 19, 20, 21, 22, 23, 24, 25");
    expectedException.expectMessage(
        "Elements that did not appear in actual after 3 extra outputs: [17]");
    tester.verifyCheckpointConsistency(
        new OffsetRange(10, Long.MAX_VALUE),
        7 /* leftNumOutputsLimit */,
        16 /* rightNumOutputsLimit */,
        3 /* extraNumOutputsLimit */);
  }
}
