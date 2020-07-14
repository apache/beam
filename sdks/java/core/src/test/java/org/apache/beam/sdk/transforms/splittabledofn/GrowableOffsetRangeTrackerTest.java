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
package org.apache.beam.sdk.transforms.splittabledofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GrowableOffsetRangeTracker}. */
@RunWith(JUnit4.class)
public class GrowableOffsetRangeTrackerTest {
  private static class SimpleEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {
    private long estimateRangeEnd = 0;

    @Override
    public long estimate() {
      return estimateRangeEnd;
    }

    public void setEstimateRangeEnd(long offset) {
      estimateRangeEnd = offset;
    }
  }

  @Rule public final ExpectedException expected = ExpectedException.none();

  @Test
  public void testIllegalInitialization() throws Exception {
    expected.expect(NullPointerException.class);
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, null);
  }

  @Test
  public void testTryClaim() throws Exception {
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, new SimpleEstimator());
    assertTrue(tracker.tryClaim(10L));
    assertTrue(tracker.tryClaim(100L));
    assertFalse(tracker.tryClaim(Long.MAX_VALUE));
    tracker.checkDone();
  }

  @Test
  public void testCheckpointBeforeStart() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    simpleEstimator.setEstimateRangeEnd(10);
    SplitResult res = tracker.trySplit(0);
    tracker.checkDone();
    assertEquals(new OffsetRange(0, 0), res.getPrimary());
    assertEquals(new OffsetRange(0, 0), tracker.currentRestriction());
    assertEquals(new OffsetRange(0, Long.MAX_VALUE), res.getResidual());
  }

  @Test
  public void testCheckpointJustStarted() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertTrue(tracker.tryClaim(5L));
    simpleEstimator.setEstimateRangeEnd(0L);
    SplitResult res = tracker.trySplit(0);
    tracker.checkDone();
    assertEquals(new OffsetRange(0, 6), res.getPrimary());
    assertEquals(new OffsetRange(0, 6), tracker.currentRestriction());
    assertEquals(new OffsetRange(6, Long.MAX_VALUE), res.getResidual());

    tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertTrue(tracker.tryClaim(5L));
    simpleEstimator.setEstimateRangeEnd(20L);
    res = tracker.trySplit(0);
    tracker.checkDone();
    assertEquals(new OffsetRange(0, 6), res.getPrimary());
    assertEquals(new OffsetRange(6, Long.MAX_VALUE), res.getResidual());
  }

  @Test
  public void testCheckpointAfterAllProcessed() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertFalse(tracker.tryClaim(Long.MAX_VALUE));
    tracker.checkDone();
    assertNull(tracker.trySplit(0));
  }

  @Test
  public void testCheckpointAtEmptyRange() throws Exception {
    GrowableOffsetRangeTracker tracker =
        new GrowableOffsetRangeTracker(Long.MAX_VALUE, new SimpleEstimator());
    tracker.checkDone();
    assertNull(tracker.trySplit(0));
  }

  @Test
  public void testSplit() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertTrue(tracker.tryClaim(0L));

    simpleEstimator.setEstimateRangeEnd(16L);
    // The split of infinite range results in one finite range and one infinite range.
    SplitResult res = tracker.trySplit(0.5);
    assertEquals(new OffsetRange(0, 8), res.getPrimary());
    assertEquals(new OffsetRange(0, 8), tracker.currentRestriction());
    assertEquals(new OffsetRange(8, Long.MAX_VALUE), res.getResidual());

    // After the first split, the tracker should track a finite range. Estimate offset should not
    // impact split.
    simpleEstimator.setEstimateRangeEnd(12L);
    res = tracker.trySplit(0.5);
    assertEquals(new OffsetRange(0, 4), res.getPrimary());
    assertEquals(new OffsetRange(0, 4), tracker.currentRestriction());
    assertEquals(new OffsetRange(4, 8), res.getResidual());
    assertFalse(tracker.tryClaim(4L));
    tracker.checkDone();
  }

  @Test
  public void testSplitWithMaxEstimateRangeEnd() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertTrue(tracker.tryClaim(1L));
    simpleEstimator.setEstimateRangeEnd(Long.MAX_VALUE);
    SplitResult res = tracker.trySplit(0.5);
    long expectedEnd = 1L + (Long.MAX_VALUE - 1L) / 2;
    assertEquals(new OffsetRange(0L, expectedEnd), res.getPrimary());
    assertEquals(new OffsetRange(expectedEnd, Long.MAX_VALUE), res.getResidual());
  }

  @Test
  public void testProgressBeforeStart() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(10L, simpleEstimator);
    simpleEstimator.setEstimateRangeEnd(20);
    Progress currentProcess = tracker.getProgress();
    assertEquals(0, currentProcess.getWorkCompleted(), 0.001);
    assertEquals(10, currentProcess.getWorkRemaining(), 0.001);

    simpleEstimator.setEstimateRangeEnd(15);
    currentProcess = tracker.getProgress();
    assertEquals(0, currentProcess.getWorkCompleted(), 0.001);
    assertEquals(5, currentProcess.getWorkRemaining(), 0.001);

    simpleEstimator.setEstimateRangeEnd(5);
    currentProcess = tracker.getProgress();
    assertEquals(0, currentProcess.getWorkCompleted(), 0.001);
    assertEquals(0, currentProcess.getWorkRemaining(), 0.001);
  }

  @Test
  public void testProgressAfterFinished() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(10L, simpleEstimator);
    assertFalse(tracker.tryClaim(Long.MAX_VALUE));
    tracker.checkDone();
    simpleEstimator.setEstimateRangeEnd(0L);
    Progress currentProgress = tracker.getProgress();
    assertEquals(Long.MAX_VALUE - 10L, currentProgress.getWorkCompleted(), 0.001);
    assertEquals(0, currentProgress.getWorkRemaining(), 0.001);
  }

  @Test
  public void testProgress() throws Exception {
    long start = 10L;
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(start, simpleEstimator);
    long cur = 20L;
    assertTrue(tracker.tryClaim(cur));

    simpleEstimator.setEstimateRangeEnd(5L);
    Progress currentProgress = tracker.getProgress();
    assertEquals(cur - start, currentProgress.getWorkCompleted(), 0.001);
    assertEquals(0, currentProgress.getWorkRemaining(), 0.001);

    simpleEstimator.setEstimateRangeEnd(35L);
    currentProgress = tracker.getProgress();
    assertEquals(cur - start, currentProgress.getWorkCompleted(), 0.001);
    assertEquals(35L - cur, currentProgress.getWorkRemaining(), 0.001);

    simpleEstimator.setEstimateRangeEnd(25L);
    currentProgress = tracker.getProgress();
    assertEquals(cur - start, currentProgress.getWorkCompleted(), 0.001);
    assertEquals(25L - cur, currentProgress.getWorkRemaining(), 0.001);

    simpleEstimator.setEstimateRangeEnd(Long.MAX_VALUE);
    currentProgress = tracker.getProgress();
    assertEquals(cur - start, currentProgress.getWorkCompleted(), 0.001);
    assertEquals(Long.MAX_VALUE - cur, currentProgress.getWorkRemaining(), 0.001);
  }

  @Test
  public void testLargeRange() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker =
        new GrowableOffsetRangeTracker(Long.MIN_VALUE, simpleEstimator);

    simpleEstimator.setEstimateRangeEnd(Long.MAX_VALUE);
    Progress progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), 0.001);
    assertEquals(
        BigDecimal.valueOf(Long.MAX_VALUE)
            .subtract(BigDecimal.valueOf(Long.MIN_VALUE), MathContext.DECIMAL128)
            .doubleValue(),
        progress.getWorkRemaining(),
        0.001);

    simpleEstimator.setEstimateRangeEnd(Long.MIN_VALUE);
    SplitResult res = tracker.trySplit(0);
    assertEquals(new OffsetRange(Long.MIN_VALUE, Long.MIN_VALUE), res.getPrimary());
    assertEquals(new OffsetRange(Long.MIN_VALUE, Long.MAX_VALUE), res.getResidual());
  }

  @Test
  public void testSmallRangeWithLargeValue() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker =
        new GrowableOffsetRangeTracker(123456789012345677L, simpleEstimator);
    assertTrue(tracker.tryClaim(123456789012345677L));
    simpleEstimator.setEstimateRangeEnd(123456789012345679L);
    SplitResult res = tracker.trySplit(0.5);
    assertEquals(new OffsetRange(123456789012345677L, 123456789012345678L), res.getPrimary());
    assertEquals(new OffsetRange(123456789012345678L, Long.MAX_VALUE), res.getResidual());

    tracker = new GrowableOffsetRangeTracker(123456789012345681L, simpleEstimator);
    assertTrue(tracker.tryClaim(123456789012345681L));
    simpleEstimator.setEstimateRangeEnd(123456789012345683L);
    res = tracker.trySplit(0.5);
    assertEquals(new OffsetRange(123456789012345681L, 123456789012345682L), res.getPrimary());
    assertEquals(new OffsetRange(123456789012345682L, Long.MAX_VALUE), res.getResidual());
  }

  @Test
  public void testIsBounded() throws Exception {
    SimpleEstimator simpleEstimator = new SimpleEstimator();
    GrowableOffsetRangeTracker tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());
    assertTrue(tracker.tryClaim(0L));
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());

    // After split, the restriction should be bounded.
    simpleEstimator.setEstimateRangeEnd(16L);
    tracker.trySplit(0.5);
    assertEquals(IsBounded.BOUNDED, tracker.isBounded());

    // The restriction should be bounded after all the work has been claimed.
    tracker = new GrowableOffsetRangeTracker(0L, simpleEstimator);
    tracker.tryClaim(Long.MAX_VALUE);
    assertEquals(IsBounded.BOUNDED, tracker.isBounded());
  }
}
