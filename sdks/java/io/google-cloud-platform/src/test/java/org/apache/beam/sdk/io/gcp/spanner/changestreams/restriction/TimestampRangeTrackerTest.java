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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.next;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import com.google.cloud.Timestamp;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TimestampGenerator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class TimestampRangeTrackerTest {
  private static final double DELTA = 1e-10;

  @Property
  public void testTryClaimReturnsTrueWhenPositionIsWithinTheRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, lessThanOrEqualTo(to));
    assumeThat(position, greaterThanOrEqualTo(from));
    assumeThat(position, lessThan(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.lastAttemptedPosition);
    assertEquals(position, tracker.lastClaimedPosition);
  }

  @Property
  public void testTryClaimReturnsFalseWhenPositionIsGreaterOrEqualToTheEndOfTheRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, lessThanOrEqualTo(to));
    assumeThat(position, greaterThanOrEqualTo(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    assertFalse(tracker.tryClaim(position));
    assertEquals(position, tracker.lastAttemptedPosition);
    assertNull(tracker.lastClaimedPosition);
  }

  @Test
  public void testTryClaimFailsWhenPositionIsLessThanPreviousClaim() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(0L);
    final Timestamp to = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp position = Timestamp.ofTimeMicroseconds(5L);
    final Timestamp nextPosition = Timestamp.ofTimeMicroseconds(3L);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    assertTrue(tracker.tryClaim(position));
    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(nextPosition));
    assertEquals(position, tracker.lastAttemptedPosition);
    assertEquals(position, tracker.lastClaimedPosition);
  }

  @Test
  public void testTryClaimFailsWhenPositionIsLessThanTheBeginningOfTheRange() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(5L);
    final Timestamp to = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp position = Timestamp.ofTimeMicroseconds(4L);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  @Property
  public void testTrySplitReturnsSplitWhenNoPositionWasClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThanOrEqualTo(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    final SplitResult<TimestampRange> splitResult = tracker.trySplit(0D);
    final TimestampRange primary = splitResult.getPrimary();
    final TimestampRange residual = splitResult.getResidual();

    assertEquals(TimestampRange.of(from, from), primary);
    assertEquals(TimestampRange.of(from, to), residual);
    assertEquals(primary, tracker.range);
  }

  @Property
  public void testTrySplitReturnsSplitWhenAPositionIsClaimedAndFractionOfRemainderIsZero(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, lessThanOrEqualTo(to));
    assumeThat(position, greaterThanOrEqualTo(from));
    assumeThat(position, lessThan(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    final SplitResult<TimestampRange> splitResult = tracker.trySplit(0D);

    final TimestampRange primary = splitResult.getPrimary();
    final TimestampRange residual = splitResult.getResidual();
    final Timestamp nextPosition = next(position);
    assertEquals(TimestampRange.of(from, nextPosition), primary);
    assertEquals(TimestampRange.of(nextPosition, to), residual);
    assertEquals(primary, tracker.range);
  }

  @Property
  public void testTrySplitReturnsNullWhenAPositionIsGreaterThanTheEndOfTheRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, lessThanOrEqualTo(to));
    assumeThat(position, greaterThan(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    final SplitResult<TimestampRange> splitResult = tracker.trySplit(0D);

    assertNull(splitResult);
  }

  @Test
  public void testCheckDoneSucceedsWhenFromIsEqualToTheEndOfTheRange() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final TimestampRange range = TimestampRange.of(from, from);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    // Method is void, succeeds if exception is not thrown
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneSucceedsWhenClaimingTheEndOfTheRangeHasBeenAttempted() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp to = Timestamp.ofTimeMicroseconds(50L);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(to);
    // Method is void, succeeds if exception is not thrown
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneSucceedsWhenClaimingPastTheEndOfTheRangeHasBeenAttempted() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp to = Timestamp.ofTimeMicroseconds(50L);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.ofTimeMicroseconds(51L));
    // Method is void, succeeds if exception is not thrown
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneFailsWhenNoClaimHasBeenMade() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp to = Timestamp.ofTimeMicroseconds(50L);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  @Property
  public void testCheckDoneFailsWhenClaimingTheEndOfTheRangeHasNotBeenAttempted(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, lessThan(to));
    assumeThat(position, greaterThanOrEqualTo(from));
    assumeThat(position, lessThan(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  @Property
  public void testGetProgressWorkCompletedAndWorkRemaining(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp position) {
    assumeThat(from, greaterThanOrEqualTo(Timestamp.ofTimeSecondsAndNanos(0, 0)));
    assumeThat(from, lessThanOrEqualTo(to));
    assumeThat(position, greaterThanOrEqualTo(from));
    assumeThat(position, lessThan(to));

    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    final Progress progress = tracker.getProgress();

    assertEquals(position.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(to.getSeconds() - position.getSeconds(), progress.getWorkRemaining(), DELTA);
  }

  @Test
  public void testGetProgressReturnsWorkRemainingAsWholeRangeWhenNoClaimWasAttempted() {
    final Timestamp from = Timestamp.ofTimeSecondsAndNanos(0, 0);
    final Timestamp to = Timestamp.now();
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    final Progress progress = tracker.getProgress();
    assertEquals(0D, progress.getWorkCompleted(), DELTA);
    assertEquals(to.getSeconds(), progress.getWorkRemaining(), DELTA);
  }

  @Test
  public void testGetProgressReturnsWorkRemainingAsRangeEndMinusAttemptedPosition() {
    final Timestamp from = Timestamp.ofTimeSecondsAndNanos(0, 0);
    final Timestamp to = Timestamp.ofTimeSecondsAndNanos(100, 0);
    final Timestamp position = Timestamp.ofTimeSecondsAndNanos(30, 0);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    final Progress progress = tracker.getProgress();

    assertTrue(progress.getWorkCompleted() >= 0);
    assertEquals(30D, progress.getWorkCompleted(), DELTA);
    assertTrue(progress.getWorkRemaining() >= 0);
    assertEquals(70D, progress.getWorkRemaining(), DELTA);
  }

  @Test
  public void testGetProgressReturnsWorkCompletedWhenRangeEndHasBeenAttempted() {
    final Timestamp from = Timestamp.ofTimeSecondsAndNanos(0, 0);
    final Timestamp to = Timestamp.ofTimeSecondsAndNanos(101, 0);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.ofTimeSecondsAndNanos(100, 0));
    tracker.tryClaim(Timestamp.ofTimeSecondsAndNanos(101, 0));
    final Progress progress = tracker.getProgress();

    assertTrue(progress.getWorkCompleted() >= 0);
    assertEquals(100D, progress.getWorkCompleted(), DELTA);
    assertTrue(progress.getWorkRemaining() >= 0);
    assertEquals(1D, progress.getWorkRemaining(), DELTA);
  }

  @Test
  public void testGetProgressReturnsWorkCompletedWhenPastRangeEndHasBeenAttempted() {
    final Timestamp from = Timestamp.ofTimeSecondsAndNanos(0, 0);
    final Timestamp to = Timestamp.ofTimeSecondsAndNanos(101, 0);
    final Timestamp position = Timestamp.ofTimeSecondsAndNanos(101, 0);
    final TimestampRange range = TimestampRange.of(from, to);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(position);
    final Progress progress = tracker.getProgress();

    assertTrue(progress.getWorkCompleted() >= 0);
    assertEquals(0D, progress.getWorkCompleted(), DELTA);
    assertTrue(progress.getWorkRemaining() >= 0);
    assertEquals(101D, progress.getWorkRemaining(), DELTA);
  }

  @Test
  public void testGetProgressForStreaming() {
    final Timestamp from = Timestamp.ofTimeSecondsAndNanos(0, 0);
    final Timestamp position = Timestamp.ofTimeSecondsAndNanos(101, 0);
    final TimestampRange range = TimestampRange.of(from, Timestamp.MAX_VALUE);
    final TimestampRangeTracker tracker = new TimestampRangeTracker(range);

    tracker.setTimeSupplier(() -> Timestamp.ofTimeSecondsAndNanos(position.getSeconds() + 10, 0));
    tracker.tryClaim(position);
    final Progress progress = tracker.getProgress();

    assertTrue(progress.getWorkCompleted() >= 0);
    assertEquals(101D, progress.getWorkCompleted(), DELTA);
    assertTrue(progress.getWorkRemaining() >= 0);
    assertEquals(10D, progress.getWorkRemaining(), DELTA);
  }
}
