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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;

public class TimestampRangeTrackerTest {
  private TimestampRange range;
  private TimestampRangeTracker tracker;

  @Before
  public void setUp() throws Exception {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new TimestampRangeTracker(range);
  }

  @Test
  public void testTryClaim() {
    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(10L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(11L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(19L)));
    assertFalse(tracker.tryClaim(Timestamp.ofTimeMicroseconds(20L)));
  }

  @Test
  public void testTrySplitReturnsPrimaryAndResidual() {
    tracker.tryClaim(Timestamp.ofTimeMicroseconds(11L));

    final SplitResult<TimestampRange> result = tracker.trySplit(0D);
    assertEquals(
        TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(11L)),
        result.getPrimary());
    assertEquals(
        TimestampRange.of(Timestamp.ofTimeMicroseconds(11L), Timestamp.ofTimeMicroseconds(20L)),
        result.getResidual());
  }

  @Test
  public void testCheckDoneSucceedsWhenIntervalIsEmpty() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(10L));
    tracker = new TimestampRangeTracker(range);

    // check this does not throw an exception
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneThrowsExceptionWhenNoClaimsWereMadeInANonEmptyInterval() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new TimestampRangeTracker(range);

    tracker.checkDone();
  }

  @Test
  public void testCheckDoneSucceedsWhenOneBeforeEndOfRangeWasClaimed() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.ofTimeSecondsAndNanos(0L, 19999));

    // check this does not throw an exception
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneSucceedsWhenTheEndOfRangeWasClaimed() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.ofTimeMicroseconds(20L));

    // check this does not throw an exception
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneThrowsExceptionWhenEndOfRangeWasNotClaimed() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.ofTimeMicroseconds(15L));

    tracker.checkDone();
  }

  @Test
  public void testCheckDoneWithMaxTimestampAsEndOfRange() {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.MAX_VALUE);
    tracker = new TimestampRangeTracker(range);

    tracker.tryClaim(Timestamp.MAX_VALUE);

    tracker.checkDone();
  }

  @Test
  public void testTrySplitUpdatesRestriction() {
    tracker.tryClaim(Timestamp.ofTimeMicroseconds(11L));
    tracker.trySplit(0D);

    assertEquals(
        TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(11L)),
        tracker.currentRestriction());
  }

  @Test
  public void testToByteKey() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(20L, 10);
    final ByteKey expectedKey = ByteKey.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 10});

    assertEquals(expectedKey, tracker.toByteKey(timestamp));
  }

  @Test
  public void testToByteKeyMaxTimestamp() {
    final Timestamp timestamp = Timestamp.MAX_VALUE;
    final ByteKey expectedKey =
        ByteKey.copyFrom(new byte[] {0, 0, 0, 58, -1, -12, 65, 127, 59, -102, -55, -1});

    assertEquals(expectedKey, tracker.toByteKey(timestamp));
  }

  @Test
  public void testToByteKeyMinTimestamp() {
    final Timestamp timestamp = Timestamp.MIN_VALUE;
    final ByteKey expectedKey =
        ByteKey.copyFrom(new byte[] {-1, -1, -1, -15, -120, 110, 9, 0, 0, 0, 0, 0});

    assertEquals(expectedKey, tracker.toByteKey(timestamp));
  }

  @Test
  public void testToTimestamp() {
    final ByteKey key = ByteKey.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 10});
    final Timestamp expectedTimestamp = Timestamp.ofTimeSecondsAndNanos(20L, 10);

    assertEquals(expectedTimestamp, tracker.toTimestamp(key));
  }

  @Test
  public void testFromBytesMaxTimestamp() {
    final ByteKey key =
        ByteKey.copyFrom(new byte[] {0, 0, 0, 58, -1, -12, 65, 127, 59, -102, -55, -1});
    final Timestamp expectedTimestamp = Timestamp.MAX_VALUE;

    assertEquals(expectedTimestamp, tracker.toTimestamp(key));
  }

  @Test
  public void testFromBytesMinTimestamp() {
    final ByteKey key = ByteKey.copyFrom(new byte[] {-1, -1, -1, -15, -120, 110, 9, 0, 0, 0, 0, 0});
    final Timestamp expectedTimestamp = Timestamp.MIN_VALUE;

    assertEquals(expectedTimestamp, tracker.toTimestamp(key));
  }
}
