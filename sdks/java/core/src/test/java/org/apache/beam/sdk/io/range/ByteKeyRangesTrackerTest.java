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
package org.apache.beam.sdk.io.range;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteKeyRangesTracker}. */
@RunWith(JUnit4.class)
public class ByteKeyRangesTrackerTest {

  private static final ByteKey BEFORE_FIRST_START_KEY = ByteKey.of(0x2);
  private static final ByteKey FIRST_START_KEY = ByteKey.of(0x3);
  private static final ByteKey AFTER_FIRST_START_KEY = ByteKey.of(0x13);
  private static final ByteKey FIRST_END_KEY = ByteKey.of(0x9);
  private static final ByteKeyRange FIRST_RANGE =
      ByteKeyRange.of(FIRST_START_KEY, FIRST_END_KEY);
  private static final double FIRST_RANGE_SIZE = 0x9 - 0x3;

  private static final ByteKey SECOND_START_KEY = ByteKey.of(0x12);
  private static final ByteKey AFTER_SECOND_START_KEY = ByteKey.of(0x13);
  private static final ByteKey SECOND_NEW_START_KEY = ByteKey.of(0x14);
  private static final ByteKey SECOND_NEW_MIDDLE_KEY = ByteKey.of(0x24);
  private static final ByteKey BEFORE_SECOND_END_KEY = ByteKey.of(0x33);
  private static final ByteKey SECOND_END_KEY = ByteKey.of(0x34);
  private static final ByteKey AFTER_SECOND_END_KEY = ByteKey.of(0x35);
  private static final double SECOND_RANGE_SIZE = 0x34 - 0x12;
  private static final ByteKeyRange SECOND_RANGE = ByteKeyRange.of(SECOND_START_KEY,
      SECOND_END_KEY);
  private static final double SECOND_NEW_RANGE_SIZE = 0x34 - 0x14;
  private static final ByteKeyRange SECOND_NEW_RANGE = ByteKeyRange.of(SECOND_NEW_START_KEY,
      SECOND_END_KEY);

  private static final ByteKey BOTH_RANGES_MIDDLE_KEY = ByteKey.of(0x20);
  private static final double BOTH_RANGES_SIZE = SECOND_RANGE_SIZE + FIRST_RANGE_SIZE;


  @Rule public final ExpectedException expected = ExpectedException.none();

  /** Tests for {@link ByteKeyRangesTracker#toString}. */
  @Test
  public void testToString() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    String expected = String.format("ByteKeyRangesTracker{range0=%s, range1=%s, position=null}",
        FIRST_RANGE, SECOND_RANGE);
    assertEquals(expected, tracker.toString());

    tracker.tryReturnRecordAt(true, FIRST_START_KEY);
    tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY);
    expected =
        String.format("ByteKeyRangesTracker{range0=%s, range1=%s, position=%s}", FIRST_RANGE,
            SECOND_RANGE, BOTH_RANGES_MIDDLE_KEY);
    assertEquals(expected, tracker.toString());
  }

  /** Tests for updating the start key to the first record returned. */
  @Test
  public void testUpdateStartKey() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));

    tracker.tryReturnRecordAt(true, SECOND_NEW_START_KEY);
    String expected =
        String.format("ByteKeyRangesTracker{range0=%s, position=%s}", SECOND_NEW_RANGE,
            SECOND_NEW_START_KEY);
    assertEquals(expected, tracker.toString());
  }

  /** Tests for {@link ByteKeyRangesTracker#of}. */
  @Test
  public void testBuilding() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));

    assertEquals(FIRST_START_KEY, tracker.getStartPosition());
    assertEquals(SECOND_END_KEY, tracker.getStopPosition());
  }

  /** Tests for {@link ByteKeyRangesTracker#getFractionConsumed()}. */
  @Test
  public void testGetFractionConsumed() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    double delta = 0.00001;

    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, FIRST_START_KEY);
    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY);
    assertEquals(0.5, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BEFORE_SECOND_END_KEY);
    assertEquals(1 - 1 / BOTH_RANGES_SIZE, tracker.getFractionConsumed(), delta);
  }

  @Test
  public void testGetFractionConsumedAfterDone() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    double delta = 0.00001;

    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    tracker.markDone();

    assertEquals(1.0, tracker.getFractionConsumed(), delta);
  }

  @Test
  public void testGetFractionConsumedAfterOutOfRangeClaim() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    double delta = 0.00001;

    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(false, AFTER_SECOND_END_KEY));

    assertEquals(1.0, tracker.getFractionConsumed(), delta);
  }

  /** Tests for {@link ByteKeyRangesTracker#getFractionConsumed()} with updated start key. */
  @Test
  public void testGetFractionConsumedUpdateStartKey() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    double delta = 0.00001;

    tracker.tryReturnRecordAt(true, SECOND_NEW_START_KEY);
    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, SECOND_NEW_MIDDLE_KEY);
    assertEquals(0.5, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BEFORE_SECOND_END_KEY);
    assertEquals(1 - 1 / SECOND_NEW_RANGE_SIZE, tracker.getFractionConsumed(), delta);
  }

  /** Tests for {@link ByteKeyRangesTracker#tryReturnRecordAt}. */
  @Test
  public void testTryReturnRecordAt() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));

    // Should be able to emit at the same key twice, should that happen.
    // Should be able to emit within range (in order, but system guarantees won't try out of order).
    // Should not be able to emit past end of range.

    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_SECOND_END_KEY));

    assertFalse(tracker.tryReturnRecordAt(true, SECOND_END_KEY)); // after end

    assertFalse(tracker.tryReturnRecordAt(true, BEFORE_SECOND_END_KEY)); // false because done
  }

  @Test
  public void testTryReturnFirstRecordNotSplitPoint() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(false, FIRST_START_KEY);
  }

  @Test
  public void testTryReturnBeforeStartKey() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, BEFORE_FIRST_START_KEY);
  }

  @Test
  public void testTryReturnBeforeLastReturnedRecord() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, AFTER_SECOND_START_KEY);
  }

  /** Tests for {@link ByteKeyRangesTracker#trySplitAtPosition}. */
  @Test
  public void testSplitAtPosition() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));

    // Unstarted, should not split.
    assertFalse(tracker.trySplitAtPosition(BOTH_RANGES_MIDDLE_KEY));

    // Start it, split it before the end.
    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    assertTrue(tracker.trySplitAtPosition(BEFORE_SECOND_END_KEY));
    assertEquals(BEFORE_SECOND_END_KEY, tracker.getStopPosition());

    // Should not be able to split it after the end.
    assertFalse(tracker.trySplitAtPosition(SECOND_END_KEY));

    // Should not be able to split after emitting.
    assertTrue(tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY));
    assertFalse(tracker.trySplitAtPosition(BOTH_RANGES_MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, BOTH_RANGES_MIDDLE_KEY));
  }

  /** Tests for {@link ByteKeyRangesTracker#getSplitPointsConsumed()}. */
  @Test
  public void testGetSplitPointsConsumed() {
    ByteKeyRangesTracker tracker = ByteKeyRangesTracker.of
        (ImmutableList.of(SECOND_RANGE, FIRST_RANGE));
    assertEquals(0, tracker.getSplitPointsConsumed());

    // Started, 0 split points consumed
    assertTrue(tracker.tryReturnRecordAt(true, FIRST_START_KEY));
    assertEquals(0, tracker.getSplitPointsConsumed());

    // Processing new split point, 1 split point consumed
    assertTrue(tracker.tryReturnRecordAt(true, AFTER_FIRST_START_KEY));
    assertEquals(1, tracker.getSplitPointsConsumed());

    // Processing new non-split point, 1 split point consumed
    assertTrue(tracker.tryReturnRecordAt(false, BOTH_RANGES_MIDDLE_KEY));
    assertEquals(1, tracker.getSplitPointsConsumed());

    // Processing new split point, 2 split points consumed
    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_SECOND_END_KEY));
    assertEquals(2, tracker.getSplitPointsConsumed());

    // Mark tracker as done, 3 split points consumed
    tracker.markDone();
    assertEquals(3, tracker.getSplitPointsConsumed());
  }
}
