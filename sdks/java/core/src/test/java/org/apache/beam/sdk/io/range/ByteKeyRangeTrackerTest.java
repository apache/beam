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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteKeyRangeTracker}. */
@RunWith(JUnit4.class)
public class ByteKeyRangeTrackerTest {
  private static final ByteKey BEFORE_START_KEY = ByteKey.of(0x11);
  private static final ByteKey INITIAL_START_KEY = ByteKey.of(0x12);
  private static final ByteKey AFTER_START_KEY = ByteKey.of(0x13);
  private static final ByteKey INITIAL_MIDDLE_KEY = ByteKey.of(0x23);
  private static final ByteKey NEW_START_KEY = ByteKey.of(0x14);
  private static final ByteKey NEW_MIDDLE_KEY = ByteKey.of(0x24);
  private static final ByteKey BEFORE_END_KEY = ByteKey.of(0x33);
  private static final ByteKey END_KEY = ByteKey.of(0x34);
  private static final ByteKey KEY_LARGER_THAN_END = ByteKey.of(0x35);
  private static final double INITIAL_RANGE_SIZE = 0x34 - 0x12;
  private static final ByteKeyRange INITIAL_RANGE = ByteKeyRange.of(INITIAL_START_KEY, END_KEY);
  private static final double NEW_RANGE_SIZE = 0x34 - 0x14;
  private static final ByteKeyRange NEW_RANGE = ByteKeyRange.of(NEW_START_KEY, END_KEY);

  @Rule public final ExpectedException expected = ExpectedException.none();

  /** Tests for {@link ByteKeyRangeTracker#toString}. */
  @Test
  public void testToString() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    String expected = String.format("ByteKeyRangeTracker{range=%s, position=null}", INITIAL_RANGE);
    assertEquals(expected, tracker.toString());

    tracker.tryReturnRecordAt(true, INITIAL_START_KEY);
    tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY);
    expected =
        String.format(
            "ByteKeyRangeTracker{range=%s, position=%s}", INITIAL_RANGE, INITIAL_MIDDLE_KEY);
    assertEquals(expected, tracker.toString());
  }

  /** Tests for updating the start key to the first record returned. */
  @Test
  public void testUpdateStartKey() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);

    tracker.tryReturnRecordAt(true, NEW_START_KEY);
    String expected =
        String.format("ByteKeyRangeTracker{range=%s, position=%s}", NEW_RANGE, NEW_START_KEY);
    assertEquals(expected, tracker.toString());
  }

  /** Tests for {@link ByteKeyRangeTracker#of}. */
  @Test
  public void testBuilding() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);

    assertEquals(INITIAL_START_KEY, tracker.getStartPosition());
    assertEquals(END_KEY, tracker.getStopPosition());
  }

  /** Tests for {@link ByteKeyRangeTracker#getFractionConsumed()}. */
  @Test
  public void testGetFractionConsumed() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    double delta = 0.00001;

    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, INITIAL_START_KEY);
    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY);
    assertEquals(0.5, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BEFORE_END_KEY);
    assertEquals(1 - 1 / INITIAL_RANGE_SIZE, tracker.getFractionConsumed(), delta);
  }

  @Test
  public void testGetFractionConsumedAfterDone() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    double delta = 0.00001;

    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    tracker.markDone();

    assertEquals(1.0, tracker.getFractionConsumed(), delta);
  }

  @Test
  public void testGetFractionConsumedAfterOutOfRangeClaim() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    double delta = 0.00001;

    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(false, KEY_LARGER_THAN_END));

    assertEquals(1.0, tracker.getFractionConsumed(), delta);
  }

  /** Tests for {@link ByteKeyRangeTracker#getFractionConsumed()} with updated start key. */
  @Test
  public void testGetFractionConsumedUpdateStartKey() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    double delta = 0.00001;

    tracker.tryReturnRecordAt(true, NEW_START_KEY);
    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, NEW_MIDDLE_KEY);
    assertEquals(0.5, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BEFORE_END_KEY);
    assertEquals(1 - 1 / NEW_RANGE_SIZE, tracker.getFractionConsumed(), delta);
  }

  /** Tests for {@link ByteKeyRangeTracker#tryReturnRecordAt}. */
  @Test
  public void testTryReturnRecordAt() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);

    // Should be able to emit at the same key twice, should that happen.
    // Should be able to emit within range (in order, but system guarantees won't try out of order).
    // Should not be able to emit past end of range.

    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_END_KEY));

    assertFalse(tracker.tryReturnRecordAt(true, END_KEY)); // after end

    assertFalse(tracker.tryReturnRecordAt(true, BEFORE_END_KEY)); // false because done
  }

  @Test
  public void testTryReturnFirstRecordNotSplitPoint() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(false, INITIAL_START_KEY);
  }

  @Test
  public void testTryReturnBeforeStartKey() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, BEFORE_START_KEY);
  }

  @Test
  public void testTryReturnBeforeLastReturnedRecord() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, AFTER_START_KEY);
  }

  /** Tests for {@link ByteKeyRangeTracker#trySplitAtPosition}. */
  @Test
  public void testSplitAtPosition() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);

    // Unstarted, should not split.
    assertFalse(tracker.trySplitAtPosition(INITIAL_MIDDLE_KEY));

    // Start it, split it before the end.
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    assertTrue(tracker.trySplitAtPosition(BEFORE_END_KEY));
    assertEquals(BEFORE_END_KEY, tracker.getStopPosition());

    // Should not be able to split it after the end.
    assertFalse(tracker.trySplitAtPosition(END_KEY));

    // Should not be able to split after emitting.
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY));
    assertFalse(tracker.trySplitAtPosition(INITIAL_MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_MIDDLE_KEY));
  }

  /** Tests for {@link ByteKeyRangeTracker#getSplitPointsConsumed()}. */
  @Test
  public void testGetSplitPointsConsumed() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(INITIAL_RANGE);
    assertEquals(0, tracker.getSplitPointsConsumed());

    // Started, 0 split points consumed
    assertTrue(tracker.tryReturnRecordAt(true, INITIAL_START_KEY));
    assertEquals(0, tracker.getSplitPointsConsumed());

    // Processing new split point, 1 split point consumed
    assertTrue(tracker.tryReturnRecordAt(true, AFTER_START_KEY));
    assertEquals(1, tracker.getSplitPointsConsumed());

    // Processing new non-split point, 1 split point consumed
    assertTrue(tracker.tryReturnRecordAt(false, INITIAL_MIDDLE_KEY));
    assertEquals(1, tracker.getSplitPointsConsumed());

    // Processing new split point, 2 split points consumed
    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_END_KEY));
    assertEquals(2, tracker.getSplitPointsConsumed());

    // Mark tracker as done, 3 split points consumed
    tracker.markDone();
    assertEquals(3, tracker.getSplitPointsConsumed());
  }
}
