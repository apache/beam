/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.io.range;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteKeyRangeTracker}. */
@RunWith(JUnit4.class)
public class ByteKeyRangeTrackerTest {
  private static final ByteKey START_KEY = ByteKey.of(0x12);
  private static final ByteKey MIDDLE_KEY = ByteKey.of(0x23);
  private static final ByteKey BEFORE_END_KEY = ByteKey.of(0x33);
  private static final ByteKey END_KEY = ByteKey.of(0x34);
  private static final double RANGE_SIZE = 0x34 - 0x12;
  private static final ByteKeyRange RANGE = ByteKeyRange.of(START_KEY, END_KEY);

  /** Tests for {@link ByteKeyRangeTracker#toString}. */
  @Test
  public void testToString() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(RANGE);
    String expected = String.format("ByteKeyRangeTracker{range=%s, position=null}", RANGE);
    assertEquals(expected, tracker.toString());

    tracker.tryReturnRecordAt(true, MIDDLE_KEY);
    expected = String.format("ByteKeyRangeTracker{range=%s, position=%s}", RANGE, MIDDLE_KEY);
    assertEquals(expected, tracker.toString());
  }

  /** Tests for {@link ByteKeyRangeTracker#of}. */
  @Test
  public void testBuilding() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(RANGE);

    assertEquals(START_KEY, tracker.getStartPosition());
    assertEquals(END_KEY, tracker.getStopPosition());
  }

  /** Tests for {@link ByteKeyRangeTracker#getFractionConsumed()}. */
  @Test
  public void testGetFractionConsumed() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(RANGE);
    double delta = 0.00001;

    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, START_KEY);
    assertEquals(0.0, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, MIDDLE_KEY);
    assertEquals(0.5, tracker.getFractionConsumed(), delta);

    tracker.tryReturnRecordAt(true, BEFORE_END_KEY);
    assertEquals(1 - 1 / RANGE_SIZE, tracker.getFractionConsumed(), delta);
  }

  /** Tests for {@link ByteKeyRangeTracker#tryReturnRecordAt}. */
  @Test
  public void testTryReturnRecordAt() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(RANGE);

    // Should be able to emit at the same key twice, should that happen.
    // Should be able to emit within range (in order, but system guarantees won't try out of order).
    // Should not be able to emit past end of range.

    assertTrue(tracker.tryReturnRecordAt(true, START_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, START_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, MIDDLE_KEY));

    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_END_KEY));

    assertFalse(tracker.tryReturnRecordAt(true, END_KEY)); // after end

    assertTrue(tracker.tryReturnRecordAt(true, BEFORE_END_KEY)); // still succeeds
  }

  /** Tests for {@link ByteKeyRangeTracker#trySplitAtPosition}. */
  @Test
  public void testSplitAtPosition() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(RANGE);

    // Unstarted, should not split.
    assertFalse(tracker.trySplitAtPosition(MIDDLE_KEY));

    // Start it, split it before the end.
    assertTrue(tracker.tryReturnRecordAt(true, START_KEY));
    assertTrue(tracker.trySplitAtPosition(BEFORE_END_KEY));
    assertEquals(BEFORE_END_KEY, tracker.getStopPosition());

    // Should not be able to split it after the end.
    assertFalse(tracker.trySplitAtPosition(END_KEY));

    // Should not be able to split after emitting.
    assertTrue(tracker.tryReturnRecordAt(true, MIDDLE_KEY));
    assertFalse(tracker.trySplitAtPosition(MIDDLE_KEY));
    assertTrue(tracker.tryReturnRecordAt(true, MIDDLE_KEY));
  }
}
