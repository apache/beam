/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * icensed under the Apache icense, Version 2.0 (the "icense"); you may not
 * use this file except in compliance with the icense. You may obtain a copy of
 * the icense at
 *
 * http://www.apache.org/licenses/ICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the icense is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * icense for the specific language governing permissions and limitations under
 * the icense.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GroupingShuffleRangeTracker}.
 */
@RunWith(JUnit4.class)
public class GroupingShuffleRangeTrackerTest {
  @Rule
  public final ExpectedException expected = ExpectedException.none();

  private static ByteArrayShufflePosition ofBytes(int... bytes) {
    byte[] b = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i) {
      b[i] = (byte) bytes[i];
    }
    return ByteArrayShufflePosition.of(b);
  }

  @Test
  public void testTryReturnRecordInfiniteRange() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(null, null);
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 5)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(3, 6, 8, 10)));
  }

  @Test
  public void testTryReturnRecordFiniteRange() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(1, 0, 0), ofBytes(5, 0, 0));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 5)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(3, 6, 8, 10)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(4, 255, 255, 255, 255)));
    // Should fail on lexicographically larger positions.
    assertFalse(tracker.copy().tryReturnRecordAt(true, ofBytes(5, 0, 0)));
    assertFalse(tracker.copy().tryReturnRecordAt(true, ofBytes(5, 0, 1)));
    assertFalse(tracker.copy().tryReturnRecordAt(true, ofBytes(6, 0, 0)));
  }

  @Test
  public void testTryReturnRecordWithNonSplitPoints() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(1, 0, 0), ofBytes(5, 0, 0));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3)));
    assertTrue(tracker.tryReturnRecordAt(false, ofBytes(1, 2, 3)));
    assertTrue(tracker.tryReturnRecordAt(false, ofBytes(1, 2, 3)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 5)));
    assertTrue(tracker.tryReturnRecordAt(false, ofBytes(1, 2, 5)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(3, 6, 8, 10)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(4, 255, 255, 255, 255)));
  }

  @Test
  public void testFirstRecordNonSplitPoint() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(3, 0, 0), ofBytes(5, 0, 0));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(false, ofBytes(3, 4, 5));
  }

  @Test
  public void testNonSplitPointRecordWithDifferentPosition() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(3, 0, 0), ofBytes(5, 0, 0));
    tracker.tryReturnRecordAt(true, ofBytes(3, 4, 5));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(false, ofBytes(3, 4, 6));
  }

  @Test
  public void testTryReturnRecordBeforeStart() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(3, 0, 0), ofBytes(5, 0, 0));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3));
  }

  @Test
  public void testTryReturnNonMonotonic() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(3, 0, 0), ofBytes(5, 0, 0));
    tracker.tryReturnRecordAt(true, ofBytes(3, 4, 5));
    tracker.tryReturnRecordAt(true, ofBytes(3, 4, 6));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, ofBytes(3, 2, 1));
  }

  @Test
  public void testTryReturnIdenticalPositions() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(3, 0, 0), ofBytes(5, 0, 0));
    tracker.tryReturnRecordAt(true, ofBytes(3, 4, 5));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, ofBytes(3, 4, 5));
  }

  @Test
  public void testTrySplitAtPositionInfiniteRange() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(null, null);
    // Should fail before first record is returned.
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));

    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3)));

    // Should now succeed.
    assertTrue(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));
    // Should not split at same or larger position.
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6, 7)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(4, 5, 6, 7)));

    // Should split at smaller position.
    assertTrue(tracker.trySplitAtPosition(ofBytes(3, 2, 1)));

    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(2, 3, 4)));

    // Should not split at a position we're already past.
    assertFalse(tracker.trySplitAtPosition(ofBytes(2, 3, 4)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(2, 3, 3)));

    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(3, 2, 0)));
    assertFalse(tracker.tryReturnRecordAt(true, ofBytes(3, 2, 1)));
  }

  @Test
  public void testTrySplitAtPositionFiniteRange() throws Exception {
    GroupingShuffleRangeTracker tracker = new GroupingShuffleRangeTracker(
        ofBytes(0, 0, 0), ofBytes(10, 20, 30));
    // Should fail before first record is returned.
    assertFalse(tracker.trySplitAtPosition(ofBytes(0, 0, 0)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));

    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(1, 2, 3)));

    // Should now succeed.
    assertTrue(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));
    // Should not split at same or larger position.
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(3, 4, 5, 6, 7)));
    assertFalse(tracker.trySplitAtPosition(ofBytes(4, 5, 6, 7)));

    // Should split at smaller position.
    assertTrue(tracker.trySplitAtPosition(ofBytes(3, 2, 1)));
    // But not at a position at or before last returned record.
    assertFalse(tracker.trySplitAtPosition(ofBytes(1, 2, 3)));

    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(2, 3, 4)));
    assertTrue(tracker.tryReturnRecordAt(true, ofBytes(3, 2, 0)));
    assertFalse(tracker.tryReturnRecordAt(true, ofBytes(3, 2, 1)));
  }
}
