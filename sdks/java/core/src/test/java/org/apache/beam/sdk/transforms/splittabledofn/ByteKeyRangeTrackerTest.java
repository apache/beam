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

import static org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker.next;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteKeyRangeTrackerTest}. */
@RunWith(JUnit4.class)
public class ByteKeyRangeTrackerTest {
  @Rule public final ExpectedException expected = ExpectedException.none();

  @Test
  public void testTryClaim() throws Exception {
    ByteKeyRange range = ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0));
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(range);
    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(ByteKey.of(0x10)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x10, 0x00)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x10, 0x00, 0x00)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x99)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xc0)));
  }

  @Test
  public void testCheckpointUnstarted() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));

    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    // We expect to get the original range back and that the current restriction
    // is effectively made empty.
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)), checkpoint);
    assertEquals(ByteKeyRangeTracker.NO_KEYS, tracker.currentRestriction());
  }

  @Test
  public void testCheckpointUnstartedForAllKeysRange() throws Exception {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRange.ALL_KEYS);

    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    // We expect to get the original range back and that the current restriction
    // is effectively made empty.
    assertEquals(ByteKeyRange.ALL_KEYS, checkpoint);
    assertEquals(ByteKeyRangeTracker.NO_KEYS, tracker.currentRestriction());
  }

  @Test
  public void testCheckpointOnlyFailedClaim() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xd0)));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)), tracker.currentRestriction());
    assertEquals(ByteKeyRangeTracker.NO_KEYS, checkpoint);
  }

  @Test
  public void testCheckpointJustStarted() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x10)));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(
        ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0x10, 0x00)), tracker.currentRestriction());
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10, 0x00), ByteKey.of(0xc0)), checkpoint);
  }

  @Test
  public void testCheckpointRegular() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(
        ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0x90, 0x00)), tracker.currentRestriction());
    assertEquals(ByteKeyRange.of(ByteKey.of(0x90, 0x00), ByteKey.of(0xc0)), checkpoint);
  }

  @Test
  public void testCheckpointAtLast() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xc0)));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)), tracker.currentRestriction());
    assertEquals(ByteKeyRangeTracker.NO_KEYS, checkpoint);
  }

  @Test
  public void testCheckpointAtLastUsingAllKeysAndEmptyKey() throws Exception {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRange.ALL_KEYS);
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertFalse(tracker.tryClaim(ByteKey.EMPTY));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(ByteKeyRange.ALL_KEYS, tracker.currentRestriction());
    assertEquals(ByteKeyRangeTracker.NO_KEYS, checkpoint);
  }

  @Test
  public void testCheckpointAfterLast() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertTrue(tracker.tryClaim(ByteKey.of(0xa0)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xd0)));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)), tracker.currentRestriction());
    assertEquals(ByteKeyRangeTracker.NO_KEYS, checkpoint);
  }

  @Test
  public void testCheckpointAfterLastUsingEmptyKey() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertTrue(tracker.tryClaim(ByteKey.of(0xa0)));
    assertFalse(tracker.tryClaim(ByteKey.EMPTY));
    ByteKeyRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)), tracker.currentRestriction());
    assertEquals(ByteKeyRangeTracker.NO_KEYS, checkpoint);
  }

  @Test
  public void testNonMonotonicClaim() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    expected.expectMessage("Trying to claim key [70] while last attempted key was [90]");
    tracker.tryClaim(ByteKey.of(0x70));
  }

  @Test
  public void testClaimBeforeStartOfRange() throws Exception {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    expected.expectMessage(
        "Trying to claim key [05] before start of the range "
            + "ByteKeyRange{startKey=[10], endKey=[c0]}");
    tracker.tryClaim(ByteKey.of(0x05));
  }

  @Test
  public void testCheckDoneAfterTryClaimPastEndOfRange() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xd0)));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneAfterTryClaimAtEndOfRange() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertFalse(tracker.tryClaim(ByteKey.of(0xc0)));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneWhenClaimingEndOfRangeForEmptyKey() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.EMPTY));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertFalse(tracker.tryClaim(ByteKey.EMPTY));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneAfterTryClaimRightBeforeEndOfRange() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    assertTrue(tracker.tryClaim(ByteKey.of(0xbf)));
    expected.expectMessage(
        "Last attempted key was [bf] in range ByteKeyRange{startKey=[10], endKey=[c0]}, "
            + "claiming work in [[bf00], [c0]) was not attempted");
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneForEmptyRange() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRangeTracker.NO_KEYS);
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneWhenNotDone() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x50)));
    assertTrue(tracker.tryClaim(ByteKey.of(0x90)));
    expected.expectMessage(
        "Last attempted key was [90] in range ByteKeyRange{startKey=[10], endKey=[c0]}, "
            + "claiming work in [[9000], [c0]) was not attempted");
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneUnstarted() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    expected.expect(IllegalStateException.class);
    tracker.checkDone();
  }

  @Test
  public void testNextByteKey() {
    assertEquals(next(ByteKey.EMPTY), ByteKey.of(0x00));
    assertEquals(next(ByteKey.of(0x00)), ByteKey.of(0x00, 0x00));
    assertEquals(next(ByteKey.of(0x9f)), ByteKey.of(0x9f, 0x00));
    assertEquals(next(ByteKey.of(0xff)), ByteKey.of(0xff, 0x00));
    assertEquals(next(ByteKey.of(0x10, 0x10)), ByteKey.of(0x10, 0x10, 0x00));
    assertEquals(next(ByteKey.of(0x00, 0xff)), ByteKey.of(0x00, 0xff, 0x00));
    assertEquals(next(ByteKey.of(0xff, 0xff)), ByteKey.of(0xff, 0xff, 0x00));
  }

  @Test
  public void testBacklogUnstarted() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRange.ALL_KEYS);
    assertEquals(1., tracker.getSize(), 0.001);

    tracker = ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    assertEquals(1., tracker.getSize(), 0.001);
  }

  @Test
  public void testBacklogFinished() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRange.ALL_KEYS);
    tracker.tryClaim(ByteKey.EMPTY);
    assertEquals(0., tracker.getSize(), 0.001);

    tracker = ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    tracker.tryClaim(ByteKey.of(0xd0));
    assertEquals(0., tracker.getSize(), 0.001);
  }

  @Test
  public void testBacklogPartiallyCompleted() {
    ByteKeyRangeTracker tracker = ByteKeyRangeTracker.of(ByteKeyRange.ALL_KEYS);
    tracker.tryClaim(ByteKey.of(0xa0));
    assertThat(tracker.getSize(), allOf(greaterThan(0.), lessThan(1.)));

    tracker = ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    tracker.tryClaim(ByteKey.of(0xa0));
    assertThat(tracker.getSize(), allOf(greaterThan(0.), lessThan(1.)));
  }
}
