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

import java.time.Instant;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link OrderedTimeRangeTracker}. */
@RunWith(JUnit4.class)
public class OrderedTimeRangeTrackerTest {
  @Rule public final ExpectedException expected = ExpectedException.none();

  @Test
  public void testTryClaim() throws Exception {
    OrderedTimeRange range = new OrderedTimeRange(100, 200);
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(range);
    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(100L));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(199L));
    assertFalse(tracker.tryClaim(200L));
  }

  @Test
  public void testCheckpointUnstarted() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    SplitResult res = tracker.trySplit(0);
    assertEquals(new OrderedTimeRange(100, 100), res.getPrimary());
    assertEquals(new OrderedTimeRange(100, 200), res.getResidual());
    tracker.checkDone();
  }

  @Test
  public void testCheckpointJustStarted() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(100L));
    OrderedTimeRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(new OrderedTimeRange(100, 101), tracker.currentRestriction());
    assertEquals(new OrderedTimeRange(101, 200), checkpoint);
    tracker.checkDone();
  }

  @Test
  public void testCheckpointRegular() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    OrderedTimeRange checkpoint = tracker.trySplit(0).getResidual();
    assertEquals(new OrderedTimeRange(100, 111), tracker.currentRestriction());
    assertEquals(new OrderedTimeRange(111, 200), checkpoint);
    tracker.checkDone();
  }

  @Test
  public void testCheckpointClaimedLast() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    assertTrue(tracker.tryClaim(199L));
    SplitResult checkpoint = tracker.trySplit(0);
    assertEquals(new OrderedTimeRange(100, 200), tracker.currentRestriction());
    assertNull(checkpoint);
    tracker.checkDone();
  }

  @Test
  public void testCheckpointAfterFailedClaim() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    assertTrue(tracker.tryClaim(160L));
    assertFalse(tracker.tryClaim(240L));
    assertNull(tracker.trySplit(0));
    tracker.checkDone();
  }

  @Test
  public void testTrySplitAfterCheckpoint() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    tracker.tryClaim(105L);
    tracker.trySplit(0);
    assertNull(tracker.trySplit(0.1));
  }

  @Test
  public void testTrySplit() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    tracker.tryClaim(100L);
    SplitResult splitRes = tracker.trySplit(0.509);
    assertEquals(new OrderedTimeRange(100, 150), splitRes.getPrimary());
    assertEquals(new OrderedTimeRange(150, 200), splitRes.getResidual());

    splitRes = tracker.trySplit(1);
    assertNull(splitRes);
  }

  @Test
  public void testTrySplitAtEmptyRange() throws Exception {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 100));
    assertNull(tracker.trySplit(0));
    assertNull(tracker.trySplit(0.1));
    assertNull(tracker.trySplit(1));
  }

  @Test
  public void testNonMonotonicClaim() throws Exception {
    expected.expectMessage(
        String.format(
            "Trying to claim offset %s while last attempted was %s",
            Instant.ofEpochMilli(103), Instant.ofEpochMilli(110)));
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    tracker.tryClaim(103L);
  }

  @Test
  public void testClaimBeforeStartOfRange() throws Exception {
    expected.expectMessage(
        String.format(
            "Trying to claim offset %s before start of the range [%s, %s)",
            Instant.ofEpochMilli(90), Instant.ofEpochMilli(100), Instant.ofEpochMilli(200)));
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    tracker.tryClaim(90L);
  }

  @Test
  public void testCheckDoneAfterTryClaimPastEndOfRange() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertFalse(tracker.tryClaim(220L));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneAfterTryClaimAtEndOfRange() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertFalse(tracker.tryClaim(200L));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneAfterTryClaimRightBeforeEndOfRange() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertTrue(tracker.tryClaim(199L));
    tracker.checkDone();
  }

  @Test
  public void testCheckDoneWhenNotDone() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    expected.expectMessage(
        String.format(
            "Last attempted offset was %s in range [%s, %s), "
                + "claiming work in [%s, %s) was not attempted",
            Instant.ofEpochMilli(175),
            Instant.ofEpochMilli(100),
            Instant.ofEpochMilli(200),
            Instant.ofEpochMilli(176),
            Instant.ofEpochMilli(200)));
    tracker.checkDone();
  }

  @Test
  public void testBacklogUnstarted() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(0, 200));
    Progress progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), 0.001);
    assertEquals(200, progress.getWorkRemaining(), 0.001);

    tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), 0.001);
    assertEquals(100, progress.getWorkRemaining(), 0.001);
  }

  @Test
  public void testBacklogFinished() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(0, 200));
    tracker.tryClaim(300L);
    Progress progress = tracker.getProgress();
    assertEquals(200, progress.getWorkCompleted(), 0.001);
    assertEquals(0, progress.getWorkRemaining(), 0.001);

    tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    tracker.tryClaim(300L);
    progress = tracker.getProgress();
    assertEquals(100, progress.getWorkCompleted(), 0.001);
    assertEquals(0, progress.getWorkRemaining(), 0.001);
  }

  @Test
  public void testBacklogPartiallyCompleted() {
    OrderedTimeRangeTracker tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(0, 200));
    tracker.tryClaim(150L);
    Progress progress = tracker.getProgress();
    assertEquals(150, progress.getWorkCompleted(), 0.001);
    assertEquals(50, progress.getWorkRemaining(), 0.001);

    tracker = new OrderedTimeRangeTracker(new OrderedTimeRange(100, 200));
    tracker.tryClaim(150L);
    progress = tracker.getProgress();
    assertEquals(50, progress.getWorkCompleted(), 0.001);
    assertEquals(50, progress.getWorkRemaining(), 0.001);
  }
}
