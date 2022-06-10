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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.UPDATE_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.next;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import com.google.cloud.Timestamp;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.PartitionPositionGenerator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TimestampGenerator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class PartitionRestrictionTrackerTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final double DELTA = 1e-10;

  // Test that trySplit returns null when no position was claimed.
  @Property
  public void testTrySplitReturnsNullhenNoPositionWasClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThanOrEqualTo(to));

    final PartitionRestriction restriction = PartitionRestriction.queryChangeStream(from, to);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);

    assertEquals(null, splitResult);
  }

  // Test that checkDone fails if the mode is not STOP and no position was claimed.
  @Property
  public void testCheckDoneFailsWhenNoPositionClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction = PartitionRestriction.queryChangeStream(from, to);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  // ---------------------------Restriction mode is UPDATE_STATE---------------------------

  // Test transitioning from UPDATE_STATE to UPDATE_STATE for tryClaim
  @Property
  public void testTryClaimPositionUpdateStateToUpdateState(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(position.getMode(), equalTo(UPDATE_STATE));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  // Test transitioning from UPDATE_STATE to QUERY_CHANGE_STREAM for tryClaim
  @Property
  public void testTryClaimPositionUpdateStateToQueryChangeStream(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(position.getMode(), equalTo(QUERY_CHANGE_STREAM));
    assumeThat(position.getTimestamp().get(), greaterThanOrEqualTo(from));
    assumeThat(position.getTimestamp().get(), lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  // Test transitioning from UPDATE_STATE to all other modes for tryClaim fail
  @Property
  public void testTryClaimPositionUpdateStateToOtherModes(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(
        position.getMode(),
        anyOf(equalTo(WAIT_FOR_CHILD_PARTITIONS), equalTo(DONE), equalTo(STOP)));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  // Test trySplit behavior when UPDATE_STATE is claimed.
  @Property
  public void testTrySplitReturnsQueryChangeStreamhenUpdateStateClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThanOrEqualTo(to));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.updateState();
    assertTrue(tracker.tryClaim(position));

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    final PartitionRestriction primary = splitResult.getPrimary();
    final PartitionRestriction residual = splitResult.getResidual();

    assertEquals(PartitionRestriction.stop(restriction), primary);
    assertEquals(PartitionRestriction.queryChangeStream(from, to), residual);
    assertEquals(primary, tracker.restriction);
  }

  // Test that checkDone fails with UPDATE_STATE is the last claimed position.
  @Property
  public void testCheckDoneFailsWithUpdateState(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.updateState();
    assertTrue(tracker.tryClaim(position));

    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  // ---------------------------Restriction mode is QUERY_CHANGE_STREAM---------------------------

  // Test that tryClaim succeeds with QUERY_CHANGE_STREAM if timestamp is within the range.
  @Property
  public void testTryClaimReturnsTrueWhenPositionIsWithinTheRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(curTimestamp, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);
    final PartitionPosition position =
        new PartitionPosition(Optional.of(curTimestamp), QUERY_CHANGE_STREAM);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  // Test that tryClaim throws an error if timestamp is greater than the range end timestamp.
  @Property
  public void testTryClaimReturnsFalseWhenPositionIsGreaterThanRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(curTimestamp, greaterThanOrEqualTo(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);
    final PartitionPosition position =
        new PartitionPosition(Optional.of(curTimestamp), QUERY_CHANGE_STREAM);

    assertFalse(tracker.tryClaim(position));
    assertEquals(null, tracker.getLastClaimedPosition());
  }

  // Test that tryClaim throws an error if the position is before the start of the range.
  @Property
  public void testTryClaimThrowsErrorWhenPositionBeforeRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, lessThan(from));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);
    final PartitionPosition position =
        new PartitionPosition(Optional.of(curTimestamp), QUERY_CHANGE_STREAM);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  // Test that tryClaim succeeds if the timestamp is greater than or equal to the previous position.
  @Property
  public void testTryClaimPositionGreaterThanOrEqualToPreviousPosition(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp,
      @From(TimestampGenerator.class) Timestamp nextTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(nextTimestamp, greaterThanOrEqualTo(curTimestamp));
    assumeThat(nextTimestamp, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);
    final PartitionPosition position =
        new PartitionPosition(Optional.of(curTimestamp), QUERY_CHANGE_STREAM);
    final PartitionPosition nextPosition =
        new PartitionPosition(Optional.of(nextTimestamp), QUERY_CHANGE_STREAM);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());

    assertTrue(tracker.tryClaim(nextPosition));
    assertEquals(nextPosition, tracker.getLastClaimedPosition());
  }

  // Test that tryClaim throws an error if it claims a timestamp less than the previous position.
  @Property
  public void testTryClaimPositionLessThanPreviousPosition(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp prevTimestamp,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(prevTimestamp, greaterThanOrEqualTo(from));
    assumeThat(prevTimestamp, lessThan(curTimestamp));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(curTimestamp, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);
    final PartitionPosition position =
        new PartitionPosition(Optional.of(curTimestamp), QUERY_CHANGE_STREAM);
    final PartitionPosition prevPosition =
        new PartitionPosition(Optional.of(prevTimestamp), QUERY_CHANGE_STREAM);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(prevPosition));
  }

  @Property
  public void testTryClaimPositionQueryChangeStreamToWaitForChildPartitions(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(position.getMode(), equalTo(WAIT_FOR_CHILD_PARTITIONS));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  // Test that tryClaim succeeds if it transitions to WAIT_FOR_CHILD_PARTITIONS.
  @Property
  public void testTryClaimPositionQueryChangeStreamToOtherModes(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(position.getMode(), anyOf(equalTo(UPDATE_STATE), equalTo(STOP), equalTo(STOP)));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  // Test trySplit behavior for QUERY_CHANGE_STREAM when the split point is within the range.
  @Property
  public void testTrySplitQueryChangeStreamTimestampWithinRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(curTimestamp, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.queryChangeStream(curTimestamp);
    assertTrue(tracker.tryClaim(position));

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    final PartitionRestriction primary = splitResult.getPrimary();
    final PartitionRestriction residual = splitResult.getResidual();

    assertEquals(PartitionRestriction.queryChangeStream(from, next(curTimestamp)), primary);
    assertEquals(PartitionRestriction.queryChangeStream(next(curTimestamp), to), residual);
    assertEquals(primary, tracker.restriction);
  }

  // Test trySplit behavior for QUERY_CHANGE_STREAM when the split point is at end of the range.
  @Property
  public void testTrySplitQueryChangeStreamTimestampEndOfRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, next(to))
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.queryChangeStream(to);
    assertTrue(tracker.tryClaim(position));

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    final PartitionRestriction primary = splitResult.getPrimary();
    final PartitionRestriction residual = splitResult.getResidual();

    assertEquals(PartitionRestriction.stop(restriction), primary);
    assertEquals(PartitionRestriction.waitForChildPartitions(from, next(to)), residual);
    assertEquals(primary, tracker.restriction);
  }

  // Test checkDone fails when mode is QUERY_CHANGE_STREAM and last timestamp was not attempted.
  @Property
  public void testCheckDoneFailsNotAtEndOfRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, next(to))
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.queryChangeStream(from);
    assertTrue(tracker.tryClaim(position));
    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  // Test checkDone succeeds when mode is QUERY_CHANGE_STREAM and last timestamp was claimed.
  @Property
  public void testCheckDoneSucceedsAtEndOfRange(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, next(to))
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.queryChangeStream(to);
    assertTrue(tracker.tryClaim(position));
    tracker.checkDone();
  }

  // ---------------------------Restriction mode is WAIT_FOR_CHILD_PARTITIONS---------------------

  // Test TryClaim transitioning from WAIT_FOR_CHILD_PARTITIONS to WAIT_FOR_CHILD_PARTITIONS.
  @Property
  public void testTryClaimPositionWaitForChildPartitions(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(position.getMode(), anyOf(equalTo(WAIT_FOR_CHILD_PARTITIONS), equalTo(DONE)));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertTrue(tracker.tryClaim(position));
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  // Test TryClaim transitioning from WAIT_FOR_CHILD_PARTITIONS to invalid states.
  @Property
  public void testTryClaimPositionWaitForChildPartitionsToInvalidStates(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));
    assumeThat(
        position.getMode(),
        anyOf(equalTo(UPDATE_STATE), anyOf(equalTo(QUERY_CHANGE_STREAM), equalTo(STOP))));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  // Test trySplit when last claimed position was WAIT_FOR_CHILD_PARTITIONS.
  @Property
  public void testTrySplitWaitForChildPartitions(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.waitForChildPartitions();
    assertTrue(tracker.tryClaim(position));

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    final PartitionRestriction primary = splitResult.getPrimary();
    final PartitionRestriction residual = splitResult.getResidual();

    assertEquals(PartitionRestriction.stop(restriction), primary);
    assertEquals(PartitionRestriction.waitForChildPartitions(from, to), residual);
    assertEquals(primary, tracker.restriction);
  }

  // Test checkDone() behavior when waiting for child partitions.
  @Property
  public void testCheckDoneWaitForChildPartitions(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.waitForChildPartitions();
    assertTrue(tracker.tryClaim(position));

    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  // ---------------------------Restriction mode is WAIT_FOR_CHILD_PARTITIONS---------------------

  // Test claiming anything fails when DONE has been claimed.
  @Property
  public void testTryClaimPositionDone(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.done(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(position));
  }

  // Test that after DONE has been claimed, a restriction cannot be split.
  @Property
  public void testTrySplitClaimedDone(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.done();
    assertTrue(tracker.tryClaim(position));

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    assertEquals(null, splitResult);
  }

  // Test checkDone succeeds with DONE.
  @Property
  public void testCheckDoneSucceedsWithDone(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.done();
    assertTrue(tracker.tryClaim(position));

    tracker.checkDone();
  }

  // ---------------------------Restriction mode is DONE-----------------------------------------

  // Test that tryClaim returns False when restriction has mode STOP.
  @Property
  public void testTryClaimPositionStop(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(PartitionPositionGenerator.class) PartitionPosition position) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());

    final PartitionRestriction stoppedRestriction = PartitionRestriction.stop(restriction);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(stoppedRestriction);

    assertFalse(tracker.tryClaim(position));
  }

  // Test splitting fails when restriction has mode STOP.
  @Property
  public void testTrySplitStop(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());

    final PartitionRestriction stoppedRestriction = PartitionRestriction.stop(restriction);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(stoppedRestriction);

    final SplitResult<PartitionRestriction> splitResult = tracker.trySplit(0D);
    assertEquals(null, splitResult);
  }

  // Test checkDone succeeds when restriction has mode STOP.
  @Property
  public void testCheckDoneSucceedsWithStop(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());

    final PartitionRestriction stoppedRestriction = PartitionRestriction.stop(restriction);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(stoppedRestriction);

    tracker.checkDone();
  }

  // ---------------------------Test progress tracking-----------------------------------------
  @Property
  public void testGetProgressWorkCompletedAndWorkRemaining(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to,
      @From(TimestampGenerator.class) Timestamp curTimestamp) {
    assumeThat(from, lessThan(to));
    assumeThat(curTimestamp, greaterThanOrEqualTo(from));
    assumeThat(curTimestamp, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.queryChangeStream(curTimestamp);

    assertTrue(tracker.tryClaim(position));
    final Progress progress = tracker.getProgress();

    assertEquals(position.getTimestamp().get().getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(
        to.getSeconds() - position.getTimestamp().get().getSeconds(),
        progress.getWorkRemaining(),
        DELTA);
  }

  @Property
  public void testGetProgressWaitForChildPartitions(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.waitForChildPartitions();

    assertTrue(tracker.tryClaim(position));
    final Progress progress = tracker.getProgress();

    assertEquals(to.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(1, progress.getWorkRemaining(), DELTA);
  }

  @Property
  public void testGetProgressWaitForChildPartitionsNoStateClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, greaterThanOrEqualTo(Timestamp.MIN_VALUE));
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final Progress progress = tracker.getProgress();

    assertEquals(to.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(1, progress.getWorkRemaining(), DELTA);
  }

  @Property
  public void testGetProgressUpdateState(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.updateState();

    assertTrue(tracker.tryClaim(position));
    final Progress progress = tracker.getProgress();

    assertEquals(from.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(to.getSeconds() - from.getSeconds(), progress.getWorkRemaining(), DELTA);
  }

  @Property
  public void testGetProgressUpdateStateNoPositionClaimed(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.updateState(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final Progress progress = tracker.getProgress();

    assertEquals(from.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(to.getSeconds() - from.getSeconds(), progress.getWorkRemaining(), DELTA);
  }

  @Property
  public void testGetProgressDone(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(restriction);

    final PartitionPosition position = PartitionPosition.done();

    assertTrue(tracker.tryClaim(position));

    final Progress progress = tracker.getProgress();

    assertEquals(to.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(1, progress.getWorkRemaining(), DELTA);
  }

  @Property
  public void testGetProgressStop(
      @From(TimestampGenerator.class) Timestamp from,
      @From(TimestampGenerator.class) Timestamp to) {
    assumeThat(from, lessThan(to));
    assumeThat(from.getSeconds(), greaterThanOrEqualTo(1L));

    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(from, to)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(PARTITION_TOKEN)
                    .build());

    final PartitionRestriction stoppedRestriction = PartitionRestriction.stop(restriction);
    final PartitionRestrictionTracker tracker = new PartitionRestrictionTracker(stoppedRestriction);

    final Progress progress = tracker.getProgress();

    assertEquals(to.getSeconds(), progress.getWorkCompleted(), DELTA);
    assertEquals(1, progress.getWorkRemaining(), DELTA);
  }
}
