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
package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionProgressCheckerTest {

  private PartitionRestrictionProgressChecker progressChecker;

  @Before
  public void setUp() {
    progressChecker = new PartitionRestrictionProgressChecker();
  }

  // ------------------------
  // QUERY_CHANGE_STREAM mode
  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionNull() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(0D, 55D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionQueryChangeStream() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(30L));

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(20D, 35D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionEndOfQueryChangeStream() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(60L));

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(50D, 5D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionWaitForChildPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForChildPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(51D, 4D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionFinishPartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.finishPartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(52D, 3D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionWaitForParentPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForParentPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(53D, 2D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionDeletePartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.deletePartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionQueryChangeStreamAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // ------------------------------
  // WAIT_FOR_CHILD_PARTITIONS mode
  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionNull() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(51D, 4D), progress);
  }

  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionWaitForChildPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForChildPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(51D, 4D), progress);
  }

  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionFinishPartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.finishPartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(52D, 3D), progress);
  }

  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionWaitForParentPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForParentPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(53D, 2D), progress);
  }

  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionDeletePartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.deletePartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionWaitForChildPartitionsAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // ------------------------
  // FINISH_PARTITION mode
  @Test
  public void testRestrictionFinishPartitionAndLastClaimedPositionFinishPartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.finishPartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.finishPartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(52D, 3D), progress);
  }

  @Test
  public void testRestrictionFinishPartitionAndLastClaimedPositionWaitForParentPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.finishPartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForParentPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(53D, 2D), progress);
  }

  @Test
  public void testRestrictionFinishPartitionAndLastClaimedPositionDeletePartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.finishPartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.deletePartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionFinishPartitionAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.finishPartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // -------------------------------
  // WAIT_FOR_PARENT_PARTITIONS mode
  @Test
  public void
      testRestrictionWaitForParentPartitionsAndLastClaimedPositionWaitForParentPartitions() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForParentPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.waitForParentPartitions();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(53D, 2D), progress);
  }

  @Test
  public void testRestrictionWaitForParentPartitionsAndLastClaimedPositionDeletePartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForParentPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.deletePartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionWaitForParentPartitionsAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.waitForParentPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // ------------------------
  // DELETE_PARTITION mode
  @Test
  public void testRestrictionDeletePartitionAndLastClaimedPositionDeletePartition() {
    final PartitionRestriction restriction =
        PartitionRestriction.deletePartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.deletePartition();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionDeletePartitionAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.deletePartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // ------------------------
  // DONE mode
  @Test
  public void testRestrictionDoneAndLastClaimedPositionDone() {
    final PartitionRestriction restriction =
        PartitionRestriction.done(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionPosition position = PartitionPosition.done();

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(55D, 0D), progress);
  }

  // ------------------------
  // STOP mode
  @Test
  public void testRestrictionStopQueryChangeStreamAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(0D, 55D), progress);
  }

  @Test
  public void testRestrictionStopQueryChangeStreamAndLastClaimedPositionQueryChangeStream() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.queryChangeStream(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(30L));

    final Progress progress = progressChecker.getProgress(restriction, position);

    assertEquals(Progress.from(20D, 35D), progress);
  }

  @Test
  public void testRestrictionStopWaitForChildPartitionsAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.waitForChildPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(51D, 4D), progress);
  }

  @Test
  public void testRestrictionStopFinishPartitionAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.finishPartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(52D, 3D), progress);
  }

  @Test
  public void testRestrictionStopWaitForParentPartitionsAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.waitForParentPartitions(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(53D, 2D), progress);
  }

  @Test
  public void testRestrictionStopDeleteAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.deletePartition(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(54D, 1D), progress);
  }

  @Test
  public void testRestrictionStopDoneAndLastClaimedPositionNull() {
    final PartitionRestriction stoppedRestriction =
        PartitionRestriction.done(
            Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(60L));
    final PartitionRestriction restriction = PartitionRestriction.stop(stoppedRestriction);

    final Progress progress = progressChecker.getProgress(restriction, null);

    assertEquals(Progress.from(55D, 0D), progress);
  }
}
