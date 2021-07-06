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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionSplitterTest {

  private Timestamp startTimestamp;
  private Timestamp endTimestamp;
  private PartitionRestriction restriction;
  private PartitionRestrictionSplitter splitter;

  @Before
  public void setUp() {
    startTimestamp = Timestamp.ofTimeSecondsAndNanos(0L, 0);
    endTimestamp = Timestamp.ofTimeSecondsAndNanos(100L, 50);
    restriction = PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp);
    splitter = new PartitionRestrictionSplitter();
  }

  @Test
  public void testSplitNotAllowed() {
    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, false, null, restriction);

    assertNull(splitResult);
  }

  @Test
  public void testLastClaimedPositionIsNull() {
    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, null, restriction);

    assertNull(splitResult);
  }

  @Test
  public void testQueryChangeStreamWithZeroFractionOfRemainder() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(50_000_250L));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.queryChangeStream(
                startTimestamp, Timestamp.ofTimeMicroseconds(50_000_251L)),
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeMicroseconds(50_000_252L), endTimestamp)),
        splitResult);
  }

  @Test
  public void testQueryChangeStreamWithNonZeroFractionOfRemainder() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(50_000_250L));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0.5D, true, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.queryChangeStream(
                startTimestamp, Timestamp.ofTimeMicroseconds(75_000_125L)),
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeMicroseconds(75_000_126L), endTimestamp)),
        splitResult);
  }

  @Test
  public void testQueryChangeStreamGreaterThanEndTimestamp() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(100L, 50));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertNull(splitResult);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryChangeStreamWithoutTimestamp() {
    final PartitionPosition position =
        new PartitionPosition(Optional.empty(), QUERY_CHANGE_STREAM, Optional.empty());

    splitter.trySplit(0D, true, position, restriction);
  }

  @Test
  public void testWaitForChildPartitions() {
    final PartitionPosition position = PartitionPosition.waitForChildPartitions(10L);

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.stop(), PartitionRestriction.waitForChildPartitions(10L)),
        splitResult);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWaitForChildPartitionsWithoutTimestamp() {
    final PartitionPosition position =
        new PartitionPosition(Optional.empty(), WAIT_FOR_CHILD_PARTITIONS, Optional.empty());

    splitter.trySplit(0D, true, position, restriction);
  }

  @Test
  public void testFinishPartition() {
    final PartitionPosition position = PartitionPosition.finishPartition();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertEquals(
        SplitResult.of(PartitionRestriction.stop(), PartitionRestriction.waitForParentPartitions()),
        splitResult);
  }

  @Test
  public void testWaitForParentPartitions() {
    final PartitionPosition position = PartitionPosition.waitForParentPartitions();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertEquals(
        SplitResult.of(PartitionRestriction.stop(), PartitionRestriction.waitForParentPartitions()),
        splitResult);
  }

  @Test
  public void testDeletePartition() {
    final PartitionPosition position = PartitionPosition.deletePartition();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertEquals(
        SplitResult.of(PartitionRestriction.stop(), PartitionRestriction.done()), splitResult);
  }

  @Test
  public void testDone() {
    final PartitionPosition position = PartitionPosition.done();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertNull(splitResult);
  }

  @Test
  public void testStop() {
    final PartitionPosition position = PartitionPosition.stop();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, true, position, restriction);

    assertNull(splitResult);
  }
}
