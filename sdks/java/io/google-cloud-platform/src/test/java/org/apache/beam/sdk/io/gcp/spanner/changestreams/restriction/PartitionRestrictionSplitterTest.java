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
import static org.junit.Assert.assertNull;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionSplitterTest {

  private Timestamp startTimestamp;
  private Timestamp endTimestamp;
  private PartitionRestriction restriction;
  private PartitionRestriction initialRestriction;
  private PartitionRestrictionSplitter splitter;

  @Before
  public void setUp() {
    startTimestamp = Timestamp.ofTimeSecondsAndNanos(0L, 0);
    endTimestamp = Timestamp.ofTimeSecondsAndNanos(100L, 50);
    restriction =
        PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken("partitionToken")
                    .build());
    initialRestriction =
        PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp)
            .withMetadata(
                PartitionRestrictionMetadata.newBuilder()
                    .withPartitionToken(InitialPartition.PARTITION_TOKEN)
                    .build());
    splitter = new PartitionRestrictionSplitter();
  }

  @Test
  public void testLastClaimedPositionIsNull() {
    final SplitResult<PartitionRestriction> splitResult = splitter.trySplit(0D, null, restriction);

    assertNull(splitResult);
  }

  @Test
  public void testUpdateState() {
    final PartitionPosition position = PartitionPosition.updateState();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.stop(restriction),
            PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp)),
        splitResult);
  }

  @Test
  public void testQueryChangeStreamInitialPartition() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(1L));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, initialRestriction);

    assertNull(splitResult);
  }

  @Test
  public void testQueryChangeStream() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(50L, 0));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.queryChangeStream(
                startTimestamp, Timestamp.ofTimeSecondsAndNanos(50L, 1)),
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeSecondsAndNanos(50L, 1), endTimestamp)),
        splitResult);
  }

  @Test
  public void testQueryChangeStreamSplitEqualToEndTimestamp() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(100L, 49));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.stop(restriction),
            PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)),
        splitResult);
  }

  @Test
  public void testQueryChangeStreamSplitGreaterThanEndTimestamp() {
    final PartitionPosition position =
        PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(100L, 50));

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.stop(restriction),
            PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)),
        splitResult);
  }

  @Test
  public void testWaitForChildPartitions() {
    final PartitionPosition position = PartitionPosition.waitForChildPartitions();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertEquals(
        SplitResult.of(
            PartitionRestriction.stop(restriction),
            PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)),
        splitResult);
  }

  @Test
  public void testDone() {
    final PartitionPosition position = PartitionPosition.done();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertNull(splitResult);
  }

  @Test
  public void testStop() {
    final PartitionPosition position = PartitionPosition.stop();

    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(0D, position, restriction);

    assertNull(splitResult);
  }
}
