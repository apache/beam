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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionClaimerTest {

  private PartitionRestrictionClaimer claimer;

  @Before
  public void setUp() {
    claimer = new PartitionRestrictionClaimer();
  }

  @Test
  public void testQueryChangeStreamWithinRestrictionRange() {
    final boolean canClaim =
        claimer.tryClaim(
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeSecondsAndNanos(10L, 0), Timestamp.ofTimeSecondsAndNanos(20L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(15L, 0)));

    assertTrue(canClaim);
  }

  @Test
  public void testQueryChangeStreamWithStartTimestamp() {
    final boolean canClaim =
        claimer.tryClaim(
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeSecondsAndNanos(10L, 0), Timestamp.ofTimeSecondsAndNanos(20L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 0)));

    assertTrue(canClaim);
  }

  @Test
  public void testQueryChangeStreamWithEndTimestamp() {
    final boolean canClaim =
        claimer.tryClaim(
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeSecondsAndNanos(10L, 0), Timestamp.ofTimeSecondsAndNanos(20L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(20L, 0)));

    assertTrue(canClaim);
  }

  @Test
  public void testQueryChangeStreamAfterEndTimestamp() {
    final boolean canClaim =
        claimer.tryClaim(
            PartitionRestriction.queryChangeStream(
                Timestamp.ofTimeSecondsAndNanos(10L, 0), Timestamp.ofTimeSecondsAndNanos(20L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 0)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(20L, 1)));

    assertFalse(canClaim);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryChangeStreamWithoutTimestamp() {
    claimer.tryClaim(
        PartitionRestriction.queryChangeStream(Timestamp.MIN_VALUE, Timestamp.MAX_VALUE),
        PartitionPosition.queryChangeStream(Timestamp.MIN_VALUE),
        new PartitionPosition(Optional.empty(), QUERY_CHANGE_STREAM));
  }

  @Test
  public void testQueryChangeStreamTransitions() {
    new TryClaimTestScenario(claimer)
        .from(QUERY_CHANGE_STREAM)
        .to(QUERY_CHANGE_STREAM, WAIT_FOR_CHILD_PARTITIONS, FINISH_PARTITION)
        .withTryClaimResultAs(true)
        .run();
  }

  @Test
  public void testWaitForChildPartitionsTransitions() {
    new TryClaimTestScenario(claimer)
        .from(WAIT_FOR_CHILD_PARTITIONS)
        .to(WAIT_FOR_CHILD_PARTITIONS, FINISH_PARTITION)
        .withTryClaimResultAs(true)
        .run();
  }

  @Test
  public void testFinishPartitionTransitions() {
    new TryClaimTestScenario(claimer)
        .from(FINISH_PARTITION)
        .to(FINISH_PARTITION, WAIT_FOR_PARENT_PARTITIONS)
        .withTryClaimResultAs(true)
        .run();
  }

  @Test
  public void testWaitForParentPartitionsTransitions() {
    new TryClaimTestScenario(claimer)
        .from(WAIT_FOR_PARENT_PARTITIONS)
        .to(WAIT_FOR_PARENT_PARTITIONS, DELETE_PARTITION)
        .withTryClaimResultAs(true)
        .run();
  }

  @Test
  public void testDeletePartitionTransitions() {
    new TryClaimTestScenario(claimer)
        .from(DELETE_PARTITION)
        .to(DELETE_PARTITION, DONE)
        .withTryClaimResultAs(true)
        .run();
  }

  @Test
  public void testDoneTransitions() {
    new TryClaimTestScenario(claimer).from(DONE).to(DONE).withTryClaimResultAs(true).run();
  }

  @Test
  public void testStopTransitionsAlwaysReturnsFalse() {
    new TryClaimTestScenario(claimer)
        .from(STOP)
        .to(PartitionMode.values())
        .withTryClaimResultAs(false)
        .run();
  }

  private static class TryClaimTestScenario {

    private final PartitionRestrictionClaimer claimer;
    private PartitionMode from;
    private Set<PartitionMode> validTransitions;
    private boolean expectedTryClaimResult;

    public TryClaimTestScenario(PartitionRestrictionClaimer claimer) {
      this.claimer = claimer;
    }

    public TryClaimTestScenario from(PartitionMode from) {
      this.from = from;
      return this;
    }

    public TryClaimTestScenario to(PartitionMode... validTransitions) {
      this.validTransitions = Sets.newHashSet(validTransitions);
      return this;
    }

    public TryClaimTestScenario withTryClaimResultAs(boolean tryClaimResultAs) {
      this.expectedTryClaimResult = tryClaimResultAs;
      return this;
    }

    public void run() {
      final PartitionRestriction restriction = partitionRestrictionFrom(from);
      final PartitionPosition lastClaimedPosition = partitionPositionFrom(from);
      // Tests with and without last claimed position
      for (PartitionMode to : PartitionMode.values()) {
        final PartitionPosition position = partitionPositionFrom(to);
        if (validTransitions.contains(to)) {
          assertEquals(
              expectedTryClaimResult, claimer.tryClaim(restriction, lastClaimedPosition, position));
          assertEquals(expectedTryClaimResult, claimer.tryClaim(restriction, null, position));
        } else {
          assertThrows(
              IllegalArgumentException.class,
              () -> claimer.tryClaim(restriction, lastClaimedPosition, position));
          assertThrows(
              IllegalArgumentException.class, () -> claimer.tryClaim(restriction, null, position));
        }
      }
    }

    private PartitionRestriction partitionRestrictionFrom(PartitionMode mode) {
      final Timestamp startTimestamp = Timestamp.MIN_VALUE;
      final Timestamp endTimestamp = Timestamp.MAX_VALUE;
      switch (mode) {
        case QUERY_CHANGE_STREAM:
          return PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp);
        case WAIT_FOR_CHILD_PARTITIONS:
          return PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp);
        case FINISH_PARTITION:
          return PartitionRestriction.finishPartition(startTimestamp, endTimestamp);
        case WAIT_FOR_PARENT_PARTITIONS:
          return PartitionRestriction.waitForParentPartitions(startTimestamp, endTimestamp);
        case DELETE_PARTITION:
          return PartitionRestriction.deletePartition(startTimestamp, endTimestamp);
        case DONE:
          return PartitionRestriction.done(startTimestamp, endTimestamp);
        case STOP:
          return PartitionRestriction.stop(
              PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp));
        default:
          throw new IllegalArgumentException("Unknown mode " + mode);
      }
    }

    private PartitionPosition partitionPositionFrom(PartitionMode mode) {
      switch (mode) {
        case QUERY_CHANGE_STREAM:
          return PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(1L, 0));
        case WAIT_FOR_CHILD_PARTITIONS:
          return PartitionPosition.waitForChildPartitions();
        case FINISH_PARTITION:
          return PartitionPosition.finishPartition();
        case WAIT_FOR_PARENT_PARTITIONS:
          return PartitionPosition.waitForParentPartitions();
        case DELETE_PARTITION:
          return PartitionPosition.deletePartition();
        case DONE:
          return PartitionPosition.done();
        case STOP:
          return PartitionPosition.stop();
        default:
          throw new IllegalArgumentException("Unknown mode " + mode);
      }
    }
  }
}
