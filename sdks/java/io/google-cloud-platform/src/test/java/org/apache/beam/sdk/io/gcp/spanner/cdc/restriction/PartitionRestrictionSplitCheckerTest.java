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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionSplitCheckerTest {

  private PartitionRestriction restriction;
  private PartitionRestrictionSplitChecker splitChecker;

  @Before
  public void setUp() {
    restriction = mock(PartitionRestriction.class, RETURNS_DEEP_STUBS);
    splitChecker = new PartitionRestrictionSplitChecker();
  }

  @Test
  public void testQueryChangeStreamAtLeastAMicrosecondHasPassed() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(10L)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(11L)));

    assertTrue("After one microsecond has passed, split should be allowed", isSplitAllowed);
  }

  @Test
  public void testQueryChangeStreamLessThanAMicrosecondHasPassed() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(0L, 1000)),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(0L, 1999)));

    assertFalse(
        "Before at least one microsecond has passed, split should not be allowed", isSplitAllowed);
  }

  @Test
  public void testQueryChangeStreamNullLastAttemptedPosition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            null,
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(11L, 29)));

    assertFalse("With no previous claim, split should not be allowed", isSplitAllowed);
  }

  @Test
  public void testQueryChangeStreamNoPreviousTimestampOnLastAttemptedPosition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            new PartitionPosition(Optional.empty(), QUERY_CHANGE_STREAM),
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(11L, 29)));

    assertFalse(
        "With no previous claim with a timestamp, split should not be allowed", isSplitAllowed);
  }

  @Test
  public void testWaitForChildPartitions() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.queryChangeStream(Timestamp.MAX_VALUE),
            PartitionPosition.waitForChildPartitions());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testFinishPartition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.waitForChildPartitions(),
            PartitionPosition.finishPartition());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testWaitForParentPartitions() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.finishPartition(),
            PartitionPosition.waitForParentPartitions());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testDeletePartition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.waitForParentPartitions(),
            PartitionPosition.deletePartition());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testDone() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction, PartitionPosition.deletePartition(), PartitionPosition.done());

    assertFalse(isSplitAllowed);
  }
}
