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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
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
    when(restriction.getStartTimestamp()).thenReturn(Timestamp.ofTimeMicroseconds(10L));
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction, PartitionPosition.queryChangeStream(Timestamp.ofTimeMicroseconds(11L)));

    assertTrue("After one microsecond has passed, split should be allowed", isSplitAllowed);
  }

  @Test
  public void testQueryChangeStreamLessThanAMicrosecondHasPassed() {
    when(restriction.getStartTimestamp()).thenReturn(Timestamp.ofTimeSecondsAndNanos(0L, 1000));
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(
            restriction,
            PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(0L, 1999)));

    assertFalse(
        "Before at least one microsecond has passed, split should not be allowed", isSplitAllowed);
  }

  @Test
  public void testWaitForChildPartitions() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(restriction, PartitionPosition.waitForChildPartitions());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testFinishPartition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(restriction, PartitionPosition.finishPartition());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testWaitForParentPartitions() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(restriction, PartitionPosition.waitForParentPartitions());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testDeletePartition() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(restriction, PartitionPosition.deletePartition());

    assertTrue(isSplitAllowed);
  }

  @Test
  public void testDone() {
    final boolean isSplitAllowed =
        splitChecker.isSplitAllowed(restriction, PartitionPosition.done());

    assertFalse(isSplitAllowed);
  }
}
