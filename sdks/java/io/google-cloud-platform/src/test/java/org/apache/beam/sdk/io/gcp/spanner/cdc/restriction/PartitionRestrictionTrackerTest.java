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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionTrackerTest {

  private PartitionRestriction restriction;
  private PartitionRestrictionSplitter splitter;
  private PartitionRestrictionClaimer claimer;
  private PartitionRestrictionSplitChecker splitChecker;
  private PartitionRestrictionProgressChecker progressChecker;
  private PartitionRestrictionTracker tracker;

  @Before
  public void setUp() {
    restriction = PartitionRestriction.queryChangeStream(Timestamp.MIN_VALUE, Timestamp.MAX_VALUE);
    splitter = mock(PartitionRestrictionSplitter.class);
    claimer = mock(PartitionRestrictionClaimer.class);
    splitChecker = mock(PartitionRestrictionSplitChecker.class);
    progressChecker = mock(PartitionRestrictionProgressChecker.class);
    tracker =
        new PartitionRestrictionTracker(
            restriction, splitter, claimer, splitChecker, progressChecker);
  }

  @Test
  public void testIsSplitAllowedQueryChangeStreamInitialization() {
    final PartitionRestrictionTracker tracker =
        new PartitionRestrictionTracker(
            PartitionRestriction.queryChangeStream(Timestamp.MIN_VALUE, Timestamp.MAX_VALUE));

    assertFalse(tracker.isSplitAllowed());
  }

  @Test
  public void testIsSplitAllowedNonQueryChangeStreamInitialization() {
    Arrays.stream(PartitionMode.values())
        .filter(mode -> mode != QUERY_CHANGE_STREAM)
        .map(
            mode ->
                new PartitionRestrictionTracker(new PartitionRestriction(null, null, mode, null)))
        .forEach(
            tracker ->
                assertTrue(
                    "Split must be allowed for " + tracker.getRestriction().getMode(),
                    tracker.isSplitAllowed()));
  }

  @Test
  public void testTrySplitWhenSplitResultIsNotNullUpdatesRestriction() {
    final SplitResult<PartitionRestriction> splitResult = mock(SplitResult.class);
    final PartitionRestriction primary = mock(PartitionRestriction.class);

    when(splitter.trySplit(anyDouble(), anyBoolean(), any(), any())).thenReturn(splitResult);
    when(splitResult.getPrimary()).thenReturn(primary);

    final SplitResult<PartitionRestriction> trySplitResult = tracker.trySplit(0D);

    assertEquals(splitResult, trySplitResult);
    assertEquals(primary, tracker.getRestriction());
  }

  @Test
  public void testTrySplitWhenSplitResultIsNotNullDoesNotUpdateRestriction() {
    final SplitResult<PartitionRestriction> trySplitResult = tracker.trySplit(0D);

    assertNull(trySplitResult);
    assertEquals(restriction, tracker.getRestriction());
  }

  @Test
  public void testTryClaimWhenCanClaimUpdatesIsSplitAllowedAndLastClaimedPosition() {
    final PartitionPosition position = mock(PartitionPosition.class);

    when(claimer.tryClaim(restriction, null, position)).thenReturn(true);
    when(splitChecker.isSplitAllowed(null, position)).thenReturn(true);

    final boolean tryClaimResult = tracker.tryClaim(position);

    assertTrue(tryClaimResult);
    assertTrue(tracker.isSplitAllowed());
    assertEquals(position, tracker.getLastClaimedPosition());
  }

  @Test
  public void testTryClaimWhenCanNotClaimDoesNotUpdateIsSplitAllowedAndLastClaimedPosition() {
    final PartitionPosition position = mock(PartitionPosition.class);

    when(claimer.tryClaim(restriction, null, position)).thenReturn(false);

    final boolean tryClaimResult = tracker.tryClaim(position);

    assertFalse(tryClaimResult);
    assertFalse(tracker.isSplitAllowed());
    assertNull(tracker.getLastClaimedPosition());
  }

  @Test
  public void testIsBounded() {
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());
  }
}
