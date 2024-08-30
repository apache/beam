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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range;
import java.math.BigDecimal;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.joda.time.Instant;
import org.junit.Test;

public class ReadChangeStreamPartitionProgressTrackerTest {
  @Test
  public void testTryClaim() {
    final StreamProgress streamProgress = new StreamProgress();
    final ReadChangeStreamPartitionProgressTracker tracker =
        new ReadChangeStreamPartitionProgressTracker(streamProgress);
    assertEquals(streamProgress, tracker.currentRestriction());

    ChangeStreamContinuationToken changeStreamContinuationToken =
        ChangeStreamContinuationToken.create(Range.ByteStringRange.create("a", "b"), "1234");
    final StreamProgress streamProgress2 =
        new StreamProgress(
            changeStreamContinuationToken, Instant.now(), BigDecimal.ONE, Instant.now(), false);
    assertTrue(tracker.tryClaim(streamProgress2));
    assertEquals(streamProgress2, tracker.currentRestriction());
    assertEquals(
        streamProgress2.getEstimatedLowWatermark(),
        tracker.currentRestriction().getEstimatedLowWatermark());

    assertNull(tracker.trySplit(0.5));
    assertEquals(streamProgress2, tracker.currentRestriction());
    assertEquals(
        streamProgress2.getEstimatedLowWatermark(),
        tracker.currentRestriction().getEstimatedLowWatermark());
    try {
      tracker.checkDone();
      fail("Should not reach here because checkDone should have thrown an exception");
    } catch (IllegalStateException e) {
      assertTrue("There's more work to be done. CheckDone threw an exception", true);
    }

    final SplitResult<StreamProgress> splitResult = SplitResult.of(null, streamProgress2);
    assertEquals(splitResult, tracker.trySplit(0));

    assertFalse(tracker.tryClaim(streamProgress2));
    // No exception thrown, it is done.
    tracker.checkDone();
  }

  @Test
  public void testTrySplitMultipleTimes() {
    final StreamProgress streamProgress = new StreamProgress();
    final ReadChangeStreamPartitionProgressTracker tracker =
        new ReadChangeStreamPartitionProgressTracker(streamProgress);
    assertEquals(streamProgress, tracker.currentRestriction());

    final SplitResult<StreamProgress> splitResult = SplitResult.of(null, streamProgress);
    assertEquals(splitResult, tracker.trySplit(0));

    // Call trySplit again
    assertNull(tracker.trySplit(0));
    assertNull(tracker.trySplit(0));
    tracker.checkDone();
  }

  @Test
  public void testDoneOnFailToLockTrue() {
    StreamProgress streamProgress = new StreamProgress();
    streamProgress.setFailToLock(true);
    ReadChangeStreamPartitionProgressTracker tracker =
        new ReadChangeStreamPartitionProgressTracker(streamProgress);
    tracker.checkDone();
  }

  @Test
  public void testNotDoneOnFailToLockFalse() {
    StreamProgress streamProgress = new StreamProgress();
    streamProgress.setFailToLock(false);
    ReadChangeStreamPartitionProgressTracker tracker =
        new ReadChangeStreamPartitionProgressTracker(streamProgress);
    assertThrows(IllegalStateException.class, tracker::checkDone);
  }
}
