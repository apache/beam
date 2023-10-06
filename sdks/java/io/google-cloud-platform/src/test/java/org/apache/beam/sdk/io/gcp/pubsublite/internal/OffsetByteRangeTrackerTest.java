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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ticker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class OffsetByteRangeTrackerTest {
  private static final double IGNORED_FRACTION = -10000000.0;
  private static final long MIN_BYTES = 1000;
  private static final OffsetRange RANGE = new OffsetRange(123L, Long.MAX_VALUE);
  private final TopicBacklogReader unownedBacklogReader = mock(TopicBacklogReader.class);

  @Spy Ticker ticker;
  private OffsetByteRangeTracker tracker;

  @Before
  public void setUp() {
    initMocks(this);
    when(ticker.read()).thenReturn(0L);
    tracker = new OffsetByteRangeTracker(OffsetByteRange.of(RANGE, 0), unownedBacklogReader);
  }

  @Test
  public void progressTracked() {
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(123), 10)));
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(124), 11)));
    when(unownedBacklogReader.computeMessageStats(Offset.of(125)))
        .thenReturn(ComputeMessageStatsResponse.newBuilder().setMessageBytes(1000).build());
    Progress progress = tracker.getProgress();
    assertEquals(21, progress.getWorkCompleted(), .0001);
    assertEquals(1000, progress.getWorkRemaining(), .0001);
  }

  @Test
  public void getProgressStatsFailure() {
    when(unownedBacklogReader.computeMessageStats(Offset.of(123)))
        .thenThrow(new CheckedApiException(Code.INTERNAL).underlying);
    assertThrows(ApiException.class, tracker::getProgress);
  }

  @Test
  @SuppressWarnings({"dereference.of.nullable", "argument"})
  public void claimSplitSuccess() {
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(1_000), MIN_BYTES)));
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(10_000), MIN_BYTES)));
    SplitResult<OffsetByteRange> splits = tracker.trySplit(IGNORED_FRACTION);
    OffsetByteRange primary = splits.getPrimary();
    assertEquals(RANGE.getFrom(), primary.getRange().getFrom());
    assertEquals(10_001, primary.getRange().getTo());
    assertEquals(MIN_BYTES * 2, primary.getByteCount());
    OffsetByteRange residual = splits.getResidual();
    assertEquals(10_001, residual.getRange().getFrom());
    assertEquals(Long.MAX_VALUE, residual.getRange().getTo());
    assertEquals(0, residual.getByteCount());
    assertEquals(splits.getPrimary(), tracker.currentRestriction());
    tracker.checkDone();
    assertNull(tracker.trySplit(IGNORED_FRACTION));
  }

  @Test
  @SuppressWarnings({"dereference.of.nullable", "argument"})
  public void splitWithoutClaimEmpty() {
    when(ticker.read()).thenReturn(100000000000000L);
    SplitResult<OffsetByteRange> splits = tracker.trySplit(IGNORED_FRACTION);
    assertEquals(RANGE.getFrom(), splits.getPrimary().getRange().getFrom());
    assertEquals(RANGE.getFrom(), splits.getPrimary().getRange().getTo());
    assertEquals(RANGE, splits.getResidual().getRange());
    assertEquals(splits.getPrimary(), tracker.currentRestriction());
    tracker.checkDone();
    assertNull(tracker.trySplit(IGNORED_FRACTION));
  }

  @Test
  public void unboundedNotDone() {
    assertThrows(IllegalStateException.class, tracker::checkDone);
  }

  @Test
  public void cannotClaimBackwards() {
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(1_000), MIN_BYTES)));
    assertThrows(
        IllegalArgumentException.class,
        () -> tracker.tryClaim(OffsetByteProgress.of(Offset.of(1_000), MIN_BYTES)));
    assertThrows(
        IllegalArgumentException.class,
        () -> tracker.tryClaim(OffsetByteProgress.of(Offset.of(999), MIN_BYTES)));
  }

  @Test
  public void cannotClaimSplitRange() {
    assertTrue(tracker.tryClaim(OffsetByteProgress.of(Offset.of(1_000), MIN_BYTES)));
    assertNotNull(tracker.trySplit(IGNORED_FRACTION));
    assertFalse(tracker.tryClaim(OffsetByteProgress.of(Offset.of(1_001), MIN_BYTES)));
  }
}
