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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;

public class DetectNewPartitionsRangeTrackerTest {

  private TimestampRange range;
  private DetectNewPartitionsRangeTracker tracker;

  @Before
  public void setUp() throws Exception {
    range = TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), Timestamp.ofTimeMicroseconds(20L));
    tracker = new DetectNewPartitionsRangeTracker(range);
  }

  @Test
  public void testTryClaim() {
    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(10L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(10L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(11L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(11L)));
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(19L)));
    assertFalse(tracker.tryClaim(Timestamp.ofTimeMicroseconds(20L)));
  }

  @Test
  public void testTrySplitRefusesDynamicSplit() {
    assertNull(tracker.trySplit(0.5));
    assertEquals(range, tracker.currentRestriction());

    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(12L)));
    assertNull(tracker.trySplit(0.5));
    assertEquals(range, tracker.currentRestriction());
  }

  @Test
  public void testTrySplitAllowsCheckpoint() {
    assertTrue(tracker.tryClaim(Timestamp.ofTimeMicroseconds(12L)));
    SplitResult<TimestampRange> split = tracker.trySplit(0.0);
    assertNotNull(split);
    Timestamp expectedSplitPos = TimestampUtils.next(Timestamp.ofTimeMicroseconds(12L));
    assertEquals(
        TimestampRange.of(Timestamp.ofTimeMicroseconds(10L), expectedSplitPos), split.getPrimary());
    assertEquals(
        TimestampRange.of(expectedSplitPos, Timestamp.ofTimeMicroseconds(20L)),
        split.getResidual());
    assertEquals(split.getPrimary(), tracker.currentRestriction());
  }
}
