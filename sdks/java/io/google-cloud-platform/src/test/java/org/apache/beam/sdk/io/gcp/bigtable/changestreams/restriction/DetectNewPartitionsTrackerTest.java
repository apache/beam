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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Test;

public class DetectNewPartitionsTrackerTest {

  @Test
  public void testTrySplit() {
    final DetectNewPartitionsTracker tracker = new DetectNewPartitionsTracker(0);
    assertNull(tracker.trySplit(.7));
    assertThrows(IllegalStateException.class, tracker::checkDone);

    assertTrue(tracker.tryClaim(tracker.currentRestriction().getFrom()));
    SplitResult<OffsetRange> checkpointResult = tracker.trySplit(0);
    tracker.checkDone(); // should not throw exception
    assertEquals(0L, checkpointResult.getPrimary().getFrom());
    assertEquals(1L, checkpointResult.getPrimary().getTo());
    assertEquals(1L, checkpointResult.getResidual().getFrom());
    assertEquals(Long.MAX_VALUE, checkpointResult.getResidual().getTo());
  }
}
