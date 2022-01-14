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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.junit.Test;

public class ReadChangeStreamPartitionRangeTrackerTest {

  @Test
  public void testTryClaim() {
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final OffsetRange range = new OffsetRange(100, 200);
    final ReadChangeStreamPartitionRangeTracker tracker =
        new ReadChangeStreamPartitionRangeTracker(partition, range);
    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(100L));
    assertTrue(tracker.tryClaim(100L));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(199L));
    assertFalse(tracker.tryClaim(200L));
  }

  @Test
  public void testTrySplitReturnsNullForInitialPartition() {
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final OffsetRange range = new OffsetRange(100, 200);
    final ReadChangeStreamPartitionRangeTracker tracker =
        new ReadChangeStreamPartitionRangeTracker(partition, range);

    when(partition.getPartitionToken()).thenReturn(InitialPartition.PARTITION_TOKEN);

    assertNull(tracker.trySplit(0.0D));
  }
}
