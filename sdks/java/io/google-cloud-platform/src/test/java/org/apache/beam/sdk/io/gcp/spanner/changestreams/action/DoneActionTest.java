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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.action;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.junit.Before;
import org.junit.Test;

public class DoneActionTest {

  private DoneAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private PartitionMetadata partition;

  @Before
  public void setUp() throws Exception {
    partition = mock(PartitionMetadata.class);
    tracker = mock(RestrictionTracker.class);
    action = new DoneAction();
  }

  @Test
  public void testRestrictionClaimed() {
    final String partitionToken = "partitionToken";
    when(tracker.tryClaim(any())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final ProcessContinuation continuation = action.run(partition, tracker);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(tracker).tryClaim(PartitionPosition.done());
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    when(tracker.tryClaim(any())).thenReturn(false);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final ProcessContinuation continuation = action.run(partition, tracker);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(tracker).tryClaim(PartitionPosition.done());
  }
}
