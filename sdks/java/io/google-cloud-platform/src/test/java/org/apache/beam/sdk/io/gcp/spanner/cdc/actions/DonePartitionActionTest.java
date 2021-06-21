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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.junit.Before;
import org.junit.Test;

public class DonePartitionActionTest {

  private DonePartitionAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private PartitionMetadata partition;

  @Before
  public void setUp() throws Exception {
    partition = mock(PartitionMetadata.class);
    tracker = mock(RestrictionTracker.class);
    action = new DonePartitionAction();
  }

  @Test
  public void testRestrictionClaimed() {
    when(tracker.tryClaim(any())).thenReturn(true);

    final ProcessContinuation continuation = action.run(partition, tracker);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(tracker).tryClaim(PartitionPosition.done());
  }

  @Test
  public void testRestrictionNotClaimed() {
    when(tracker.tryClaim(any())).thenReturn(false);

    final ProcessContinuation continuation = action.run(partition, tracker);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(tracker).tryClaim(PartitionPosition.done());
  }
}
