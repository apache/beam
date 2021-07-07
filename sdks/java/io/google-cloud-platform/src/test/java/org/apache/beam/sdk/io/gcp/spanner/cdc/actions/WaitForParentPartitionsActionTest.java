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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class WaitForParentPartitionsActionTest {

  private PartitionMetadataDao dao;
  private Duration resumeDuration;
  private WaitForParentPartitionsAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    resumeDuration = Duration.millis(100L);
    action = new WaitForParentPartitionsAction(dao, resumeDuration);
    tracker = mock(RestrictionTracker.class);
  }

  @Test
  public void testRestrictionClaimedAndAllParentsDeleted() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.waitForParentPartitions())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(dao.countExistingParents(partitionToken)).thenReturn(0L);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.empty(), maybeContinuation);
  }

  @Test
  public void testRestrictionClaimedAndAtLeastOneParentNotDeleted() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.waitForParentPartitions())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(dao.countExistingParents(partitionToken)).thenReturn(1L);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(
        Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration)),
        maybeContinuation);
  }
}
