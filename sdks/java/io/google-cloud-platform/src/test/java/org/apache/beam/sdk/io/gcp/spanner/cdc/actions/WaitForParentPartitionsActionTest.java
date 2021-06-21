package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.junit.Assert.*;
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

    assertEquals(Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration)), maybeContinuation);
  }
}
