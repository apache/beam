package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.junit.Before;
import org.junit.Test;

public class FinishPartitionActionTest {

  private FinishPartitionAction action;
  private PartitionMetadataDao dao;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;

  @Before
  public void setUp() throws Exception {
    dao = mock(PartitionMetadataDao.class);
    action = new FinishPartitionAction(dao);
    tracker = mock(RestrictionTracker.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.finishPartition())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(dao).updateState(partitionToken, FINISHED);
  }

  @Test
  public void testRestrictionNotClaimed() {
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.finishPartition())).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(dao, never()).updateState(anyString(), any());
  }
}
