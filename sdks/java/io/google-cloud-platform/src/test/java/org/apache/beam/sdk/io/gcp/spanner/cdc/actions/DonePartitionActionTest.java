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
