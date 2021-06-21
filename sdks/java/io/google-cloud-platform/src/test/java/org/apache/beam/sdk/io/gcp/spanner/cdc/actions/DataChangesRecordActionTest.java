package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class DataChangesRecordActionTest {

  private DataChangesRecordAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private OutputReceiver<DataChangesRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    action = new DataChangesRecordAction();
    tracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final DataChangesRecord record = mock(DataChangesRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(true);

    final Optional<ProcessContinuation> maybeContinuation = action
        .run(record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(outputReceiver).output(record);
    verify(watermarkEstimator).setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final DataChangesRecord record = mock(DataChangesRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation = action
        .run(record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(outputReceiver, never()).output(any());
    verify(watermarkEstimator, never()).setWatermark(any());
  }
}
