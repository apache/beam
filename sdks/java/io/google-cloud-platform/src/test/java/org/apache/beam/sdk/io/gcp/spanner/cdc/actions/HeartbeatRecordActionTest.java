package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class HeartbeatRecordActionTest {

  private HeartbeatRecordAction action;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;

  @Before
  public void setUp() {
    action = new HeartbeatRecordAction();
    tracker = mock(RestrictionTracker.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);

    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(true);

    final Optional<ProcessContinuation> maybeContinuation = action.run(
        new HeartbeatRecord(timestamp),
        tracker,
        watermarkEstimator
    );

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);

    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation = action.run(
        new HeartbeatRecord(timestamp),
        tracker,
        watermarkEstimator
    );

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
  }
}
