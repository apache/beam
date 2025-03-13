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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.RestrictionInterrupter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class HeartbeatRecordActionTest {

  private HeartbeatRecordAction action;
  private PartitionMetadata partition;
  private RestrictionTracker<TimestampRange, Timestamp> tracker;
  private RestrictionInterrupter<Timestamp> interrupter;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    final ChangeStreamMetrics metrics = mock(ChangeStreamMetrics.class);
    action = new HeartbeatRecordAction(metrics);
    partition = mock(PartitionMetadata.class);
    tracker = mock(RestrictionTracker.class);
    interrupter = mock(RestrictionInterrupter.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);

    when(tracker.tryClaim(timestamp)).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(
            partition,
            new HeartbeatRecord(timestamp, null),
            tracker,
            interrupter,
            watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);

    when(tracker.tryClaim(timestamp)).thenReturn(false);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(
            partition,
            new HeartbeatRecord(timestamp, null),
            tracker,
            interrupter,
            watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
  }

  @Test
  public void testSoftDeadlineReached() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);

    when(interrupter.tryInterrupt(timestamp)).thenReturn(true);
    when(tracker.tryClaim(timestamp)).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(
            partition,
            new HeartbeatRecord(timestamp, null),
            tracker,
            interrupter,
            watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.resume()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
  }
}
