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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.ReadChangeStreamPartitionRangeTracker;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class DataChangeRecordActionTest {

  private DataChangeRecordAction action;
  private PartitionMetadata partition;
  private ReadChangeStreamPartitionRangeTracker tracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private BytesThroughputEstimator<DataChangeRecord> throughputEstimator;

  @Before
  public void setUp() {
    throughputEstimator = mock(BytesThroughputEstimator.class);
    action = new DataChangeRecordAction(throughputEstimator);
    partition = mock(PartitionMetadata.class);
    tracker = mock(ReadChangeStreamPartitionRangeTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);
    final Instant instant = new Instant(timestamp.toSqlTimestamp().getTime());
    final DataChangeRecord record = mock(DataChangeRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.shouldContinue(timestamp)).thenReturn(true);
    when(tracker.tryClaim(timestamp)).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(outputReceiver).outputWithTimestamp(record, instant);
    verify(watermarkEstimator).setWatermark(instant);
    verify(throughputEstimator).update(any(), eq(record));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);
    final DataChangeRecord record = mock(DataChangeRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.shouldContinue(timestamp)).thenReturn(true);
    when(tracker.tryClaim(timestamp)).thenReturn(false);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(outputReceiver, never()).outputWithTimestamp(any(), any());
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(throughputEstimator, never()).update(any(), any());
  }

  @Test
  public void testSoftDeadlineReached() {
    final String partitionToken = "partitionToken";
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(10L);
    final DataChangeRecord record = mock(DataChangeRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.shouldContinue(timestamp)).thenReturn(false);
    when(tracker.tryClaim(timestamp)).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.resume()), maybeContinuation);
    verify(outputReceiver, never()).outputWithTimestamp(any(), any());
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(throughputEstimator, never()).update(any(), any());
  }
}
