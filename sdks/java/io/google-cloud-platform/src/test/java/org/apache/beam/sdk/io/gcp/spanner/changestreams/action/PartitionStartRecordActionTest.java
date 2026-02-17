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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.CREATED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionStartRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.RestrictionInterrupter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestTransactionAnswer;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class PartitionStartRecordActionTest {

  private PartitionMetadataDao dao;
  private InTransactionContext transaction;
  private ChangeStreamMetrics metrics;
  private PartitionStartRecordAction action;
  private RestrictionTracker<TimestampRange, Timestamp> tracker;
  private RestrictionInterrupter<Timestamp> interrupter;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    transaction = mock(InTransactionContext.class);
    metrics = mock(ChangeStreamMetrics.class);
    action = new PartitionStartRecordAction(dao, metrics);
    tracker = mock(RestrictionTracker.class);
    interrupter = mock(RestrictionInterrupter.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(dao.runInTransaction(any(), any())).thenAnswer(new TestTransactionAnswer(transaction));
  }

  @Test
  public void testRestrictionClaimed() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(20L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final PartitionStartRecord record =
        new PartitionStartRecord(
            startTimestamp, "recordSequence", Arrays.asList(partitionToken), null);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);
    when(transaction.getPartition(partitionToken)).thenReturn(null);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken(partitionToken)
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .setWatermark(startTimestamp)
                .build());
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final PartitionStartRecord record =
        new PartitionStartRecord(
            startTimestamp, "recordSequence", Arrays.asList(partitionToken), null);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(dao, never()).insert(any());
  }

  @Test
  public void testSoftDeadlineReached() {
    final String partitionToken = "partitionToken";
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final PartitionStartRecord record =
        new PartitionStartRecord(
            startTimestamp, "recordSequence", Arrays.asList(partitionToken), null);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(interrupter.tryInterrupt(startTimestamp)).thenReturn(true);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.resume()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(dao, never()).insert(any());
  }
}
