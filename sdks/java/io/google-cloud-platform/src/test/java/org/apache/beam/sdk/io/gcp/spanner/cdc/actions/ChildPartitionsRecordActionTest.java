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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.util.TestTransactionAnswer;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class ChildPartitionsRecordActionTest {

  private PartitionMetadataDao dao;
  private InTransactionContext transaction;
  private ChildPartitionsRecordAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    transaction = mock(InTransactionContext.class);
    action = new ChildPartitionsRecordAction(dao);
    tracker = mock(RestrictionTracker.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(dao.runInTransaction(any())).thenAnswer(new TestTransactionAnswer(transaction));
  }

  @Test
  public void testRestrictionClaimedAndIsSplitCase() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)));
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(dao)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken("childPartition1")
                .setParentTokens(Sets.newHashSet(partitionToken))
                .setStartTimestamp(startTimestamp)
                .setInclusiveStart(true)
                .setEndTimestamp(endTimestamp)
                .setInclusiveEnd(false)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .build());
    verify(dao)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken("childPartition2")
                .setParentTokens(Sets.newHashSet(partitionToken))
                .setStartTimestamp(startTimestamp)
                .setInclusiveStart(true)
                .setEndTimestamp(endTimestamp)
                .setInclusiveEnd(false)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .build());
  }

  @Test
  public void testRestrictionClaimedAnsIsSplitCaseAndChildExists() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)));
    final SpannerException spannerException = mock(SpannerException.class);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);
    when(spannerException.getErrorCode()).thenReturn(ErrorCode.ALREADY_EXISTS);
    doThrow(spannerException).when(dao).insert(any());

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionClaimedAndIsMergeCaseAndAllParentsFinishedAndChildNotExists() {
    final String partitionToken = "partitionToken";
    final String anotherPartitionToken = "anotherPartitionToken";
    final HashSet<String> parentTokens = Sets.newHashSet(partitionToken, anotherPartitionToken);
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Collections.singletonList(new ChildPartition("childPartition1", parentTokens)));
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);
    when(transaction.countPartitionsInStates(parentTokens, Collections.singletonList(FINISHED)))
        .thenReturn(1L);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken("childPartition1")
                .setParentTokens(parentTokens)
                .setStartTimestamp(startTimestamp)
                .setInclusiveStart(true)
                .setEndTimestamp(endTimestamp)
                .setInclusiveEnd(false)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .build());
  }

  @Test
  public void testRestrictionClaimedAndIsMergeCaseAndAllParentsFinishedAndChildExists() {
    final String partitionToken = "partitionToken";
    final String anotherPartitionToken = "anotherPartitionToken";
    final HashSet<String> parentTokens = Sets.newHashSet(partitionToken, anotherPartitionToken);
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Collections.singletonList(new ChildPartition("childPartition1", parentTokens)));
    final SpannerException spannerException = mock(SpannerException.class);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);
    when(transaction.countPartitionsInStates(parentTokens, Collections.singletonList(FINISHED)))
        .thenReturn(1L);
    when(spannerException.getErrorCode()).thenReturn(ErrorCode.ALREADY_EXISTS);
    doThrow(spannerException).when(transaction).insert(any());

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionClaimedAndIsMergeCaseAndAtLeastOneParentIsNotFinished() {
    final String partitionToken = "partitionToken";
    final String anotherPartitionToken = "anotherPartitionToken";
    final HashSet<String> parentTokens = Sets.newHashSet(partitionToken, anotherPartitionToken);
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Collections.singletonList(new ChildPartition("childPartition1", parentTokens)));
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);
    when(transaction.countPartitionsInStates(parentTokens, Collections.singletonList(FINISHED)))
        .thenReturn(0L);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction, never()).insert(any());
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)));
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(dao, never()).insert(any());
  }
}
