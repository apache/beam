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
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.RestrictionInterrupter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestTransactionAnswer;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class ChildPartitionsRecordActionTest {

  private PartitionMetadataDao dao;
  private InTransactionContext transaction;
  private ChangeStreamMetrics metrics;
  private ChildPartitionsRecordAction action;
  private RestrictionTracker<TimestampRange, Timestamp> tracker;
  private RestrictionInterrupter<Timestamp> interrupter;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    transaction = mock(InTransactionContext.class);
    metrics = mock(ChangeStreamMetrics.class);
    action = new ChildPartitionsRecordAction(dao, metrics);
    tracker = mock(RestrictionTracker.class);
    interrupter = mock(RestrictionInterrupter.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(dao.runInTransaction(any(), anyObject()))
        .thenAnswer(new TestTransactionAnswer(transaction));
  }

  @Test
  public void testRestrictionClaimedAndIsSplitCase() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(20L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)),
            null);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);
    when(transaction.getPartition("childPartition1")).thenReturn(null);
    when(transaction.getPartition("childPartition2")).thenReturn(null);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken("childPartition1")
                .setParentTokens(Sets.newHashSet(partitionToken))
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .setWatermark(startTimestamp)
                .build());
    verify(transaction)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken("childPartition2")
                .setParentTokens(Sets.newHashSet(partitionToken))
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .setWatermark(startTimestamp)
                .build());
  }

  @Test
  public void testRestrictionClaimedAnsIsSplitCaseAndChildExists() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(20L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)),
            null);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);
    when(transaction.getPartition("childPartition1")).thenReturn(mock(Struct.class));
    when(transaction.getPartition("childPartition2")).thenReturn(mock(Struct.class));

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionClaimedAndIsMergeCaseAndChildNotExists() {
    final String partitionToken = "partitionToken";
    final String anotherPartitionToken = "anotherPartitionToken";
    final String childPartitionToken = "childPartition1";
    final HashSet<String> parentTokens = Sets.newHashSet(partitionToken, anotherPartitionToken);
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(20L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Collections.singletonList(new ChildPartition(childPartitionToken, parentTokens)),
            null);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);
    when(transaction.getPartition(childPartitionToken)).thenReturn(null);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction)
        .insert(
            PartitionMetadata.newBuilder()
                .setPartitionToken(childPartitionToken)
                .setParentTokens(parentTokens)
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setHeartbeatMillis(heartbeat)
                .setState(CREATED)
                .setWatermark(startTimestamp)
                .build());
  }

  @Test
  public void testRestrictionClaimedAndIsMergeCaseAndChildExists() {
    final String partitionToken = "partitionToken";
    final String anotherPartitionToken = "anotherPartitionToken";
    final String childPartitionToken = "childPartition1";
    final HashSet<String> parentTokens = Sets.newHashSet(partitionToken, anotherPartitionToken);
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(20L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Collections.singletonList(new ChildPartition(childPartitionToken, parentTokens)),
            null);
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatMillis()).thenReturn(heartbeat);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(tracker.tryClaim(startTimestamp)).thenReturn(true);
    when(transaction.getPartition(childPartitionToken)).thenReturn(mock(Struct.class));

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(partition, record, tracker, interrupter, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(transaction, never()).insert(any());
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(10L);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)),
            null);
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
    final ChildPartitionsRecord record =
        new ChildPartitionsRecord(
            startTimestamp,
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", partitionToken),
                new ChildPartition("childPartition2", partitionToken)),
            null);
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
