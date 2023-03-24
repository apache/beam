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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.action;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamContinuationTokenHelper.getTokenWithCorrectPartition;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReadChangeStreamPartitionActionTest {

  private ReadChangeStreamPartitionAction action;

  private MetadataTableDao metadataTableDao;
  private ChangeStreamDao changeStreamDao;
  private ChangeStreamMetrics metrics;
  private ChangeStreamAction changeStreamAction;

  //    private PartitionRecord partitionRecord;
  private StreamProgress restriction;
  private RestrictionTracker<StreamProgress, StreamProgress> tracker;
  private DoFn.OutputReceiver<KV<ByteString, ChangeStreamMutation>> receiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  private ByteStringRange partition;
  private String uuid;
  private PartitionRecord partitionRecord;

  @Before
  public void setUp() throws Exception {
    metadataTableDao = mock(MetadataTableDao.class);
    changeStreamDao = mock(ChangeStreamDao.class);
    metrics = mock(ChangeStreamMetrics.class);
    changeStreamAction = mock(ChangeStreamAction.class);
    Duration heartbeatDuration = Duration.standardSeconds(1);

    action =
        new ReadChangeStreamPartitionAction(
            metadataTableDao, changeStreamDao, metrics, changeStreamAction, heartbeatDuration);

    restriction = mock(StreamProgress.class);
    tracker = mock(ReadChangeStreamPartitionProgressTracker.class);
    receiver = mock(DoFn.OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    partition = ByteStringRange.create("A", "B");
    uuid = "123456";
    Instant startTime = Instant.now();
    Instant parentLowWatermark = Instant.now();
    partitionRecord = new PartitionRecord(partition, startTime, uuid, parentLowWatermark);
    when(tracker.currentRestriction()).thenReturn(restriction);
    when(metadataTableDao.lockPartition(partition, uuid)).thenReturn(true);
    when(restriction.getCurrentToken()).thenReturn(null);
    when(restriction.getCloseStream()).thenReturn(null);
    // Setting watermark estimator to now so we don't debug.
    when(watermarkEstimator.getState()).thenReturn(Instant.now());
  }

  @Test
  public void testLockingRowSucceed() throws IOException {
    final ServerStream<ChangeStreamRecord> responses = mock(ServerStream.class);
    final Iterator<ChangeStreamRecord> responseIterator = mock(Iterator.class);
    when(responses.iterator()).thenReturn(responseIterator);

    Heartbeat mockHeartBeat = Mockito.mock(Heartbeat.class);
    when(responseIterator.next()).thenReturn(mockHeartBeat);
    when(responseIterator.hasNext()).thenReturn(true);
    when(changeStreamDao.readChangeStreamPartition(any(), any(), any(), anyBoolean()))
        .thenReturn(responses);

    when(changeStreamAction.run(any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(Optional.of(DoFn.ProcessContinuation.stop()));

    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);

    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Verify that on successful lock, we don't tryClaim on the tracker because we have no outputs.
    verify(tracker, never()).tryClaim(any());
    verify(changeStreamAction).run(any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void testLockingRowFailsStops() throws IOException {
    when(metadataTableDao.lockPartition(partition, uuid)).thenReturn(false);

    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);

    assertEquals(DoFn.ProcessContinuation.stop(), result);

    // On failure to lock, we try to claim a fail to lock, so it will terminate gracefully.
    StreamProgress streamProgress = new StreamProgress();
    streamProgress.setFailToLock(true);
    verify(tracker).tryClaim(streamProgress);
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void testThatChangeStreamWorkerCounterIsIncrementedOnInitialRun() throws IOException {
    // Return null token to indicate that this is the first ever run.
    when(restriction.getCurrentToken()).thenReturn(null);
    when(restriction.getCloseStream()).thenReturn(null);

    final ServerStream<ChangeStreamRecord> responses = mock(ServerStream.class);
    final Iterator<ChangeStreamRecord> responseIterator = mock(Iterator.class);
    when(responses.iterator()).thenReturn(responseIterator);

    Heartbeat mockHeartBeat = Mockito.mock(Heartbeat.class);
    when(responseIterator.next()).thenReturn(mockHeartBeat);
    when(responseIterator.hasNext()).thenReturn(true);
    when(changeStreamDao.readChangeStreamPartition(any(), any(), any(), anyBoolean()))
        .thenReturn(responses);

    when(changeStreamAction.run(any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(Optional.of(DoFn.ProcessContinuation.stop()));

    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    verify(changeStreamAction).run(any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void testCloseStreamTerminateOKStatus() throws IOException {
    CloseStream mockCloseStream = Mockito.mock(CloseStream.class);
    Status statusProto = Status.newBuilder().setCode(0).build();
    Mockito.when(mockCloseStream.getStatus())
        .thenReturn(com.google.cloud.bigtable.common.Status.fromProto(statusProto));
    when(restriction.getCloseStream()).thenReturn(mockCloseStream);
    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Should terminate before reaching processing stream partition responses.
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
    // Should decrement the metric on termination.
    verify(metrics).decPartitionStreamCount();
    // Should not try to write any new partition to the metadata table.
    verify(metadataTableDao, never()).writeNewPartition(any(), any(), any(), any());
    verify(metadataTableDao, never()).deleteStreamPartitionRow(any());
  }

  @Test
  public void testCloseStreamTerminateNotOutOfRangeStatus() throws IOException {
    // Out of Range code is 11.
    CloseStream mockCloseStream = Mockito.mock(CloseStream.class);
    Status statusProto = Status.newBuilder().setCode(10).build();
    Mockito.when(mockCloseStream.getStatus())
        .thenReturn(com.google.cloud.bigtable.common.Status.fromProto(statusProto));
    when(restriction.getCloseStream()).thenReturn(mockCloseStream);
    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Should terminate before reaching processing stream partition responses.
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
    // Should decrement the metric on termination.
    verify(metrics).decPartitionStreamCount();
    // Should not try to write any new partition to the metadata table.
    verify(metadataTableDao, never()).writeNewPartition(any(), any(), any(), any());
    verify(metadataTableDao, never()).deleteStreamPartitionRow(any());
  }

  @Test
  public void testCloseStreamWritesContinuationTokens() throws IOException {
    ChangeStreamContinuationToken changeStreamContinuationToken1 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "AJ"), "1234");
    ChangeStreamContinuationToken changeStreamContinuationToken2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("AJ", "B"), "5678");

    CloseStream mockCloseStream = Mockito.mock(CloseStream.class);
    Status statusProto = Status.newBuilder().setCode(11).build();
    Mockito.when(mockCloseStream.getStatus())
        .thenReturn(com.google.cloud.bigtable.common.Status.fromProto(statusProto));
    Mockito.when(mockCloseStream.getChangeStreamContinuationTokens())
        .thenReturn(Arrays.asList(changeStreamContinuationToken1, changeStreamContinuationToken2));
    Mockito.when(mockCloseStream.getNewPartitions())
        .thenReturn(
            Arrays.asList(
                changeStreamContinuationToken1.getPartition(),
                changeStreamContinuationToken2.getPartition()));

    when(restriction.getCloseStream()).thenReturn(mockCloseStream);
    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Should terminate before reaching processing stream partition responses.
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
    // Should decrement the metric on termination.
    verify(metrics).decPartitionStreamCount();
    // Write the new partitions.
    verify(metadataTableDao)
        .writeNewPartition(any(), eq(changeStreamContinuationToken1), any(), any());
    verify(metadataTableDao)
        .writeNewPartition(any(), eq(changeStreamContinuationToken2), any(), any());
    verify(metadataTableDao, times(1)).deleteStreamPartitionRow(partitionRecord.getPartition());
  }

  @Test
  public void testCloseStreamNewPartitionMerge() throws IOException {
    // NewPartitions field includes the merge target. ChangeStreamContinuationToken's partition may
    // not be the same as the new partition.
    // AB merging into AC. The new partition is AC and the corresponding
    // ChangeStreamContinuationToken is for AB
    ByteStringRange childPartitionAC = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken parentTokenAB =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "B"), "AB");

    CloseStream mockCloseStream = Mockito.mock(CloseStream.class);
    Status statusProto = Status.newBuilder().setCode(11).build();
    Mockito.when(mockCloseStream.getStatus())
        .thenReturn(com.google.cloud.bigtable.common.Status.fromProto(statusProto));
    Mockito.when(mockCloseStream.getChangeStreamContinuationTokens())
        .thenReturn(Collections.singletonList(parentTokenAB));
    Mockito.when(mockCloseStream.getNewPartitions())
        .thenReturn(Collections.singletonList(childPartitionAC));

    when(restriction.getCloseStream()).thenReturn(mockCloseStream);
    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Should terminate before reaching processing stream partition responses.
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
    // Should decrement the metric on termination.
    verify(metrics).decPartitionStreamCount();
    // Write the new partitions.
    // Write the new partitions.
    verify(metadataTableDao)
        .writeNewPartition(eq(childPartitionAC), eq(parentTokenAB), eq(partition), any());
    verify(metadataTableDao, times(1)).deleteStreamPartitionRow(partitionRecord.getPartition());
    verify(metadataTableDao, times(1)).deleteStreamPartitionRow(partitionRecord.getPartition());
  }

  @Test
  public void testCloseStreamMergeWithoutNewPartitionsField() throws IOException {
    // This is testing the old version of the API without the NewPartitions field. This means the
    // ChangeStreamContinuationToken's partition represents the new partition.
    // AB merging into AC.
    ByteStringRange childPartitionAC = ByteStringRange.create("A", "C");
    // The partition in the token is different from the test above. The token is actually encoded
    // for partition AB, but in this version, the partition in the token represents the NEW (child)
    // partition. This has been replaced by the new_partitions field in CloseStream.
    ChangeStreamContinuationToken parentTokenAB =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "C"), "AB");

    CloseStream mockCloseStream = Mockito.mock(CloseStream.class);
    Status statusProto = Status.newBuilder().setCode(11).build();
    Mockito.when(mockCloseStream.getStatus())
        .thenReturn(com.google.cloud.bigtable.common.Status.fromProto(statusProto));
    Mockito.when(mockCloseStream.getChangeStreamContinuationTokens())
        .thenReturn(Collections.singletonList(parentTokenAB));
    Mockito.when(mockCloseStream.getNewPartitions()).thenReturn(Collections.emptyList());

    when(restriction.getCloseStream()).thenReturn(mockCloseStream);
    final DoFn.ProcessContinuation result =
        action.run(partitionRecord, tracker, receiver, watermarkEstimator);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    // Should terminate before reaching processing stream partition responses.
    verify(changeStreamAction, never()).run(any(), any(), any(), any(), any(), anyBoolean());
    // Should decrement the metric on termination.
    verify(metrics).decPartitionStreamCount();
    // We have to correct the partition in the tokens if we don't have new_partitions field.
    ChangeStreamContinuationToken realTokenAB =
        getTokenWithCorrectPartition(ByteStringRange.create("A", "B"), parentTokenAB);
    // Write the new partitions.
    verify(metadataTableDao)
        .writeNewPartition(eq(childPartitionAC), eq(realTokenAB), eq(partition), any());
    verify(metadataTableDao, times(1)).deleteStreamPartitionRow(partitionRecord.getPartition());
  }
}
