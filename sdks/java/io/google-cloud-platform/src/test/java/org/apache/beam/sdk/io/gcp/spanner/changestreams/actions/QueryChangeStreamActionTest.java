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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.actions;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestTransactionAnswer;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class QueryChangeStreamActionTest {
  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP = Timestamp.ofTimeMicroseconds(10L);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeMicroseconds(30L);
  private static final long PARTITION_END_MICROS = 30L;
  private static final long PARTITION_HEARTBEAT_MILLIS = 30_000L;
  private static final Instant WATERMARK = Instant.now();
  private static final Timestamp WATERMARK_TIMESTAMP =
      TimestampConverter.timestampFromMillis(WATERMARK.getMillis());

  private ChangeStreamDao changeStreamDao;
  private PartitionMetadataDao partitionMetadataDao;
  private InTransactionContext transaction;
  private PartitionMetadata partition;
  private OffsetRange restriction;
  private RestrictionTracker<OffsetRange, Long> restrictionTracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ChangeStreamRecordMapper changeStreamRecordMapper;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private BundleFinalizer bundleFinalizer;
  private DataChangeRecordAction dataChangeRecordAction;
  private HeartbeatRecordAction heartbeatRecordAction;
  private ChildPartitionsRecordAction childPartitionsRecordAction;
  private QueryChangeStreamAction action;

  @Before
  public void setUp() throws Exception {
    changeStreamDao = mock(ChangeStreamDao.class);
    partitionMetadataDao = mock(PartitionMetadataDao.class);
    transaction = mock(InTransactionContext.class);
    changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);
    dataChangeRecordAction = mock(DataChangeRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);

    action =
        new QueryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction);

    partition =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet("parentToken"))
            .setStartTimestamp(PARTITION_START_TIMESTAMP)
            .setEndTimestamp(PARTITION_END_TIMESTAMP)
            .setHeartbeatMillis(PARTITION_HEARTBEAT_MILLIS)
            .setState(SCHEDULED)
            .setScheduledAt(Timestamp.now())
            .build();
    restriction = mock(OffsetRange.class);
    restrictionTracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
    bundleFinalizer = new BundleFinalizerStub();

    when(restrictionTracker.currentRestriction()).thenReturn(restriction);
    when(restriction.getFrom()).thenReturn(10L);
    when(partitionMetadataDao.runInTransaction(any()))
        .thenAnswer(new TestTransactionAnswer(transaction));
    when(transaction.getPartition(PARTITION_TOKEN)).thenReturn(partition);
  }

  @Test
  public void testQueryChangeStreamWithDataChangeRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final DataChangeRecord record1 = mock(DataChangeRecord.class);
    final DataChangeRecord record2 = mock(DataChangeRecord.class);
    when(record1.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(record2.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, rowAsStruct, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(dataChangeRecordAction.run(
            partition, record1, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(dataChangeRecordAction.run(
            partition, record2, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(dataChangeRecordAction)
        .run(partition, record1, restrictionTracker, outputReceiver, watermarkEstimator);
    verify(dataChangeRecordAction)
        .run(partition, record2, restrictionTracker, outputReceiver, watermarkEstimator);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamWithHeartbeatRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final HeartbeatRecord record1 = mock(HeartbeatRecord.class);
    final HeartbeatRecord record2 = mock(HeartbeatRecord.class);
    when(record1.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(record2.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, rowAsStruct, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(heartbeatRecordAction.run(partition, record1, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(heartbeatRecordAction.run(partition, record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(heartbeatRecordAction).run(partition, record1, restrictionTracker, watermarkEstimator);
    verify(heartbeatRecordAction).run(partition, record2, restrictionTracker, watermarkEstimator);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamWithChildPartitionsRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final ChildPartitionsRecord record1 = mock(ChildPartitionsRecord.class);
    final ChildPartitionsRecord record2 = mock(ChildPartitionsRecord.class);
    when(record1.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(record2.getRecordTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, rowAsStruct, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(
            partition, record1, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(childPartitionsRecordAction.run(
            partition, record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(childPartitionsRecordAction)
        .run(partition, record1, restrictionTracker, watermarkEstimator);
    verify(childPartitionsRecordAction)
        .run(partition, record2, restrictionTracker, watermarkEstimator);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamWithRestrictionStartAfterPartitionStart() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final ChildPartitionsRecord record1 = mock(ChildPartitionsRecord.class);
    final ChildPartitionsRecord record2 = mock(ChildPartitionsRecord.class);

    // One microsecond after partition start timestamp
    when(restriction.getFrom()).thenReturn(11L);
    // This record should be ignored because it is before restriction.getFrom
    when(record1.getRecordTimestamp()).thenReturn(Timestamp.ofTimeMicroseconds(10L));
    // This record should be included because it is at the restriction.getFrom
    when(record2.getRecordTimestamp()).thenReturn(Timestamp.ofTimeMicroseconds(11L));
    // We should start the query 1 microsecond before the restriction.getFrom
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            Timestamp.ofTimeMicroseconds(10L),
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, rowAsStruct, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(
            partition, record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(childPartitionsRecordAction)
        .run(partition, record2, restrictionTracker, watermarkEstimator);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(childPartitionsRecordAction, never())
        .run(partition, record1, restrictionTracker, watermarkEstimator);
    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamWithStreamFinished() {
    final ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(changeStreamResultSet);
    when(changeStreamResultSet.next()).thenReturn(false);
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);
    when(restrictionTracker.tryClaim(PARTITION_END_MICROS)).thenReturn(true);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);
    verify(partitionMetadataDao).updateToFinished(PARTITION_TOKEN);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
  }

  private static class BundleFinalizerStub implements BundleFinalizer {
    @Override
    public void afterBundleCommit(Instant callbackExpiry, Callback callback) {
      try {
        callback.onBundleSuccess();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
