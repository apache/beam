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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.RestrictionInterrupter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class QueryChangeStreamActionTest {
  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP = Timestamp.ofTimeMicroseconds(10L);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeMicroseconds(30L);
  private static final long PARTITION_HEARTBEAT_MILLIS = 30_000L;
  private static final Instant WATERMARK = Instant.now();
  private static final Timestamp WATERMARK_TIMESTAMP =
      Timestamp.ofTimeMicroseconds(WATERMARK.getMillis() * 1_000L);

  private ChangeStreamDao changeStreamDao;
  private PartitionMetadataDao partitionMetadataDao;
  private PartitionMetadata partition;
  private ChangeStreamMetrics metrics;
  private TimestampRange restriction;
  private RestrictionTracker<TimestampRange, Timestamp> restrictionTracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ChangeStreamRecordMapper changeStreamRecordMapper;
  private PartitionMetadataMapper partitionMetadataMapper;
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
    changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);
    partitionMetadataMapper = mock(PartitionMetadataMapper.class);
    dataChangeRecordAction = mock(DataChangeRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);
    metrics = mock(ChangeStreamMetrics.class);

    action =
        new QueryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            partitionMetadataMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction,
            metrics);
    final Struct row = mock(Struct.class);
    partition =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet("parentToken"))
            .setStartTimestamp(PARTITION_START_TIMESTAMP)
            .setEndTimestamp(PARTITION_END_TIMESTAMP)
            .setHeartbeatMillis(PARTITION_HEARTBEAT_MILLIS)
            .setState(SCHEDULED)
            .setWatermark(WATERMARK_TIMESTAMP)
            .setScheduledAt(Timestamp.now())
            .build();
    restriction = mock(TimestampRange.class);
    restrictionTracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
    bundleFinalizer = new BundleFinalizerStub();

    when(restrictionTracker.currentRestriction()).thenReturn(restriction);
    when(restriction.getFrom()).thenReturn(PARTITION_START_TIMESTAMP);
    when(restriction.getTo()).thenReturn(PARTITION_END_TIMESTAMP);
    when(partitionMetadataDao.getPartition(PARTITION_TOKEN)).thenReturn(row);
    when(partitionMetadataMapper.from(row)).thenReturn(partition);
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
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(dataChangeRecordAction.run(
            eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(outputReceiver), eq(watermarkEstimator)))
        .thenReturn(Optional.empty());
    when(dataChangeRecordAction.run(
            eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(outputReceiver), eq(watermarkEstimator)))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(dataChangeRecordAction)
        .run(eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(outputReceiver), eq(watermarkEstimator));
    verify(dataChangeRecordAction)
        .run(eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(outputReceiver), eq(watermarkEstimator));
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any(), any());
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
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(heartbeatRecordAction.run(eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator)))
        .thenReturn(Optional.empty());
    when(heartbeatRecordAction.run(eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator)))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(heartbeatRecordAction).run(eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(heartbeatRecordAction).run(eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any(), any());
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
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(
            eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator)))
        .thenReturn(Optional.empty());
    when(childPartitionsRecordAction.run(
            eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator)))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(childPartitionsRecordAction)
        .run(eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(childPartitionsRecordAction)
        .run(eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamWithRestrictionFromAfterPartitionStart() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final ChildPartitionsRecord record1 = mock(ChildPartitionsRecord.class);
    final ChildPartitionsRecord record2 = mock(ChildPartitionsRecord.class);

    // From is after Partition start at
    when(restriction.getFrom()).thenReturn(Timestamp.ofTimeMicroseconds(15L));
    // Both records should be included
    when(record1.getRecordTimestamp()).thenReturn(Timestamp.ofTimeMicroseconds(15L));
    when(record2.getRecordTimestamp()).thenReturn(Timestamp.ofTimeMicroseconds(25L));
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            Timestamp.ofTimeMicroseconds(15L),
            PARTITION_END_TIMESTAMP,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(
        eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator)))
        .thenReturn(Optional.of(ProcessContinuation.stop()));
    when(watermarkEstimator.currentWatermark()).thenReturn(WATERMARK);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(childPartitionsRecordAction)
        .run(eq(partition), eq(record1), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(childPartitionsRecordAction)
        .run(eq(partition), eq(record2), eq(restrictionTracker), any(RestrictionInterrupter.class), eq(watermarkEstimator));
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any(), any());
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
    when(restrictionTracker.tryClaim(PARTITION_END_TIMESTAMP)).thenReturn(true);

    final ProcessContinuation result =
        action.run(
            partition, restrictionTracker, outputReceiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(partitionMetadataDao).updateWatermark(PARTITION_TOKEN, WATERMARK_TIMESTAMP);
    verify(partitionMetadataDao).updateToFinished(PARTITION_TOKEN);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any(), any());
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
