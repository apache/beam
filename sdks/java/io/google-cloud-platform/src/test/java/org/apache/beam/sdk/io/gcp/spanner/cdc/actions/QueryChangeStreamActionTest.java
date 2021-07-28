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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;
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
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionMetadata;
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
  private static final Timestamp PARTITION_START_TIMESTAMP =
      Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final boolean PARTITION_IS_INCLUSIVE_START = true;
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  private static final boolean PARTITION_IS_INCLUSIVE_END = false;
  private static final long PARTITION_HEARTBEAT_MILLIS = 30_000L;

  private ChangeStreamDao changeStreamDao;
  private PartitionMetadata partition;
  private PartitionRestriction restriction;
  private PartitionRestrictionMetadata restrictionMetadata;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> restrictionTracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ChangeStreamRecordMapper changeStreamRecordMapper;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private DataChangeRecordAction dataChangeRecordAction;
  private HeartbeatRecordAction heartbeatRecordAction;
  private ChildPartitionsRecordAction childPartitionsRecordAction;
  private QueryChangeStreamAction action;

  @Before
  public void setUp() throws Exception {
    changeStreamDao = mock(ChangeStreamDao.class);
    changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);
    dataChangeRecordAction = mock(DataChangeRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);

    action =
        new QueryChangeStreamAction(
            changeStreamDao,
            changeStreamRecordMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction);

    partition =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet("parentToken"))
            .setStartTimestamp(PARTITION_START_TIMESTAMP)
            .setInclusiveStart(PARTITION_IS_INCLUSIVE_START)
            .setEndTimestamp(PARTITION_END_TIMESTAMP)
            .setInclusiveEnd(PARTITION_IS_INCLUSIVE_END)
            .setHeartbeatMillis(PARTITION_HEARTBEAT_MILLIS)
            .setState(SCHEDULED)
            .setScheduledAt(Timestamp.now())
            .build();
    restriction = mock(PartitionRestriction.class);
    restrictionMetadata = mock(PartitionRestrictionMetadata.class);
    restrictionTracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(restriction.getMetadata()).thenReturn(restrictionMetadata);
    when(restrictionTracker.currentRestriction()).thenReturn(restriction);
    when(restriction.getStartTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
  }

  @Test
  public void testQueryChangeStreamModeWithDataChangeRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final DataChangeRecord record1 = mock(DataChangeRecord.class);
    final DataChangeRecord record2 = mock(DataChangeRecord.class);
    when(restriction.getMode()).thenReturn(PartitionMode.QUERY_CHANGE_STREAM);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_START,
            PARTITION_END_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_END,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(
            PARTITION_TOKEN, rowAsStruct, resultSetMetadata, restrictionMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(dataChangeRecordAction.run(
            partition, record1, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(dataChangeRecordAction.run(
            partition, record2, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final Optional<ProcessContinuation> result =
        action.run(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), result);
    verify(dataChangeRecordAction)
        .run(partition, record1, restrictionTracker, outputReceiver, watermarkEstimator);
    verify(dataChangeRecordAction)
        .run(partition, record2, restrictionTracker, outputReceiver, watermarkEstimator);

    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamModeWithHeartbeatRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final HeartbeatRecord record1 = mock(HeartbeatRecord.class);
    final HeartbeatRecord record2 = mock(HeartbeatRecord.class);
    when(restriction.getMode()).thenReturn(PartitionMode.QUERY_CHANGE_STREAM);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_START,
            PARTITION_END_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_END,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(
            PARTITION_TOKEN, rowAsStruct, resultSetMetadata, restrictionMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(heartbeatRecordAction.run(partition, record1, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(heartbeatRecordAction.run(partition, record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final Optional<ProcessContinuation> result =
        action.run(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), result);
    verify(heartbeatRecordAction).run(partition, record1, restrictionTracker, watermarkEstimator);
    verify(heartbeatRecordAction).run(partition, record2, restrictionTracker, watermarkEstimator);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamModeWithChildPartitionsRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ChangeStreamResultSetMetadata resultSetMetadata =
        mock(ChangeStreamResultSetMetadata.class);
    final ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    final ChildPartitionsRecord record1 = mock(ChildPartitionsRecord.class);
    final ChildPartitionsRecord record2 = mock(ChildPartitionsRecord.class);
    when(restriction.getMode()).thenReturn(PartitionMode.QUERY_CHANGE_STREAM);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_START,
            PARTITION_END_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_END,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(resultSet.getMetadata()).thenReturn(resultSetMetadata);
    when(changeStreamRecordMapper.toChangeStreamRecords(
            PARTITION_TOKEN, rowAsStruct, resultSetMetadata, restrictionMetadata))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(
            partition, record1, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(childPartitionsRecordAction.run(
            partition, record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final Optional<ProcessContinuation> result =
        action.run(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), result);
    verify(childPartitionsRecordAction)
        .run(partition, record1, restrictionTracker, watermarkEstimator);
    verify(childPartitionsRecordAction)
        .run(partition, record2, restrictionTracker, watermarkEstimator);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testQueryChangeStreamModeWithStreamFinished() {
    final ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
    when(restriction.getMode()).thenReturn(PartitionMode.QUERY_CHANGE_STREAM);
    when(changeStreamDao.changeStreamQuery(
            PARTITION_TOKEN,
            PARTITION_START_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_START,
            PARTITION_END_TIMESTAMP,
            PARTITION_IS_INCLUSIVE_END,
            PARTITION_HEARTBEAT_MILLIS))
        .thenReturn(changeStreamResultSet);
    when(changeStreamResultSet.next()).thenReturn(false);

    final Optional<ProcessContinuation> result =
        action.run(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.empty(), result);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
  }
}
