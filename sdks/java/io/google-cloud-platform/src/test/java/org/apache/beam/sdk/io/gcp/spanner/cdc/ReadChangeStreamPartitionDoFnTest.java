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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DeletePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DonePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.FinishPartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForParentPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadChangeStreamPartitionDoFnTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP =
      Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final boolean PARTITION_IS_INCLUSIVE_START = true;
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  private static final boolean PARTITION_IS_INCLUSIVE_END = false;
  private static final long PARTITION_HEARTBEAT_MILLIS = 30_000L;

  private ReadChangeStreamPartitionDoFn doFn;
  private PartitionMetadata partition;
  private PartitionRestriction restriction;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> restrictionTracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private DataChangeRecordAction dataChangeRecordAction;
  private HeartbeatRecordAction heartbeatRecordAction;
  private ChildPartitionsRecordAction childPartitionsRecordAction;
  private QueryChangeStreamAction queryChangeStreamAction;
  private WaitForChildPartitionsAction waitForChildPartitionsAction;
  private FinishPartitionAction finishPartitionAction;
  private WaitForParentPartitionsAction waitForParentPartitionsAction;
  private DeletePartitionAction deletePartitionAction;
  private DonePartitionAction donePartitionAction;

  @Before
  public void setUp() {
    final Duration resumeDuration = Duration.millis(100);
    final DaoFactory daoFactory = mock(DaoFactory.class);
    final MapperFactory mapperFactory = mock(MapperFactory.class);
    final ActionFactory actionFactory = mock(ActionFactory.class);
    final PartitionMetadataDao partitionMetadataDao = mock(PartitionMetadataDao.class);
    ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
    ChangeStreamRecordMapper changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);
    dataChangeRecordAction = mock(DataChangeRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);
    queryChangeStreamAction = mock(QueryChangeStreamAction.class);
    waitForChildPartitionsAction = mock(WaitForChildPartitionsAction.class);
    finishPartitionAction = mock(FinishPartitionAction.class);
    waitForParentPartitionsAction = mock(WaitForParentPartitionsAction.class);
    deletePartitionAction = mock(DeletePartitionAction.class);
    donePartitionAction = mock(DonePartitionAction.class);

    doFn = new ReadChangeStreamPartitionDoFn(daoFactory, mapperFactory, actionFactory);

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
    restrictionTracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(restrictionTracker.currentRestriction()).thenReturn(restriction);
    when(restriction.getStartTimestamp()).thenReturn(PARTITION_START_TIMESTAMP);
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(daoFactory.getChangeStreamDao()).thenReturn(changeStreamDao);
    when(mapperFactory.changeStreamRecordMapper()).thenReturn(changeStreamRecordMapper);

    when(actionFactory.dataChangeRecordAction()).thenReturn(dataChangeRecordAction);
    when(actionFactory.heartbeatRecordAction()).thenReturn(heartbeatRecordAction);
    when(actionFactory.childPartitionsRecordAction(partitionMetadataDao))
        .thenReturn(childPartitionsRecordAction);
    when(actionFactory.queryChangeStreamAction(
            changeStreamDao,
            changeStreamRecordMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction))
        .thenReturn(queryChangeStreamAction);
    when(actionFactory.waitForChildPartitionsAction(partitionMetadataDao, resumeDuration))
        .thenReturn(waitForChildPartitionsAction);
    when(actionFactory.finishPartitionAction(partitionMetadataDao))
        .thenReturn(finishPartitionAction);
    when(actionFactory.waitForParentPartitionsAction(partitionMetadataDao, resumeDuration))
        .thenReturn(waitForParentPartitionsAction);
    when(actionFactory.deletePartitionAction(partitionMetadataDao))
        .thenReturn(deletePartitionAction);
    when(actionFactory.donePartitionAction()).thenReturn(donePartitionAction);

    doFn.setup();
  }

  @Test
  public void testQueryChangeStreamMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.QUERY_CHANGE_STREAM);
    when(queryChangeStreamAction.run(any(), any(), any(), any()))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(queryChangeStreamAction)
        .run(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    verify(waitForChildPartitionsAction, never()).run(any(), any());
    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(donePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testWaitForChildPartitionsMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.WAIT_FOR_CHILD_PARTITIONS);
    when(waitForChildPartitionsAction.run(any(), any()))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(waitForChildPartitionsAction).run(partition, restrictionTracker);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(donePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testFinishPartitionMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.FINISH_PARTITION);
    when(finishPartitionAction.run(any(), any()))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(finishPartitionAction).run(partition, restrictionTracker);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(donePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testWaitForParentPartitionsMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.WAIT_FOR_PARENT_PARTITIONS);
    when(waitForParentPartitionsAction.run(any(), any()))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(waitForParentPartitionsAction).run(partition, restrictionTracker);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(donePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testDeletePartitionMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.DELETE_PARTITION);
    when(deletePartitionAction.run(any(), any()))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(deletePartitionAction).run(partition, restrictionTracker);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(donePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testDoneMode() {
    when(restriction.getMode()).thenReturn(PartitionMode.DONE);
    when(donePartitionAction.run(any(), any())).thenReturn(ProcessContinuation.stop());

    final ProcessContinuation result =
        doFn.processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(donePartitionAction).run(partition, restrictionTracker);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  // --------------------------
  // Sad Paths

  // Client library errors:
  //   1. RESOURCE_EXHAUSTED error on client library
  //   2. DEADLINE_EXCEEDED error on client library
  //   3. INTERNAL error on client library
  //   4. UNAVAILABLE error on client library
  //   5. UNKNOWN error on client library (transaction outcome unknown)
  //   6. ABORTED error on client library
  //   7. UNAUTHORIZED error on client library

  // Metadata table
  //   - Table is deleted
  //   - Database is deleted
  //   - No permissions for the metadata table
  // --------------------------

}
