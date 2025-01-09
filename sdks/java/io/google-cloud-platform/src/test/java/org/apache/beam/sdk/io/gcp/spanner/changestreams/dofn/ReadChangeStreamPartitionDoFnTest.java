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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadChangeStreamPartitionDoFnTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP =
      Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  private static final long PARTITION_HEARTBEAT_MILLIS = 30_000L;

  private ReadChangeStreamPartitionDoFn doFn;
  private PartitionMetadata partition;
  private TimestampRange restriction;
  private RestrictionTracker<TimestampRange, Timestamp> tracker;
  private OutputReceiver<DataChangeRecord> receiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private BundleFinalizer bundleFinalizer;
  private DataChangeRecordAction dataChangeRecordAction;
  private HeartbeatRecordAction heartbeatRecordAction;
  private ChildPartitionsRecordAction childPartitionsRecordAction;
  private QueryChangeStreamAction queryChangeStreamAction;

  @Before
  public void setUp() {
    final DaoFactory daoFactory = mock(DaoFactory.class);
    final MapperFactory mapperFactory = mock(MapperFactory.class);
    final ChangeStreamMetrics metrics = mock(ChangeStreamMetrics.class);
    final BytesThroughputEstimator<DataChangeRecord> throughputEstimator =
        mock(BytesThroughputEstimator.class);
    final ActionFactory actionFactory = mock(ActionFactory.class);
    final PartitionMetadataDao partitionMetadataDao = mock(PartitionMetadataDao.class);
    final ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
    final ChangeStreamRecordMapper changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);
    final PartitionMetadataMapper partitionMetadataMapper = mock(PartitionMetadataMapper.class);
    dataChangeRecordAction = mock(DataChangeRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);
    queryChangeStreamAction = mock(QueryChangeStreamAction.class);

    doFn = new ReadChangeStreamPartitionDoFn(daoFactory, mapperFactory, actionFactory, metrics);
    doFn.setThroughputEstimator(throughputEstimator);

    partition =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet("parentToken"))
            .setStartTimestamp(PARTITION_START_TIMESTAMP)
            .setEndTimestamp(PARTITION_END_TIMESTAMP)
            .setHeartbeatMillis(PARTITION_HEARTBEAT_MILLIS)
            .setState(SCHEDULED)
            .setWatermark(PARTITION_START_TIMESTAMP)
            .setScheduledAt(Timestamp.now())
            .build();
    restriction = mock(TimestampRange.class);
    tracker = mock(RestrictionTracker.class);
    receiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
    bundleFinalizer = mock(BundleFinalizer.class);

    when(tracker.currentRestriction()).thenReturn(restriction);
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(daoFactory.getChangeStreamDao()).thenReturn(changeStreamDao);
    when(mapperFactory.changeStreamRecordMapper()).thenReturn(changeStreamRecordMapper);
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);

    when(actionFactory.dataChangeRecordAction(throughputEstimator))
        .thenReturn(dataChangeRecordAction);
    when(actionFactory.heartbeatRecordAction(metrics)).thenReturn(heartbeatRecordAction);
    when(actionFactory.childPartitionsRecordAction(partitionMetadataDao, metrics))
        .thenReturn(childPartitionsRecordAction);
    when(actionFactory.queryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            partitionMetadataMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction,
            metrics))
        .thenReturn(queryChangeStreamAction);

    doFn.setup();
  }

  @Test
  public void testQueryChangeStreamMode() {
    when(queryChangeStreamAction.run(any(), any(), any(), any(), any()))
        .thenReturn(ProcessContinuation.stop());

    final ProcessContinuation result =
        doFn.processElement(partition, tracker, receiver, watermarkEstimator, bundleFinalizer);

    assertEquals(ProcessContinuation.stop(), result);
    verify(queryChangeStreamAction)
        .run(partition, tracker, receiver, watermarkEstimator, bundleFinalizer);

    verify(dataChangeRecordAction, never()).run(any(), any(), any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any(), any());
    verify(tracker, never()).tryClaim(any());
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
