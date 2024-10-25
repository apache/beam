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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.toThreetenInstant;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ChangeStreamAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ReadChangeStreamPartitionAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.CoderSizeEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class ReadChangeStreamPartitionDoFnTest {
  private ChangeStreamDao changeStreamDao;
  private MetadataTableDao metadataTableDao;
  private CoderSizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator;
  private DaoFactory daoFactory;
  private ActionFactory actionFactory;
  private ChangeStreamMetrics metrics;
  private ReadChangeStreamPartitionDoFn doFn;

  @Before
  public void setup() throws IOException {
    Duration heartbeatDuration = Duration.standardSeconds(1);
    daoFactory = mock(DaoFactory.class);
    changeStreamDao = mock(ChangeStreamDao.class);
    metadataTableDao = mock(MetadataTableDao.class);
    when(daoFactory.getChangeStreamDao()).thenReturn(changeStreamDao);
    when(daoFactory.getMetadataTableDao()).thenReturn(metadataTableDao);
    when(daoFactory.getChangeStreamName()).thenReturn("test-id");

    actionFactory = mock(ActionFactory.class);
    metrics = mock(ChangeStreamMetrics.class);

    sizeEstimator = mock(CoderSizeEstimator.class);
    ChangeStreamAction changeStreamAction = new ChangeStreamAction(metrics);
    ReadChangeStreamPartitionAction readChangeStreamPartitionAction =
        new ReadChangeStreamPartitionAction(
            metadataTableDao,
            changeStreamDao,
            metrics,
            changeStreamAction,
            heartbeatDuration,
            sizeEstimator);
    when(actionFactory.changeStreamAction(metrics)).thenReturn(changeStreamAction);
    when(actionFactory.readChangeStreamPartitionAction(
            metadataTableDao,
            changeStreamDao,
            metrics,
            changeStreamAction,
            heartbeatDuration,
            sizeEstimator))
        .thenReturn(readChangeStreamPartitionAction);

    doFn = new ReadChangeStreamPartitionDoFn(daoFactory, actionFactory, metrics, Duration.ZERO);
    doFn.setSizeEstimator(sizeEstimator);
  }

  @Test
  public void testProcessElementAndGetSize() throws IOException, InterruptedException {
    long watermarkLag = 10;
    Instant tenSecondsAgo = Instant.now().minus(Duration.standardSeconds(watermarkLag));
    Range.ByteStringRange partitionRange = Range.ByteStringRange.create("", "");
    ChangeStreamContinuationToken testToken =
        ChangeStreamContinuationToken.create(partitionRange, "test");
    PartitionRecord partition =
        new PartitionRecord(
            partitionRange,
            tenSecondsAgo,
            "uid-a",
            tenSecondsAgo,
            Collections.emptyList(),
            Instant.now().plus(Duration.standardSeconds(60)));
    long mutationSize = 100L;
    when(sizeEstimator.sizeOf(any())).thenReturn(mutationSize);
    ReadChangeStreamPartitionProgressTracker restrictionTracker =
        mock(ReadChangeStreamPartitionProgressTracker.class);
    when(restrictionTracker.currentRestriction()).thenReturn(new StreamProgress());
    DoFn.OutputReceiver<KV<ByteString, ChangeStreamRecord>> receiver =
        mock(DoFn.OutputReceiver.class);
    ManualWatermarkEstimator<Instant> watermarkEstimator = mock(ManualWatermarkEstimator.class);
    doFn.setup();

    ByteString rowKey = ByteString.copyFromUtf8("a");
    ChangeStreamMutation mockMutation = mock(ChangeStreamMutation.class);
    when(mockMutation.getRowKey()).thenReturn(rowKey);
    when(mockMutation.getEstimatedLowWatermark()).thenReturn(toThreetenInstant(tenSecondsAgo));
    when(mockMutation.getToken()).thenReturn(testToken.getToken());
    when(mockMutation.getCommitTimestamp()).thenReturn(toThreetenInstant(tenSecondsAgo));

    when(metadataTableDao.lockAndRecordPartition(any())).thenReturn(true);
    ServerStream<ChangeStreamRecord> mockStream = mock(ServerStream.class);
    Iterator<ChangeStreamRecord> mockResponses = mock(Iterator.class);
    when(mockResponses.hasNext()).thenReturn(true, true, true);
    when(mockResponses.next()).thenReturn(mockMutation, mockMutation, mockMutation);
    when(mockStream.iterator()).thenReturn(mockResponses);
    when(changeStreamDao.readChangeStreamPartition(any(), any(), any(), any()))
        .thenReturn(mockStream);

    when(watermarkEstimator.getState()).thenReturn(tenSecondsAgo);
    // Checkpoint after receiving 2 mutations
    when(restrictionTracker.tryClaim(any())).thenReturn(true, true, false);

    doFn.processElement(partition, restrictionTracker, receiver, watermarkEstimator);
    double sizeEstimate =
        doFn.getSize(
            new StreamProgress(
                testToken, tenSecondsAgo, BigDecimal.valueOf(20), Instant.now(), false));
    // we should have output 2 100B mutations in the past 10s
    long bytesPerSecond = (mutationSize * 2) / 10;
    assertEquals(sizeEstimate, bytesPerSecond * watermarkLag, 10);
    verify(receiver, times(2)).outputWithTimestamp(KV.of(rowKey, mockMutation), Instant.EPOCH);
  }

  @Test
  public void testGetSizeCantBeNegative() throws IOException {
    long mutationSize = 100L;
    when(sizeEstimator.sizeOf(any())).thenReturn(mutationSize);
    Range.ByteStringRange partitionRange = Range.ByteStringRange.create("", "");
    ChangeStreamContinuationToken testToken =
        ChangeStreamContinuationToken.create(partitionRange, "test");
    doFn.setup();

    double mutationEstimate =
        doFn.getSize(
            new StreamProgress(
                testToken,
                Instant.now().plus(Duration.standardMinutes(10)),
                BigDecimal.valueOf(1000),
                Instant.now().plus(Duration.standardMinutes(10)),
                false));
    assertEquals(0, mutationEstimate, 0);

    double heartbeatEstimate =
        doFn.getSize(
            new StreamProgress(
                testToken,
                Instant.now().plus(Duration.standardMinutes(10)),
                BigDecimal.valueOf(1000),
                Instant.now().plus(Duration.standardMinutes(10)),
                true));
    assertEquals(0, heartbeatEstimate, 0);
  }

  @Test
  public void backlogReplicationAdjustment() throws IOException {
    SerializableSupplier<Instant> mockClock = () -> Instant.ofEpochSecond(1000);
    doFn =
        new ReadChangeStreamPartitionDoFn(
            daoFactory, actionFactory, metrics, Duration.standardSeconds(30), mockClock);
    long mutationSize = 100L;
    when(sizeEstimator.sizeOf(any())).thenReturn(mutationSize);
    doFn.setSizeEstimator(sizeEstimator);

    Range.ByteStringRange partitionRange = Range.ByteStringRange.create("", "");
    ChangeStreamContinuationToken testToken =
        ChangeStreamContinuationToken.create(partitionRange, "test");
    doFn.setup();

    double mutationEstimate10Second =
        doFn.getSize(
            new StreamProgress(
                testToken,
                mockClock.get().minus(Duration.standardSeconds(10)),
                BigDecimal.valueOf(1000),
                mockClock.get().minus(Duration.standardSeconds(10)),
                false));
    // With 30s backlogReplicationAdjustment we should have no backlog when watermarkLag is < 30s
    assertEquals(0, mutationEstimate10Second, 0);

    double mutationEstimateOneMinute =
        doFn.getSize(
            new StreamProgress(
                testToken,
                mockClock.get().minus(Duration.standardSeconds(60)),
                BigDecimal.valueOf(1000),
                mockClock.get().minus(Duration.standardSeconds(60)),
                false));
    // We ignore the first 30s of backlog so this should be throughput * (60 - 30)
    assertEquals(1000 * 30, mutationEstimateOneMinute, 0);
  }
}
