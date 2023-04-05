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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class DetectNewPartitionsActionTest {

  private PartitionMetadataDao dao;
  private PartitionMetadataMapper mapper;
  private ChangeStreamMetrics metrics;
  private Duration resumeDuration;
  private RestrictionTracker<TimestampRange, Timestamp> tracker;
  private TimestampRange restriction;
  private OutputReceiver<PartitionMetadata> receiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private DetectNewPartitionsAction action;

  @Before
  public void setUp() throws Exception {
    dao = mock(PartitionMetadataDao.class);
    mapper = mock(PartitionMetadataMapper.class);
    metrics = mock(ChangeStreamMetrics.class);
    resumeDuration = Duration.standardSeconds(1);
    tracker = mock(RestrictionTracker.class);
    restriction = mock(TimestampRange.class);
    receiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    action = new DetectNewPartitionsAction(dao, mapper, metrics, resumeDuration);

    when(tracker.currentRestriction()).thenReturn(restriction);
  }

  @Test
  public void testSchedulesPartitionsAndResumesWhenPartitionsWereCreated() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    final Timestamp partitionCreatedAt = Timestamp.ofTimeMicroseconds(15L);
    final Timestamp partitionScheduledAt = Timestamp.ofTimeMicroseconds(30L);
    final PartitionMetadata partition1 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    final PartitionMetadata partition2 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    when(partition1.getPartitionToken()).thenReturn("token1");
    when(partition1.getCreatedAt()).thenReturn(partitionCreatedAt);
    when(partition2.getPartitionToken()).thenReturn("token2");
    when(partition2.getCreatedAt()).thenReturn(partitionCreatedAt);
    when(restriction.getFrom()).thenReturn(from);
    when(dao.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(dao.updateToScheduled(Arrays.asList("token1", "token2"))).thenReturn(partitionScheduledAt);
    when(resultSet.next()).thenReturn(true, true, false);
    when(mapper.from(any())).thenReturn(partition1, partition2);
    when(tracker.tryClaim(partitionCreatedAt)).thenReturn(true);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.resume().withResumeDelay(resumeDuration), continuation);
    verify(watermarkEstimator).setWatermark(minWatermarkInstant);
    verify(receiver, times(2)).outputWithTimestamp(any(), eq(minWatermarkInstant));
  }

  @Test
  public void testDoesNothingWhenNoPartitionsWereCreated() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    when(restriction.getFrom()).thenReturn(from);
    when(dao.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.resume().withResumeDelay(resumeDuration), continuation);
    verify(watermarkEstimator).setWatermark(minWatermarkInstant);
    verify(receiver, never()).outputWithTimestamp(any(), any());
  }

  @Test
  public void testTerminatesWhenAllPartitionsAreFinished() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    when(restriction.getFrom()).thenReturn(from);
    when(dao.getUnfinishedMinWatermark()).thenReturn(null);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(receiver, never()).outputWithTimestamp(any(), any());
  }
}
