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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.cache.WatermarkCache;
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
import org.mockito.InOrder;

public class DetectNewPartitionsActionTest {

  private PartitionMetadataDao dao;
  private PartitionMetadataMapper mapper;
  private WatermarkCache cache;
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
    cache = mock(WatermarkCache.class);
    metrics = mock(ChangeStreamMetrics.class);
    resumeDuration = Duration.standardSeconds(1);
    tracker = mock(RestrictionTracker.class);
    restriction = mock(TimestampRange.class);
    receiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    action = new DetectNewPartitionsAction(dao, mapper, cache, metrics, resumeDuration);

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
    when(cache.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(dao.updateToScheduled(Arrays.asList("token1", "token2"))).thenReturn(partitionScheduledAt);
    when(resultSet.next()).thenReturn(true, true, false);
    when(mapper.from(any())).thenReturn(partition1, partition2);
    when(tracker.tryClaim(partitionCreatedAt)).thenReturn(true);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.resume().withResumeDelay(resumeDuration), continuation);
    verify(watermarkEstimator).setWatermark(minWatermarkInstant);
    verify(receiver, times(2)).outputWithTimestamp(any(), eq(minWatermarkInstant));
    InOrder order = inOrder(tracker, dao, receiver);
    order.verify(tracker).tryClaim(partitionCreatedAt);
    order.verify(dao).updateToScheduled(Arrays.asList("token1", "token2"));
    order.verify(receiver, times(2)).outputWithTimestamp(any(), eq(minWatermarkInstant));
  }

  @Test
  public void testStopsWithoutSchedulingWhenFirstCreatedAtGroupCannotBeClaimed() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    final Timestamp partitionCreatedAt = Timestamp.ofTimeMicroseconds(15L);
    final PartitionMetadata partition1 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    when(partition1.getPartitionToken()).thenReturn("token1");
    when(partition1.getCreatedAt()).thenReturn(partitionCreatedAt);
    when(restriction.getFrom()).thenReturn(from);
    when(cache.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(mapper.from(any())).thenReturn(partition1);
    when(tracker.tryClaim(partitionCreatedAt)).thenReturn(false);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(watermarkEstimator).setWatermark(minWatermarkInstant);
    verify(dao, never()).updateToScheduled(any());
    verify(receiver, never()).outputWithTimestamp(any(), any());
  }

  @Test
  public void testSchedulesOnlyClaimedCreatedAtGroupsWhenSplitPreventsClaimingLaterGroup() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    final Timestamp createdAt1 = Timestamp.ofTimeMicroseconds(15L);
    final Timestamp createdAt2 = Timestamp.ofTimeMicroseconds(25L);
    final Timestamp partitionScheduledAt = Timestamp.ofTimeMicroseconds(30L);
    final PartitionMetadata partition1 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    final PartitionMetadata partition2 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    when(partition1.getPartitionToken()).thenReturn("token1");
    when(partition1.getCreatedAt()).thenReturn(createdAt1);
    when(partition2.getPartitionToken()).thenReturn("token2");
    when(partition2.getCreatedAt()).thenReturn(createdAt2);
    when(restriction.getFrom()).thenReturn(from);
    when(cache.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(dao.updateToScheduled(Collections.singletonList("token1")))
        .thenReturn(partitionScheduledAt);
    when(resultSet.next()).thenReturn(true, true, false);
    when(mapper.from(any())).thenReturn(partition1, partition2);
    when(tracker.tryClaim(createdAt1)).thenReturn(true);
    when(tracker.tryClaim(createdAt2)).thenReturn(false);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(watermarkEstimator).setWatermark(minWatermarkInstant);
    InOrder order = inOrder(tracker, dao, receiver);
    order.verify(tracker).tryClaim(createdAt1);
    order.verify(tracker).tryClaim(createdAt2);
    order.verify(dao).updateToScheduled(Collections.singletonList("token1"));
    order.verify(receiver).outputWithTimestamp(eq(partition1), eq(minWatermarkInstant));
    verify(dao, times(1)).updateToScheduled(any());
    verify(receiver, never()).outputWithTimestamp(eq(partition2), any());
  }

  @Test
  public void testFlushesFullBatchAndPartialClaimedBatchBeforeStoppingOnUnclaimedGroup() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    final Timestamp createdAt1 = Timestamp.ofTimeMicroseconds(15L);
    final Timestamp createdAt2 = Timestamp.ofTimeMicroseconds(25L);
    final Timestamp createdAt3 = Timestamp.ofTimeMicroseconds(35L);
    final Timestamp scheduledAt1 = Timestamp.ofTimeMicroseconds(40L);
    final Timestamp scheduledAt2 = Timestamp.ofTimeMicroseconds(45L);

    final List<PartitionMetadata> allPartitions = new ArrayList<>();
    final List<String> firstBatchTokens = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      final String token = "token1_" + i;
      firstBatchTokens.add(token);
      final PartitionMetadata p = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
      when(p.getPartitionToken()).thenReturn(token);
      when(p.getCreatedAt()).thenReturn(createdAt1);
      allPartitions.add(p);
    }
    final PartitionMetadata partition2 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    when(partition2.getPartitionToken()).thenReturn("token2");
    when(partition2.getCreatedAt()).thenReturn(createdAt2);
    allPartitions.add(partition2);

    final PartitionMetadata partition3 = mock(PartitionMetadata.class, RETURNS_DEEP_STUBS);
    when(partition3.getPartitionToken()).thenReturn("token3");
    when(partition3.getCreatedAt()).thenReturn(createdAt3);
    allPartitions.add(partition3);

    final Boolean[] nextReturns = new Boolean[allPartitions.size()];
    Arrays.fill(nextReturns, true);
    nextReturns[allPartitions.size() - 1] = false;
    when(resultSet.next()).thenReturn(true, nextReturns);
    when(mapper.from(any()))
        .thenReturn(
            allPartitions.get(0),
            allPartitions.subList(1, allPartitions.size()).toArray(new PartitionMetadata[0]));

    when(restriction.getFrom()).thenReturn(from);
    when(cache.getUnfinishedMinWatermark()).thenReturn(minWatermark);
    when(dao.getAllPartitionsCreatedAfter(from)).thenReturn(resultSet);
    when(dao.updateToScheduled(firstBatchTokens)).thenReturn(scheduledAt1);
    when(dao.updateToScheduled(Collections.singletonList("token2"))).thenReturn(scheduledAt2);
    when(tracker.tryClaim(createdAt1)).thenReturn(true);
    when(tracker.tryClaim(createdAt2)).thenReturn(true);
    when(tracker.tryClaim(createdAt3)).thenReturn(false);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), continuation);
    InOrder order = inOrder(tracker, dao, receiver);
    order.verify(tracker).tryClaim(createdAt1);
    order.verify(dao).updateToScheduled(firstBatchTokens);
    order.verify(receiver, times(200)).outputWithTimestamp(any(), eq(minWatermarkInstant));
    order.verify(tracker).tryClaim(createdAt2);
    order.verify(tracker).tryClaim(createdAt3);
    order.verify(dao).updateToScheduled(Collections.singletonList("token2"));
    order.verify(receiver).outputWithTimestamp(eq(partition2), eq(minWatermarkInstant));
    verify(dao, times(2)).updateToScheduled(any());
    verify(receiver, never()).outputWithTimestamp(eq(partition3), any());
  }

  @Test
  public void testDoesNothingWhenNoPartitionsWereCreated() {
    final Timestamp from = Timestamp.ofTimeMicroseconds(10L);
    final Timestamp minWatermark = Timestamp.ofTimeMicroseconds(20L);
    final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
    final ResultSet resultSet = mock(ResultSet.class);
    when(restriction.getFrom()).thenReturn(from);
    when(cache.getUnfinishedMinWatermark()).thenReturn(minWatermark);
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
    when(cache.getUnfinishedMinWatermark()).thenReturn(null);

    final ProcessContinuation continuation = action.run(tracker, receiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), continuation);
    verify(watermarkEstimator, never()).setWatermark(any());
    verify(receiver, never()).outputWithTimestamp(any(), any());
  }
}
