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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DetectNewPartitionsActionTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  @Mock private ChangeStreamMetrics metrics;
  @Mock private GenerateInitialPartitionsAction generateInitialPartitionsAction;

  private DetectNewPartitionsAction action;

  @Mock private RestrictionTracker<OffsetRange, Long> tracker;
  @Mock private OutputReceiver<PartitionRecord> receiver;
  @Mock private BundleFinalizer bundleFinalizer;

  private MetadataTableDao metadataTableDao;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private Instant startTime;
  private Instant partitionTime;
  private Instant endTime;
  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;

  @Captor ArgumentCaptor<PartitionRecord> partitionRecordArgumentCaptor;

  @BeforeClass
  public static void beforeClass() throws IOException {
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(BIGTABLE_EMULATOR_RULE.getPort())
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
    BigtableDataSettings dataSettingsBuilder =
        BigtableDataSettings.newBuilderForEmulator(BIGTABLE_EMULATOR_RULE.getPort())
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .build();
    dataClient = BigtableDataClient.create(dataSettingsBuilder);
  }

  @Before
  public void setUp() throws Exception {
    String changeStreamId = UniqueIdGenerator.generateRowKeyPrefix();
    MetadataTableAdminDao metadataTableAdminDao =
        new MetadataTableAdminDao(
            adminClient, null, changeStreamId, MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME);
    metadataTableAdminDao.createMetadataTable();
    metadataTableDao =
        new MetadataTableDao(
            dataClient,
            metadataTableAdminDao.getTableId(),
            metadataTableAdminDao.getChangeStreamNamePrefix());

    startTime = Instant.now();
    endTime = startTime.plus(Duration.standardSeconds(10));
    partitionTime = startTime.plus(Duration.standardSeconds(10));
    action =
        new DetectNewPartitionsAction(
            metrics, metadataTableDao, endTime, generateInitialPartitionsAction);
    watermarkEstimator = new WatermarkEstimators.Manual(startTime);
  }

  @Test
  public void testInitialPartitions() throws Exception {
    OffsetRange offsetRange = new OffsetRange(0, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    assertEquals(startTime, watermarkEstimator.currentWatermark());

    when(generateInitialPartitionsAction.run(receiver, tracker, watermarkEstimator, startTime))
        .thenReturn(ProcessContinuation.resume());

    assertEquals(
        ProcessContinuation.resume(),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
  }

  // Every 10 tryClaim, DNP updates the watermark based on the watermark of all the RCSP.
  @Test
  public void testAdvanceWatermarkWithAllPartitions() throws Exception {
    // We advance watermark on every 10 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(10, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);

    assertEquals(startTime, watermarkEstimator.currentWatermark());

    // Write 2 partitions to the table that covers entire keyspace.
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    Instant watermark1 = partitionTime.plus(Duration.millis(100));
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // TryClaim should be called with the end value because the watermark should be updated to
    // beyond endTime causing the pipeline to terminate.
    verify(tracker, times(1)).tryClaim(offsetRange.getTo());

    // Because the 2 partitions cover the entire keyspace, the watermark should have advanced.
    // Also note the watermark is watermark2 which is the lowest of the 2 watermarks.
    assertEquals(watermark2, watermarkEstimator.currentWatermark());
  }

  // Every 10 tryClaim, DNP only updates its watermark if all the RCSP currently streamed covers the
  // entire key space. If there's any missing, they are in the process of split or merge. If the
  // watermark is updated with missing partitions, the watermark might be further ahead than it
  // actually is.
  @Test
  public void testAdvanceWatermarkWithMissingPartitions() throws Exception {
    // We advance watermark on every 10 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(10, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    // We return false here, so we do need to test any other functionality other than updating the
    // watermark.
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(false);

    assertEquals(startTime, watermarkEstimator.currentWatermark());

    // Write 2 partitions to the table that DO NOT cover the entire keyspace.
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    Instant watermark1 = partitionTime.plus(Duration.millis(100));
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // Ensure that we did indeed try to claim the start of offset range because that means watermark
    // was not updated beyond endTime.
    verify(tracker, times(1)).tryClaim(offsetRange.getFrom());

    // Because the 2 partitions DO NOT cover the entire keyspace, watermark stays at startTime.
    assertEquals(startTime, watermarkEstimator.currentWatermark());
  }

  @Test
  public void testProcessSplitNewPartitions() throws Exception {
    // Avoid 0 and multiples of 10 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["", "") splits into ["", "a"), ["a","c"), ["c", "")
    ByteStringRange parentPartition = ByteStringRange.create("", "");
    Instant watermark = Instant.now();
    ByteStringRange child1 = ByteStringRange.create("", "a");
    metadataTableDao.writeNewPartition(
        child1,
        ChangeStreamContinuationToken.create(
            child1, ByteStringRange.serializeToByteString(child1).toStringUtf8()),
        parentPartition,
        watermark);
    ByteStringRange child2 = ByteStringRange.create("k", "");
    metadataTableDao.writeNewPartition(
        child2,
        ChangeStreamContinuationToken.create(
            child2, ByteStringRange.serializeToByteString(child2).toStringUtf8()),
        parentPartition,
        watermark);
    ByteStringRange child3 = ByteStringRange.create("a", "k");
    metadataTableDao.writeNewPartition(
        child3,
        ChangeStreamContinuationToken.create(
            child3, ByteStringRange.serializeToByteString(child3).toStringUtf8()),
        parentPartition,
        watermark);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));

    // Check what parameters were passed to OutputReceiver to verify the new partitions created by
    // DNP are correct.
    verify(receiver, times(3))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<ByteStringRange> expectedPartitions =
        partitionRecordArgumentCaptor.getAllValues().stream()
            .map(PartitionRecord::getPartition)
            .collect(Collectors.toList());
    List<ByteStringRange> actualPartitions = Arrays.asList(child1, child2, child3);
    assertThat(expectedPartitions, Matchers.containsInAnyOrder(actualPartitions.toArray()));
    for (PartitionRecord value : partitionRecordArgumentCaptor.getAllValues()) {
      assertEquals(value.getParentLowWatermark(), watermark);
      assertNotNull(value.getEndTime());
      assertEquals(value.getEndTime(), endTime);
      assertNotNull(value.getChangeStreamContinuationTokens());
      assertEquals(1, value.getChangeStreamContinuationTokens().size());
      // Verify token is correct. For the test, the continuation token string is the string
      // representation of the partition itself.
      assertEquals(
          value.getChangeStreamContinuationTokens().get(0).getToken(),
          ByteStringRange.serializeToByteString(
                  value.getChangeStreamContinuationTokens().get(0).getPartition())
              .toStringUtf8());
    }
  }

  @Test
  public void testProcessMergeNewPartitions() throws Exception {
    // Avoid 0 and multiples of 10 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["a, "b") and ["b", "c") merge into ["a", "c")
    ByteStringRange parentPartition1 = ByteStringRange.create("a", "b");
    Instant watermark1 = startTime;
    ByteStringRange parentPartition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = startTime.plus(Duration.millis(10));

    ByteStringRange childPartition = ByteStringRange.create("a", "c");
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(childPartition, "token1");
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(childPartition, "token2");
    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(childPartition, token1, parentPartition1, watermark1);
    metadataTableDao.writeNewPartition(childPartition, token2, parentPartition2, watermark2);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartition, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermark1, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertThat(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Matchers.containsInAnyOrder(token1, token2));
  }

  // Test merging partition with a parent partition that hasn't stopped yet.
  @Test
  public void testProcessMergeNewPartitionsMissingParent() throws Exception {
    // Avoid 0 and multiples of 10 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["a, "b") and ["b", "c") merge into ["a", "c") but ["b", "c") is still processing.
    ByteStringRange parentPartition1 = ByteStringRange.create("a", "b");
    Instant watermark1 = startTime;

    ByteStringRange childPartition = ByteStringRange.create("a", "c");
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(childPartition, "token1");
    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(childPartition, token1, parentPartition1, watermark1);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, never()).outputWithTimestamp(any(), any());

    // On next iteration, ["b", "c") received CloseStream and writes to the metadata table.
    ByteStringRange parentPartition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = startTime.plus(Duration.millis(10));
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(childPartition, "token2");
    metadataTableDao.writeNewPartition(childPartition, token2, parentPartition2, watermark2);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartition, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermark1, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertThat(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Matchers.containsInAnyOrder(token1, token2));
  }

  @Test
  public void testMissingPartitionReconciled() throws Exception {
    // We advance watermark on every 10 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(10, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // Write 2 partitions to the table, missing [a, b)
    ByteStringRange partition1 = ByteStringRange.create("", "a");
    Instant watermark1 = partitionTime.plus(Duration.millis(100));
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    HashMap<ByteStringRange, Long> missingPartitionDurations = new HashMap<>();
    ByteStringRange partitionAB = ByteStringRange.create("a", "b");
    // Partition missing for 5 minutes less 1 seconds.
    missingPartitionDurations.put(partitionAB, Instant.now().getMillis() - (5 * 60 - 1) * 1000L);
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    // No new partitions and missing partition has not been missing for long enough.
    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    verify(receiver, never()).outputWithTimestamp(any(), any());

    // Sleep for 1 second, enough that the missing partition needs to be reconciled.
    Thread.sleep(1000);

    // We advance the restriction tracker by 1. Because it is not a multiple of 10, we don't
    // evaluate missing partitions, which means we don't perform reconciliation.
    offsetRange = new OffsetRange(11, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    verify(receiver, never()).outputWithTimestamp(any(), any());

    // Multiple of 10, reconciliation should happen.
    offsetRange = new OffsetRange(20, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    assertEquals(partitionAB, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(
        watermarkEstimator.currentWatermark(),
        partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertNotNull(partitionRecordArgumentCaptor.getValue().getStartTime());
    assertEquals(
        watermarkEstimator.currentWatermark(),
        partitionRecordArgumentCaptor.getValue().getStartTime());
  }
}
