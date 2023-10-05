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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
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
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.InitialPipelineState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
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
  @Mock private ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction;

  private DetectNewPartitionsAction action;

  @Mock private RestrictionTracker<OffsetRange, Long> tracker;
  @Mock private DoFn.OutputReceiver<PartitionRecord> receiver;

  private MetadataTableDao metadataTableDao;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private Instant startTime;
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
    metadataTableAdminDao.cleanUpPrefix();
    metadataTableDao =
        new MetadataTableDao(
            dataClient,
            metadataTableAdminDao.getTableId(),
            metadataTableAdminDao.getChangeStreamNamePrefix());

    startTime = Instant.now();
    endTime = startTime.plus(Duration.standardSeconds(10));
    ProcessNewPartitionsAction processNewPartitionsAction =
        new ProcessNewPartitionsAction(metrics, metadataTableDao, endTime);
    action =
        new DetectNewPartitionsAction(
            metrics,
            metadataTableDao,
            endTime,
            generateInitialPartitionsAction,
            resumeFromPreviousPipelineAction,
            processNewPartitionsAction);
    watermarkEstimator = new WatermarkEstimators.Manual(startTime);
  }

  @Test
  public void testUpdateWatermarkAfterCheckpoint() throws Exception {
    Instant watermark = endTime;
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // Watermark estimator is not updated before a checkpoint.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Update the watermark in the metadata table. This run terminates because watermark == endTime.
    metadataTableDao.updateDetectNewPartitionWatermark(watermark);
    // Watermark estimator will be updated with the watermark from the metadata table.
    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(watermark, watermarkEstimator.currentWatermark());
    assertEquals(watermark, metadataTableDao.readDetectNewPartitionsState().getWatermark());
  }

  @Test
  public void testUpdateWatermarkOnEvenCountAfter10Seconds() throws Exception {
    // We update watermark every 2 iterations only if it's been more than 10s since the last update.
    OffsetRange offsetRange = new OffsetRange(2, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Manually set the watermark of DNP to start time with a timestamp of 10s prior.
    RowMutation rowMutation =
        RowMutation.create(
                MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME,
                metadataTableDao
                    .getChangeStreamNamePrefix()
                    .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                Instant.now().minus(Duration.standardSeconds(10)).getMillis() * 1000L,
                startTime.getMillis());
    dataClient.mutateRow(rowMutation);

    // Create a partition covering the entire keyspace with watermark after endTime.
    ByteStringRange partition1 = ByteStringRange.create("", "");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecord1 =
        new PartitionRecord(
            partition1,
            watermark1,
            UniqueIdGenerator.getNextId(),
            watermark1,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertEquals(watermark1, metadataTableDao.readDetectNewPartitionsState().getWatermark());

    // On the 2nd run, watermark estimator is updated which is beyond endTime and terminates.
    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getTo());
    assertEquals(watermark1, watermarkEstimator.currentWatermark());
  }

  @Test
  public void testDoNotUpdateWatermarkLessThan10s() throws Exception {
    // We update watermark every 2 iterations only if it's been more than 10s since the last update.
    OffsetRange offsetRange = new OffsetRange(2, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Manually set the watermark of DNP to start time with a timestamp of 1s prior.
    RowMutation rowMutation =
        RowMutation.create(
                MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME,
                metadataTableDao
                    .getChangeStreamNamePrefix()
                    .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                Instant.now().minus(Duration.standardSeconds(1)).getMillis() * 1000L,
                startTime.getMillis());
    dataClient.mutateRow(rowMutation);

    // Create a partition covering the entire keyspace with watermark after endTime.
    ByteStringRange partition1 = ByteStringRange.create("", "");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecord1 =
        new PartitionRecord(
            partition1,
            watermark1,
            UniqueIdGenerator.getNextId(),
            watermark1,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);

    // Watermark doesn't get updated because the last time watermark was updated was less than 10s.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertEquals(startTime, metadataTableDao.readDetectNewPartitionsState().getWatermark());

    // On the 2nd run, watermark estimator is still the same since watermark isn't updated.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
  }

  @Test
  public void testDoNotUpdateWatermarkOnOddCount() throws Exception {
    // We only update watermark on even count.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Manually set the watermark of DNP to start time with a timestamp of 10s prior.
    RowMutation rowMutation =
        RowMutation.create(
                MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME,
                metadataTableDao
                    .getChangeStreamNamePrefix()
                    .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                Instant.now().minus(Duration.standardSeconds(10)).getMillis() * 1000L,
                startTime.getMillis());
    dataClient.mutateRow(rowMutation);

    // Create a partition covering the entire keyspace with watermark after endTime.
    ByteStringRange partition1 = ByteStringRange.create("", "");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecord1 =
        new PartitionRecord(
            partition1,
            watermark1,
            UniqueIdGenerator.getNextId(),
            watermark1,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);

    // It's been more than 10s since last update but the count is odd.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertEquals(startTime, metadataTableDao.readDetectNewPartitionsState().getWatermark());

    // On the 2nd run, watermark estimator is still the same since watermark isn't updated.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(startTime, watermarkEstimator.currentWatermark());
  }

  // Every 2 tryClaim, DNP updates the watermark based on the watermark of all the RCSP.
  @Test
  public void testAdvanceWatermarkWithAllPartitions() throws Exception {
    // We advance watermark on every 2 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(10, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);
    when(tracker.tryClaim(offsetRange.getTo())).thenReturn(true);

    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Write 2 partitions to the table that covers entire keyspace.
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecord1 =
        new PartitionRecord(
            partition1,
            watermark1,
            UniqueIdGenerator.getNextId(),
            watermark1,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);
    ByteStringRange partition2 = ByteStringRange.create("b", "");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    PartitionRecord partitionRecord2 =
        new PartitionRecord(
            partition2,
            watermark2,
            UniqueIdGenerator.getNextId(),
            watermark2,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord2);

    // Updating watermark does not affect the watermark estimator of this run.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getFrom());

    // Because the 2 partitions cover the entire keyspace, the watermark should have advanced.
    // Also note the watermark is watermark2 which is the lowest of the 2 watermarks. Watermark
    // estimator isn't updated because we update the watermark estimator on the start of the run. We
    // do update the metadata table with the new watermark value.
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertEquals(watermark2, metadataTableDao.readDetectNewPartitionsState().getWatermark());

    // On the 2nd run, watermark estimator is updated which is beyond endTime and terminates.
    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getTo());
    assertEquals(watermark2, watermarkEstimator.currentWatermark());
  }

  // Every 2 tryClaim, DNP only updates its watermark if all the RCSP currently streamed covers the
  // entire key space. If there's any missing, they are in the process of split or merge. If the
  // watermark is updated with missing partitions, the watermark might be further ahead than it
  // actually is.
  @Test
  public void testAdvanceWatermarkWithMissingPartitions() throws Exception {
    // We advance watermark on every 2 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(2, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Write 2 partitions to the table that DO NOT cover the entire keyspace.
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getFrom());

    // Because the 2 partitions DO NOT cover the entire keyspace, watermark stays at startTime.
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());
  }

  @Test
  public void testAdvanceWatermarkWithNewPartitions() throws Exception {
    // We advance watermark on every 2 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(2, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);
    when(tracker.tryClaim(offsetRange.getTo())).thenReturn(true);

    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertNull(metadataTableDao.readDetectNewPartitionsState());

    // Write 2 partitions to the table that DO NOT cover the entire keyspace.
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    Instant watermark1 = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecord1 =
        new PartitionRecord(
            partition1,
            watermark1,
            UniqueIdGenerator.getNextId(),
            watermark1,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);
    ByteStringRange partition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = endTime.plus(Duration.millis(1));
    PartitionRecord partitionRecord2 =
        new PartitionRecord(
            partition2,
            watermark2,
            UniqueIdGenerator.getNextId(),
            watermark2,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord2);

    ByteStringRange partition3 = ByteStringRange.create("c", "");
    ChangeStreamContinuationToken token = ChangeStreamContinuationToken.create(partition3, "token");
    NewPartition newPartition =
        new NewPartition(partition3, Collections.singletonList(token), watermark2);
    metadataTableDao.writeNewPartition(newPartition);

    // Updating watermark does not affect the result of this run.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getFrom());

    // Because the StreamPartition and NewPartition cover the entire keyspace, the watermark should
    // have advanced. Also note the watermark is watermark2 which is the lowest watermark. Watermark
    // estimator isn't updated because we update the watermark estimator on the start of the run. We
    // do update the metadata table with the new watermark value.
    assertEquals(startTime, watermarkEstimator.currentWatermark());
    assertEquals(watermark2, metadataTableDao.readDetectNewPartitionsState().getWatermark());

    // On the 2nd run, watermark estimator is updated which is beyond endTime and terminates.
    assertEquals(
        DoFn.ProcessContinuation.stop(),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(tracker, times(1)).tryClaim(offsetRange.getTo());
    assertEquals(watermark2, watermarkEstimator.currentWatermark());
  }

  @Test
  public void testProcessSplitNewPartitions() throws Exception {
    // Avoid 0 and multiples of 2 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["", "") splits into ["", "a"), ["a","k"), ["k", "")
    Instant watermark = Instant.now();
    ByteStringRange childEmptyA = ByteStringRange.create("", "a");
    ChangeStreamContinuationToken tokenEmptyA =
        ChangeStreamContinuationToken.create(
            childEmptyA, ByteStringRange.serializeToByteString(childEmptyA).toStringUtf8());
    NewPartition newPartitionEmptyA =
        new NewPartition(childEmptyA, Collections.singletonList(tokenEmptyA), watermark);
    metadataTableDao.writeNewPartition(newPartitionEmptyA);
    ByteStringRange childKEmpty = ByteStringRange.create("k", "");
    ChangeStreamContinuationToken tokenKEmpty =
        ChangeStreamContinuationToken.create(
            childKEmpty, ByteStringRange.serializeToByteString(childKEmpty).toStringUtf8());
    NewPartition newPartitionKEmpty =
        new NewPartition(childKEmpty, Collections.singletonList(tokenKEmpty), watermark);
    metadataTableDao.writeNewPartition(newPartitionKEmpty);
    ByteStringRange childAK = ByteStringRange.create("a", "k");
    ChangeStreamContinuationToken tokenAK =
        ChangeStreamContinuationToken.create(
            childAK, ByteStringRange.serializeToByteString(childAK).toStringUtf8());
    NewPartition newPartitionAK =
        new NewPartition(childAK, Collections.singletonList(tokenAK), watermark);
    metadataTableDao.writeNewPartition(newPartitionAK);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));

    // Check what parameters were passed to OutputReceiver to verify the new partitions created by
    // DNP are correct.
    verify(receiver, times(3))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<ByteStringRange> expectedPartitions =
        partitionRecordArgumentCaptor.getAllValues().stream()
            .map(PartitionRecord::getPartition)
            .collect(Collectors.toList());
    List<ByteStringRange> actualPartitions = Arrays.asList(childEmptyA, childKEmpty, childAK);
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
    assertTrue(metadataTableDao.readNewPartitions().isEmpty());
  }

  @Test
  public void testProcessMergeNewPartitions() throws Exception {
    // Avoid 0 and multiples of 2 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["a, "b") and ["b", "c") merge into ["a", "c")
    ByteStringRange childPartitionAC = ByteStringRange.create("a", "c");

    ByteStringRange parentPartitionAB = ByteStringRange.create("a", "b");
    Instant watermarkAB = startTime;
    ChangeStreamContinuationToken tokenAB =
        ChangeStreamContinuationToken.create(parentPartitionAB, "ab");
    NewPartition newPartitionACFromAB =
        new NewPartition(childPartitionAC, Collections.singletonList(tokenAB), watermarkAB);
    ByteStringRange parentPartitionBC = ByteStringRange.create("b", "c");
    Instant watermarkBC = startTime.plus(Duration.millis(10));
    ChangeStreamContinuationToken tokenBC =
        ChangeStreamContinuationToken.create(parentPartitionBC, "bc");
    NewPartition newPartitionACFromBC =
        new NewPartition(childPartitionAC, Collections.singletonList(tokenBC), watermarkBC);
    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(newPartitionACFromAB);
    metadataTableDao.writeNewPartition(newPartitionACFromBC);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartitionAC, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermarkAB, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertThat(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Matchers.containsInAnyOrder(tokenAB, tokenBC));
    assertTrue(metadataTableDao.readNewPartitions().isEmpty());
  }

  // Test merging partition with a parent partition that hasn't stopped yet.
  @Test
  public void testProcessMergeNewPartitionsMissingParent() throws Exception {
    // Avoid 0 and multiples of 2 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["a, "b") and ["b", "c") merge into ["a", "c") but ["b", "c") is still processing.
    ByteStringRange parentPartitionAB = ByteStringRange.create("a", "b");
    Instant watermarkAB = startTime;
    ChangeStreamContinuationToken tokenAB =
        ChangeStreamContinuationToken.create(parentPartitionAB, "ab");

    ByteStringRange childPartitionAC = ByteStringRange.create("a", "c");

    NewPartition newPartitionACFromAB =
        new NewPartition(childPartitionAC, Collections.singletonList(tokenAB), watermarkAB);

    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(newPartitionACFromAB);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    // No partitions are outputted because it's missing a parent still.
    verify(receiver, never()).outputWithTimestamp(any(), any());

    assertEquals(1, metadataTableDao.readNewPartitions().size());

    // On next iteration, ["b", "c") received CloseStream and writes to the metadata table.
    ByteStringRange parentPartitionBC = ByteStringRange.create("b", "c");
    Instant watermarkBC = startTime.plus(Duration.millis(10));
    ChangeStreamContinuationToken tokenBC =
        ChangeStreamContinuationToken.create(parentPartitionBC, "bc");
    NewPartition newPartitionACFromBC =
        new NewPartition(childPartitionAC, Collections.singletonList(tokenBC), watermarkBC);
    metadataTableDao.writeNewPartition(newPartitionACFromBC);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartitionAC, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermarkAB, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertThat(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Matchers.containsInAnyOrder(tokenAB, tokenBC));
    assertTrue(metadataTableDao.readNewPartitions().isEmpty());
  }

  @Test
  public void testMissingPartitionReconciled() throws Exception {
    // We only start reconciling after 50.
    // We advance watermark on every 2 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(52, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // Write 2 partitions to the table, missing [a, b)
    ByteStringRange partitionEmptyA = ByteStringRange.create("", "a");
    Instant watermarkEmptyA = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecordEmptyA =
        new PartitionRecord(
            partitionEmptyA,
            watermarkEmptyA,
            UniqueIdGenerator.getNextId(),
            watermarkEmptyA,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecordEmptyA);
    ByteStringRange partitionBEmpty = ByteStringRange.create("b", "");
    Instant watermarkBEmpty = endTime.plus(Duration.millis(1));
    PartitionRecord partitionRecordBEmpty =
        new PartitionRecord(
            partitionBEmpty,
            watermarkBEmpty,
            UniqueIdGenerator.getNextId(),
            watermarkBEmpty,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecordBEmpty);

    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    ByteStringRange partitionAB = ByteStringRange.create("a", "b");
    // Partition missing for 10 minutes less 1 second.
    missingPartitionDurations.put(
        partitionAB, Instant.now().minus(Duration.standardSeconds(20 * 60 - 1)));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    // Since there's no NewPartition corresponding to the missing partition, we can't reconcile with
    // continuation tokens. In order to reconcile without continuation tokens, the partition needs
    // to have been missing for more than 10 minutes.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(receiver, never()).outputWithTimestamp(any(), any());
    assertEquals(1, metadataTableDao.readDetectNewPartitionMissingPartitions().size());

    // Sleep for more than 1 second, enough that the missing partition needs to be reconciled.
    Thread.sleep(1001);

    // We advance the restriction tracker by 1. Because it is not a multiple of 2, we don't
    // evaluate missing partitions, which means we don't perform reconciliation.
    offsetRange = new OffsetRange(53, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    verify(receiver, never()).outputWithTimestamp(any(), any());
    assertEquals(1, metadataTableDao.readDetectNewPartitionMissingPartitions().size());

    // Multiple of 2, reconciliation should happen.
    offsetRange = new OffsetRange(54, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    assertEquals(0, metadataTableDao.readDetectNewPartitionMissingPartitions().size());
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    assertEquals(partitionAB, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(
        watermarkEstimator.currentWatermark(),
        partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertNotNull(partitionRecordArgumentCaptor.getValue().getStartTime());
    // The startTime should be the startTime of the pipeline because it's less than 1 hour before
    // the low watermark.
    assertEquals(startTime, partitionRecordArgumentCaptor.getValue().getStartTime());
  }

  // Reconcile runs immediately after a previous reconcile without RCSP working on the previous
  // reconciled result. This can happen if the runner is backed up and slow.
  // 1. Partition in NewPartition waiting for 1 minute
  // 2. Reconciler takes the partition and outputs it. Reconciler marks the partition in
  // NewPartition as deleted
  // 3. Reconciler runs again
  @Test
  public void testBackToBackReconcile() throws Exception {
    // We only start reconciling after 50.
    // We advance watermark on every 2 restriction tracker advancement
    OffsetRange offsetRange = new OffsetRange(52, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // Write 2 partitions to the table, missing [a, b) because [a, b) is trying to merge into [a, c)
    ByteStringRange partitionEmptyA = ByteStringRange.create("", "a");
    Instant watermarkEmptyA = endTime.plus(Duration.millis(100));
    PartitionRecord partitionRecordEmptyA =
        new PartitionRecord(
            partitionEmptyA,
            watermarkEmptyA,
            UniqueIdGenerator.getNextId(),
            watermarkEmptyA,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecordEmptyA);
    ByteStringRange partitionBEmpty = ByteStringRange.create("b", "");
    Instant watermarkBEmpty = endTime.plus(Duration.millis(1));
    PartitionRecord partitionRecordBEmpty =
        new PartitionRecord(
            partitionBEmpty,
            watermarkBEmpty,
            UniqueIdGenerator.getNextId(),
            watermarkBEmpty,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecordBEmpty);

    // NewPartition [a, b) trying to merge into [a, c)
    ByteStringRange parentPartitionAB = ByteStringRange.create("a", "b");
    Instant watermarkAB = startTime;
    ChangeStreamContinuationToken tokenAB =
        ChangeStreamContinuationToken.create(parentPartitionAB, "ab");

    ByteStringRange childPartitionAC = ByteStringRange.create("a", "c");

    NewPartition newPartitionACFromAB =
        new NewPartition(childPartitionAC, Collections.singletonList(tokenAB), watermarkAB);

    metadataTableDao.writeNewPartition(newPartitionACFromAB);

    // Artificially create that partitionAB has been missing for more than 1 minute.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(
        parentPartitionAB, Instant.now().minus(Duration.standardSeconds(121)));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    assertEquals(1, metadataTableDao.readNewPartitions().size());

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    // AB should be reconciled with token because it's been missing for more than 1 minute
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(parentPartitionAB, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermarkAB, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertEquals(endTime, partitionRecordArgumentCaptor.getValue().getEndTime());
    assertEquals(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Collections.singletonList(tokenAB));
    assertTrue(metadataTableDao.readNewPartitions().isEmpty());
    assertTrue(metadataTableDao.readDetectNewPartitionMissingPartitions().isEmpty());

    clearInvocations(receiver);
    // The reconciled partition was not processed by RCSP, so NewPartition is still marked for
    // deletion and the partition is still considered missing. We run DNP again.
    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(100)),
        action.run(
            tracker, receiver, watermarkEstimator, new InitialPipelineState(startTime, false)));
    // We don't reconcile the partition again.
    verify(receiver, never()).outputWithTimestamp(any(), any());
  }
}
