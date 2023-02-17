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
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
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
    action =
        new DetectNewPartitionsAction(metrics, metadataTableDao, generateInitialPartitionsAction);
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

  @Test
  public void testProcessSplitNewPartitions() throws Exception {
    // Avoid 0 and multiples of 10 so that we can specifically test just reading new partitions.
    OffsetRange offsetRange = new OffsetRange(1, Long.MAX_VALUE);
    when(tracker.currentRestriction()).thenReturn(offsetRange);
    when(tracker.tryClaim(offsetRange.getFrom())).thenReturn(true);

    // ["", "") splits into ["", "a"), ["a","c"), ["c", "")
    Range.ByteStringRange parentPartition = Range.ByteStringRange.create("", "");
    Instant watermark = Instant.now();
    Range.ByteStringRange child1 = Range.ByteStringRange.create("", "a");
    metadataTableDao.writeNewPartition(
        ChangeStreamContinuationToken.create(
            child1, Range.ByteStringRange.serializeToByteString(child1).toStringUtf8()),
        parentPartition,
        watermark);
    Range.ByteStringRange child2 = Range.ByteStringRange.create("k", "");
    metadataTableDao.writeNewPartition(
        ChangeStreamContinuationToken.create(
            child2, Range.ByteStringRange.serializeToByteString(child2).toStringUtf8()),
        parentPartition,
        watermark);
    Range.ByteStringRange child3 = Range.ByteStringRange.create("a", "k");
    metadataTableDao.writeNewPartition(
        ChangeStreamContinuationToken.create(
            child3, Range.ByteStringRange.serializeToByteString(child3).toStringUtf8()),
        parentPartition,
        watermark);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));

    // Check what parameters were passed to OutputReceiver to verify the new partitions created by
    // DNP are correct.
    verify(receiver, times(3))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<Range.ByteStringRange> expectedPartitions =
        partitionRecordArgumentCaptor.getAllValues().stream()
            .map(PartitionRecord::getPartition)
            .collect(Collectors.toList());
    List<Range.ByteStringRange> actualPartitions = Arrays.asList(child1, child2, child3);
    assertThat(expectedPartitions, Matchers.containsInAnyOrder(actualPartitions.toArray()));
    for (PartitionRecord value : partitionRecordArgumentCaptor.getAllValues()) {
      assertEquals(value.getParentLowWatermark(), watermark);
      assertNotNull(value.getChangeStreamContinuationTokens());
      assertEquals(1, value.getChangeStreamContinuationTokens().size());
      // Verify token is correct. For the test, the continuation token string is the string
      // representation of the partition itself.
      assertEquals(
          value.getChangeStreamContinuationTokens().get(0).getToken(),
          Range.ByteStringRange.serializeToByteString(
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
    Range.ByteStringRange parentPartition1 = Range.ByteStringRange.create("a", "b");
    Instant watermark1 = startTime;
    Range.ByteStringRange parentPartition2 = Range.ByteStringRange.create("b", "c");
    Instant watermark2 = startTime.plus(Duration.millis(10));

    Range.ByteStringRange childPartition = Range.ByteStringRange.create("a", "c");
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(childPartition, "token1");
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(childPartition, "token2");
    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(token1, parentPartition1, watermark1);
    metadataTableDao.writeNewPartition(token2, parentPartition2, watermark2);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartition, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermark1, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
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
    Range.ByteStringRange parentPartition1 = Range.ByteStringRange.create("a", "b");
    Instant watermark1 = startTime;

    Range.ByteStringRange childPartition = Range.ByteStringRange.create("a", "c");
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(childPartition, "token1");
    // Write a new partition for every parent partition that merges into the child.
    metadataTableDao.writeNewPartition(token1, parentPartition1, watermark1);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, never()).outputWithTimestamp(any(), any());

    // On next iteration, ["b", "c") received CloseStream and writes to the metadata table.
    Range.ByteStringRange parentPartition2 = Range.ByteStringRange.create("b", "c");
    Instant watermark2 = startTime.plus(Duration.millis(10));
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(childPartition, "token2");
    metadataTableDao.writeNewPartition(token2, parentPartition2, watermark2);

    assertEquals(
        DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1)),
        action.run(tracker, receiver, watermarkEstimator, bundleFinalizer, startTime));
    // The partition is outputted with watermark1 because that is the lowest of the 2 forming the
    // parent low watermark.
    verify(receiver, times(1))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));

    assertEquals(childPartition, partitionRecordArgumentCaptor.getValue().getPartition());
    assertEquals(watermark1, partitionRecordArgumentCaptor.getValue().getParentLowWatermark());
    assertThat(
        partitionRecordArgumentCaptor.getValue().getChangeStreamContinuationTokens(),
        Matchers.containsInAnyOrder(token1, token2));
  }
}
