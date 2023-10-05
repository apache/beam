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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
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
public class ResumeFromPreviousPipelineActionTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  private MetadataTableDao metadataTableDao;
  @Mock private ChangeStreamMetrics metrics;
  @Mock private OutputReceiver<PartitionRecord> receiver;

  private Instant endTime;
  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;

  private ResumeFromPreviousPipelineAction action;

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
    endTime = Instant.now().plus(Duration.standardSeconds(10));
    ProcessNewPartitionsAction processNewPartitionsAction =
        new ProcessNewPartitionsAction(metrics, metadataTableDao, endTime);
    action =
        new ResumeFromPreviousPipelineAction(
            metrics, metadataTableDao, endTime, processNewPartitionsAction);
  }

  @Test
  public void testResetMissingPartitions() throws InvalidProtocolBufferException {
    HashMap<ByteStringRange, Instant> missingPartitionsDuration = new HashMap<>();
    missingPartitionsDuration.put(ByteStringRange.create("A", "B"), Instant.now());
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionsDuration);
    assertFalse(metadataTableDao.readDetectNewPartitionMissingPartitions().isEmpty());
    action.run(receiver);
    assertTrue(metadataTableDao.readDetectNewPartitionMissingPartitions().isEmpty());
  }

  @Test
  public void testOutputExistingStreamPartitions() throws InvalidProtocolBufferException {
    ByteStringRange partition1 = ByteStringRange.create("A", "B");
    Instant watermark1 = Instant.now().minus(Duration.standardSeconds(10));
    PartitionRecord partitionWithStartTime =
        new PartitionRecord(partition1, watermark1, "1", watermark1, Collections.emptyList(), null);
    metadataTableDao.lockAndRecordPartition(partitionWithStartTime);

    ByteStringRange partition2 = ByteStringRange.create("B", "D");
    ChangeStreamContinuationToken partition2Token1 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "tokenBC");
    ChangeStreamContinuationToken partition2Token2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("C", "D"), "tokenCD");
    Instant watermark2 = Instant.now().plus(Duration.standardMinutes(1));
    PartitionRecord partitionWithInitialTokens =
        new PartitionRecord(
            partition2,
            Arrays.asList(partition2Token1, partition2Token2),
            "2",
            watermark2,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionWithInitialTokens);

    ByteStringRange partition3 = ByteStringRange.create("D", "H");
    Instant watermark3 = Instant.now();
    PartitionRecord partitionWithContinuationToken =
        new PartitionRecord(partition3, watermark3, "3", watermark3, Collections.emptyList(), null);
    metadataTableDao.lockAndRecordPartition(partitionWithContinuationToken);
    ChangeStreamContinuationToken partition3token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("D", "H"), "tokenDH");
    metadataTableDao.updateWatermark(partition3, watermark3, partition3token);

    ByteStringRange partition4 = ByteStringRange.create("H", "I");
    ChangeStreamContinuationToken partition4token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("H", "I"), "tokenHI");
    Instant watermark4 = Instant.now();
    PartitionRecord unlockedPartition =
        new PartitionRecord(
            partition4,
            Collections.singletonList(partition4token),
            "4",
            watermark4,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(unlockedPartition);
    metadataTableDao.releaseStreamPartitionLockForDeletion(partition4, unlockedPartition.getUuid());

    action.run(receiver);
    verify(receiver, times(4))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<PartitionRecord> actualPartitions = partitionRecordArgumentCaptor.getAllValues();

    PartitionRecord actualPartition1 = actualPartitions.get(0);
    assertEquals(partition1, actualPartition1.getPartition());
    assertEquals(partitionWithStartTime.getUuid(), actualPartition1.getUuid());
    assertNull(actualPartition1.getChangeStreamContinuationTokens());
    assertEquals(partitionWithStartTime.getStartTime(), actualPartition1.getStartTime());
    assertEquals(endTime, actualPartition1.getEndTime());
    assertEquals(
        partitionWithStartTime.getParentLowWatermark(), actualPartition1.getParentLowWatermark());

    PartitionRecord actualPartition2 = actualPartitions.get(1);
    assertEquals(partition2, actualPartition2.getPartition());
    assertEquals(partitionWithInitialTokens.getUuid(), actualPartition2.getUuid());
    assertNull(actualPartition2.getStartTime());
    assertEquals(
        partitionWithInitialTokens.getChangeStreamContinuationTokens(),
        actualPartition2.getChangeStreamContinuationTokens());
    assertEquals(endTime, actualPartition2.getEndTime());
    assertEquals(
        partitionWithInitialTokens.getParentLowWatermark(),
        actualPartition2.getParentLowWatermark());

    PartitionRecord actualPartition3 = actualPartitions.get(2);
    assertEquals(partition3, actualPartition3.getPartition());
    assertEquals(partitionWithContinuationToken.getUuid(), actualPartition3.getUuid());
    assertNull(actualPartition3.getStartTime());
    assertNotNull(actualPartition3.getChangeStreamContinuationTokens());
    assertEquals(1, actualPartition3.getChangeStreamContinuationTokens().size());
    assertEquals(partition3token, actualPartition3.getChangeStreamContinuationTokens().get(0));
    assertEquals(endTime, actualPartition3.getEndTime());
    assertEquals(
        partitionWithContinuationToken.getParentLowWatermark(),
        actualPartition3.getParentLowWatermark());

    PartitionRecord actualPartition4 = actualPartitions.get(3);
    assertEquals(partition4, actualPartition4.getPartition());
    // partition4 was unlocked so it gets a new uuid.
    assertNotEquals("4", actualPartition4.getUuid());
    assertNull(actualPartition4.getStartTime());
    assertNotNull(actualPartition4.getChangeStreamContinuationTokens());
    assertEquals(1, actualPartition4.getChangeStreamContinuationTokens().size());
    assertEquals(partition4token, actualPartition4.getChangeStreamContinuationTokens().get(0));
    assertEquals(endTime, actualPartition4.getEndTime());
    assertEquals(
        unlockedPartition.getParentLowWatermark(), actualPartition4.getParentLowWatermark());
  }

  @Test
  public void testOutputNewPartitions() throws InvalidProtocolBufferException {
    ByteStringRange partition1 = ByteStringRange.create("A", "B");
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(partition1, "tokenAB");
    Instant watermark1 = Instant.now().minus(Duration.standardSeconds(10));
    NewPartition newSplitPartition =
        new NewPartition(partition1, Collections.singletonList(token1), watermark1);
    metadataTableDao.writeNewPartition(newSplitPartition);

    ByteStringRange partition2 = ByteStringRange.create("B", "D");
    ByteStringRange partition2parent1 = ByteStringRange.create("C", "D");
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(partition2parent1, "tokenCD");
    Instant watermark2 = Instant.now().plus(Duration.standardSeconds(10));
    NewPartition newPartitionMissingParent =
        new NewPartition(partition2, Collections.singletonList(token2), watermark2);
    metadataTableDao.writeNewPartition(newPartitionMissingParent);

    ByteStringRange partition3 = ByteStringRange.create("D", "E");
    ChangeStreamContinuationToken token3 =
        ChangeStreamContinuationToken.create(partition3, "tokenDE");
    Instant watermark3 = Instant.now().plus(Duration.standardSeconds(5));
    NewPartition deletedNewPartition =
        new NewPartition(partition3, Collections.singletonList(token3), watermark3);
    metadataTableDao.writeNewPartition(deletedNewPartition);
    metadataTableDao.markNewPartitionForDeletion(deletedNewPartition);

    // There are only 2 NewPartition rows because partition3 is deleted.
    assertEquals(2, metadataTableDao.readNewPartitions().size());

    action.run(receiver);
    verify(receiver, times(2))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<PartitionRecord> actualPartitions = partitionRecordArgumentCaptor.getAllValues();

    PartitionRecord actualPartition1 = actualPartitions.get(0);
    assertEquals(partition1, actualPartition1.getPartition());
    assertEquals(1, actualPartition1.getChangeStreamContinuationTokens().size());
    assertEquals(token1, actualPartition1.getChangeStreamContinuationTokens().get(0));
    assertNull(actualPartition1.getStartTime());
    assertEquals(watermark1, actualPartition1.getParentLowWatermark());
    assertEquals(endTime, actualPartition1.getEndTime());
    assertEquals(
        Collections.singletonList(newSplitPartition), actualPartition1.getParentPartitions());
    // Uuid is filled.
    assertFalse(actualPartition1.getUuid().isEmpty());

    // The 2nd partition is not partition2 but partition3 because partition2 is missing a parent.
    // Even though partition3 is marked for deletion, we still process it.
    PartitionRecord actualPartition2 = actualPartitions.get(1);
    assertEquals(partition3, actualPartition2.getPartition());
    assertEquals(1, actualPartition2.getChangeStreamContinuationTokens().size());
    assertEquals(token3, actualPartition2.getChangeStreamContinuationTokens().get(0));
    assertNull(actualPartition2.getStartTime());
    assertEquals(watermark3, actualPartition2.getParentLowWatermark());
    assertEquals(endTime, actualPartition2.getEndTime());
    assertEquals(
        Collections.singletonList(deletedNewPartition), actualPartition2.getParentPartitions());

    // There is only 1 NewPartition row now because we processed them except for the NewPartition
    // with missing parent.
    assertEquals(1, metadataTableDao.readNewPartitions().size());
  }
}
