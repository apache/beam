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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseWatermarkFromRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.StreamPartitionWithWatermark;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class MetadataTableDaoTest {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableDaoTest.class);

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  private static MetadataTableDao metadataTableDao;
  private static MetadataTableAdminDao metadataTableAdminDao;
  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;

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
  public void before() {
    String changeStreamId = UniqueIdGenerator.generateRowKeyPrefix();
    metadataTableAdminDao =
        new MetadataTableAdminDao(
            adminClient, null, changeStreamId, MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME);
    metadataTableAdminDao.createMetadataTable();

    metadataTableDao =
        new MetadataTableDao(
            dataClient,
            metadataTableAdminDao.getTableId(),
            metadataTableAdminDao.getChangeStreamNamePrefix());
  }

  @Test
  public void testStreamPartitionRowKeyConversion() throws InvalidProtocolBufferException {
    ByteStringRange rowRange = ByteStringRange.create("a", "b");
    ByteString rowKey = metadataTableDao.convertPartitionToStreamPartitionRowKey(rowRange);
    assertEquals(rowRange, metadataTableDao.convertStreamPartitionRowKeyToPartition(rowKey));
  }

  @Test
  public void testStreamPartitionRowKeyConversionWithIllegalUtf8()
      throws InvalidProtocolBufferException {
    // Test that the conversion is able to handle non-utf8 values.
    byte[] nonUtf8Bytes = {(byte) 0b10001100};
    ByteString nonUtf8RowKey = ByteString.copyFrom(nonUtf8Bytes);
    ByteStringRange rowRange = ByteStringRange.create(nonUtf8RowKey, nonUtf8RowKey);
    ByteString rowKey = metadataTableDao.convertPartitionToStreamPartitionRowKey(rowRange);
    assertEquals(rowRange, metadataTableDao.convertStreamPartitionRowKeyToPartition(rowKey));
  }

  @Test
  public void testNewPartitionRowKeyConversion() throws InvalidProtocolBufferException {
    ByteStringRange rowRange = ByteStringRange.create("a", "b");
    ByteString rowKey = metadataTableDao.convertPartitionToNewPartitionRowKey(rowRange);
    assertEquals(rowRange, metadataTableDao.convertNewPartitionRowKeyToPartition(rowKey));
  }

  @Test
  public void testNewPartitionConversionWithWithIllegalUtf8()
      throws InvalidProtocolBufferException {
    // Test that the conversion is able to handle non-utf8 values.
    byte[] nonUtf8Bytes = {(byte) 0b10001100};
    ByteString nonUtf8BytesString = ByteString.copyFrom(nonUtf8Bytes);
    ByteStringRange rowRange = ByteStringRange.create(nonUtf8BytesString, nonUtf8BytesString);
    ByteString rowKey = metadataTableDao.convertPartitionToNewPartitionRowKey(rowRange);
    assertEquals(rowRange, metadataTableDao.convertNewPartitionRowKeyToPartition(rowKey));
  }

  @Test
  public void testLockPartitionRaceUniqueIds() throws InterruptedException {
    ByteStringRange partition = ByteStringRange.create("", "");
    ByteString rowKey = metadataTableDao.convertPartitionToStreamPartitionRowKey(partition);
    // Class to try to lock the partition in a separate thread.
    class LockPartition implements Runnable {
      final PartitionRecord partitionRecord =
          new PartitionRecord(
              partition, Collections.emptyList(), Instant.now(), Collections.emptyList());
      boolean locked = false;

      LockPartition(String id) {
        partitionRecord.setUuid(id);
      }

      @Override
      public void run() {
        try {
          // Sleep for a random amount before trying to lock the partition to add variability to the
          // race.
          int sleep = (int) (Math.random() * 1000);
          Thread.sleep(sleep);
          if (metadataTableDao.lockAndRecordPartition(partitionRecord)) {
            locked = true;
          }
        } catch (InterruptedException e) {
          LOG.error(e.toString());
        }
      }
    }

    List<LockPartition> lockPartitions = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();

    int competingThreadCount = 1000;

    // Create and start the threads to lock the partition.
    for (int i = 0; i < competingThreadCount; i++) {
      lockPartitions.add(new LockPartition(Integer.toString(i)));
      threads.add(new Thread(lockPartitions.get(i)));
    }
    for (int i = 0; i < competingThreadCount; i++) {
      threads.get(i).start();
    }
    for (int i = 0; i < competingThreadCount; i++) {
      threads.get(i).join();
    }
    int lockOwner = -1;
    for (int i = 0; i < competingThreadCount; i++) {
      if (lockPartitions.get(i).locked) {
        // There can be only 1 owner for the lock.
        if (lockOwner == -1) {
          lockOwner = i;
        } else {
          fail(
              "Multiple owner on the lock. Both "
                  + lockOwner
                  + " and "
                  + i
                  + " (and possibly more) think they hold the lock.");
        }
      }
    }
    // Verify that the owner is indeed the owner of the locker.
    Row row =
        dataClient.readRow(
            metadataTableAdminDao.getTableId(),
            rowKey,
            Filters.FILTERS
                .chain()
                .filter(Filters.FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
                .filter(
                    Filters.FILTERS
                        .qualifier()
                        .exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT)));
    assertEquals(1, row.getCells().size());
    assertEquals(Integer.toString(lockOwner), row.getCells().get(0).getValue().toStringUtf8());
    // Clean up the locked partition row.
    RowMutation rowMutation =
        RowMutation.create(metadataTableAdminDao.getTableId(), rowKey).deleteRow();
    dataClient.mutateRow(rowMutation);
  }

  @Test
  public void testLockPartitionRaceDuplicateIds() throws InterruptedException {
    ByteStringRange partition = ByteStringRange.create("", "");
    String uid = "a";
    MetadataTableDao spy = Mockito.spy(metadataTableDao);
    // First call we sleep for ten seconds to ensure the duplicate acquires the
    // lock before we return.
    when(spy.doHoldLock(partition, uid))
        .then(
            (Answer<Boolean>)
                invocation -> {
                  Thread.sleep(10000);
                  return false;
                })
        .thenCallRealMethod()
        .thenCallRealMethod();

    class LockPartition implements Runnable {
      final PartitionRecord partitionRecord =
          new PartitionRecord(
              partition,
              Collections.emptyList(),
              uid,
              Instant.now(),
              Collections.emptyList(),
              Instant.now().plus(Duration.standardMinutes(10)));
      boolean locked = false;

      @Override
      public void run() {
        locked = spy.lockAndRecordPartition(partitionRecord);
      }
    }

    LockPartition dup1 = new LockPartition();
    Thread dup1Thread = new Thread(dup1);
    LockPartition dup2 = new LockPartition();
    Thread dup2Thread = new Thread(dup2);

    dup1Thread.start();
    dup2Thread.start();
    dup1Thread.join();
    dup2Thread.join();

    assertTrue(dup2.locked);
    assertTrue(dup1.locked);
  }

  @Test
  public void testReadStreamPartitionsWithWatermark() throws InvalidProtocolBufferException {
    ByteStringRange lockedPartition = ByteStringRange.create("", "a");
    PartitionRecord partitionRecord =
        new PartitionRecord(
            lockedPartition,
            Instant.now(),
            UniqueIdGenerator.getNextId(),
            Instant.now(),
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord);

    // Only one row has both watermark and is locked.
    List<StreamPartitionWithWatermark> streamPartitionsWithWatermark =
        metadataTableDao.readStreamPartitionsWithWatermark();
    assertEquals(1, streamPartitionsWithWatermark.size());
    assertEquals(
        partitionRecord.getParentLowWatermark(),
        streamPartitionsWithWatermark.get(0).getWatermark());
    assertEquals(
        partitionRecord.getPartition(), streamPartitionsWithWatermark.get(0).getPartition());

    Instant watermark = Instant.now();
    // Update the watermark on the locked partition.
    metadataTableDao.updateWatermark(lockedPartition, watermark, null);

    // Watermark is updated.
    streamPartitionsWithWatermark = metadataTableDao.readStreamPartitionsWithWatermark();
    assertEquals(1, streamPartitionsWithWatermark.size());
    assertEquals(watermark, streamPartitionsWithWatermark.get(0).getWatermark());
  }

  @Test
  public void testNewPartitionsWriteRead() throws InvalidProtocolBufferException {
    // This test a split of ["", "") to ["", "a") and ["a", "")
    Instant lowWatermark = Instant.now();
    ByteStringRange partitionEmptyA = ByteStringRange.create("", "a");
    ChangeStreamContinuationToken tokenEmptyA =
        ChangeStreamContinuationToken.create(partitionEmptyA, "EmptyA");
    NewPartition newPartitionEmptyA =
        new NewPartition(partitionEmptyA, Collections.singletonList(tokenEmptyA), lowWatermark);

    ByteStringRange partitionAEmpty = ByteStringRange.create("a", "");
    ChangeStreamContinuationToken tokenAEmpty =
        ChangeStreamContinuationToken.create(partitionAEmpty, "AEmpty");
    NewPartition newPartitionAEmpty =
        new NewPartition(partitionAEmpty, Collections.singletonList(tokenAEmpty), lowWatermark);

    metadataTableDao.writeNewPartition(newPartitionEmptyA);
    metadataTableDao.writeNewPartition(newPartitionAEmpty);

    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    int rowsCount = 0;
    boolean matchedPartition1 = false;
    boolean matchedPartition2 = false;
    for (NewPartition newPartition : newPartitions) {
      rowsCount++;
      if (newPartition.getPartition().equals(partitionEmptyA)) {
        matchedPartition1 = true;
      } else if (newPartition.getPartition().equals(partitionAEmpty)) {
        matchedPartition2 = true;
      }
    }
    assertTrue(matchedPartition1);
    assertTrue(matchedPartition2);
    assertEquals(2, rowsCount);
  }

  @Test
  public void testMarkNewPartitionForDeletion() throws InvalidProtocolBufferException {
    ByteStringRange childPartition = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "BC");
    NewPartition newPartition =
        new NewPartition(childPartition, Collections.singletonList(token), Instant.now());
    metadataTableDao.writeNewPartition(newPartition);
    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());

    // Once it's marked for deletion, it is not visible to readNewPartitions.
    metadataTableDao.markNewPartitionForDeletion(newPartitions.get(0));
    assertEquals(0, metadataTableDao.readNewPartitions().size());
  }

  @Test
  public void testMarkNewPartitionForDeletionVisibleAfterOneMinute()
      throws InvalidProtocolBufferException {
    ByteStringRange childPartition = ByteStringRange.create("A", "C");
    ByteStringRange parentPartition = ByteStringRange.create("B", "C");
    ChangeStreamContinuationToken token =
        ChangeStreamContinuationToken.create(parentPartition, "BC");
    NewPartition newPartition =
        new NewPartition(childPartition, Collections.singletonList(token), Instant.now());
    metadataTableDao.writeNewPartition(newPartition);
    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());

    // Mark it for deletion but more than 60s ago.
    RowMutation rowMutation =
        RowMutation.create(
                metadataTableAdminDao.getTableId(),
                metadataTableDao.convertPartitionToNewPartitionRowKey(childPartition))
            .setCell(
                MetadataTableAdminDao.CF_SHOULD_DELETE,
                ByteStringRange.serializeToByteString(parentPartition),
                Instant.now().minus(Duration.standardSeconds(61)).getMillis() * 1_000L,
                1);
    dataClient.mutateRow(rowMutation);

    // We should be able to read back the NewPartition.
    newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());
  }

  @Test
  public void testDeleteNewPartition() throws InvalidProtocolBufferException {
    ByteStringRange childPartition = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "BC");
    NewPartition newPartition =
        new NewPartition(childPartition, Collections.singletonList(token), Instant.now());
    metadataTableDao.writeNewPartition(newPartition);
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "B"), "AB");
    NewPartition newPartition2 =
        new NewPartition(childPartition, Collections.singletonList(token2), Instant.now());
    metadataTableDao.writeNewPartition(newPartition2);

    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());

    // Did not mark new partition for deletion. Cannot delete.
    assertFalse(metadataTableDao.deleteNewPartition(newPartitions.get(0)));
    assertEquals(1, metadataTableDao.readNewPartitions().size());

    metadataTableDao.markNewPartitionForDeletion(newPartitions.get(0));
    assertTrue(metadataTableDao.deleteNewPartition(newPartitions.get(0)));
    assertEquals(0, metadataTableDao.readNewPartitions().size());
  }

  @Test
  public void testPartialDeleteNewPartition() throws InvalidProtocolBufferException {
    ByteStringRange childPartition = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "BC");
    NewPartition newPartition =
        new NewPartition(childPartition, Collections.singletonList(token), Instant.now());
    metadataTableDao.writeNewPartition(newPartition);
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "B"), "AB");
    NewPartition newPartition2 =
        new NewPartition(childPartition, Collections.singletonList(token2), Instant.now());
    metadataTableDao.writeNewPartition(newPartition2);

    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());
    assertEquals(2, newPartitions.get(0).getChangeStreamContinuationTokens().size());

    // Only mark one of the parent for deletion but try to delete the entire new partition. This
    // will succeed but only the parent marked for deletion will be deleted.
    metadataTableDao.markNewPartitionForDeletion(newPartition);
    assertFalse(metadataTableDao.deleteNewPartition(newPartitions.get(0)));
    newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());
    assertEquals(1, newPartitions.get(0).getChangeStreamContinuationTokens().size());
    assertEquals(token2, newPartitions.get(0).getChangeStreamContinuationTokens().get(0));

    // Try deleting the parent that isn't marked for deletion. It is not possible
    assertFalse(metadataTableDao.deleteNewPartition(newPartition2));
    newPartitions = metadataTableDao.readNewPartitions();
    assertEquals(1, newPartitions.size());
    assertEquals(childPartition, newPartitions.get(0).getPartition());
    assertEquals(1, newPartitions.get(0).getChangeStreamContinuationTokens().size());
    assertEquals(token2, newPartitions.get(0).getChangeStreamContinuationTokens().get(0));

    // Mark the remaining parent for deletion.
    metadataTableDao.markNewPartitionForDeletion(newPartition2);
    assertTrue(metadataTableDao.deleteNewPartition(newPartition2));
    assertEquals(0, metadataTableDao.readNewPartitions().size());
  }

  @Test
  public void testUpdateAndReadWatermark() throws InvalidProtocolBufferException {
    ByteStringRange partition1 = ByteStringRange.create("a", "b");
    Instant watermark1 = Instant.now();

    PartitionRecord partitionRecord =
        new PartitionRecord(
            partition1,
            Instant.now(),
            UniqueIdGenerator.getNextId(),
            Instant.now(),
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord);
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = Instant.now();
    PartitionRecord partitionRecord2 =
        new PartitionRecord(
            partition2,
            Instant.now(),
            UniqueIdGenerator.getNextId(),
            Instant.now(),
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord2);
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    List<StreamPartitionWithWatermark> streamPartitionsWithWatermark =
        metadataTableDao.readStreamPartitionsWithWatermark();
    assertThat(
        streamPartitionsWithWatermark,
        containsInAnyOrder(
            new StreamPartitionWithWatermark(partition1, watermark1),
            new StreamPartitionWithWatermark(partition2, watermark2)));
  }

  @Test
  public void testReadingExistingStreamPartitions() throws InvalidProtocolBufferException {
    ByteStringRange partition1 = ByteStringRange.create("A", "B");
    Instant watermark1 = Instant.now();
    PartitionRecord partitionRecord1 =
        new PartitionRecord(partition1, watermark1, "1", watermark1, Collections.emptyList(), null);
    metadataTableDao.lockAndRecordPartition(partitionRecord1);

    ByteStringRange partition2 = ByteStringRange.create("B", "D");
    ChangeStreamContinuationToken partition2Token1 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "tokenBC");
    ChangeStreamContinuationToken partition2Token2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("C", "D"), "tokenCD");
    Instant watermark2 = Instant.now();
    PartitionRecord partitionRecord2 =
        new PartitionRecord(
            partition2,
            Arrays.asList(partition2Token1, partition2Token2),
            "2",
            watermark2,
            Collections.emptyList(),
            null);
    metadataTableDao.lockAndRecordPartition(partitionRecord2);

    List<PartitionRecord> partitionRecords = metadataTableDao.readAllStreamPartitions();
    assertThat(partitionRecords, containsInAnyOrder(partitionRecord1, partitionRecord2));

    // Update the watermark of partition2
    Instant watermark3 = watermark2.plus(Duration.standardSeconds(10));
    metadataTableDao.updateWatermark(partition2, watermark3, null);
    PartitionRecord partitionRecord2UpdatedWatermark =
        new PartitionRecord(
            partition2,
            Arrays.asList(partition2Token1, partition2Token2),
            "2",
            watermark3,
            Collections.emptyList(),
            null);
    partitionRecords = metadataTableDao.readAllStreamPartitions();
    assertThat(
        partitionRecords, containsInAnyOrder(partitionRecord1, partitionRecord2UpdatedWatermark));

    // Update partition1 with new watermark and continuation token
    Instant watermark4 = watermark1.plus(Duration.standardMinutes(1));
    ChangeStreamContinuationToken partition1Token =
        ChangeStreamContinuationToken.create(partition1, "tokenAB");
    metadataTableDao.updateWatermark(partition1, watermark4, partition1Token);
    PartitionRecord partitionRecord1UpdatedWatermark =
        new PartitionRecord(
            partition1,
            Collections.singletonList(partition1Token),
            "1",
            watermark4,
            Collections.emptyList(),
            null);
    partitionRecords = metadataTableDao.readAllStreamPartitions();
    assertThat(
        partitionRecords,
        containsInAnyOrder(partitionRecord1UpdatedWatermark, partitionRecord2UpdatedWatermark));

    // Release the lock on partition2, the results should be the same.
    metadataTableDao.releaseStreamPartitionLockForDeletion(partition2, partitionRecord2.getUuid());
    partitionRecords = metadataTableDao.readAllStreamPartitions();
    PartitionRecord partitionRecord2NoUuid =
        new PartitionRecord(
            partition2,
            Arrays.asList(partition2Token1, partition2Token2),
            watermark3,
            Collections.emptyList());
    assertThat(
        partitionRecords,
        containsInAnyOrder(partitionRecord1UpdatedWatermark, partitionRecord2NoUuid));

    // Delete partition2
    metadataTableDao.deleteStreamPartitionRow(partition2);
    partitionRecords = metadataTableDao.readAllStreamPartitions();
    assertThat(partitionRecords, containsInAnyOrder(partitionRecord1UpdatedWatermark));
  }

  @Test
  public void testUpdateDetectNewPartitionWatermark() {
    Instant watermark = Instant.now();
    metadataTableDao.updateDetectNewPartitionWatermark(watermark);
    Row row =
        dataClient.readRow(
            metadataTableAdminDao.getTableId(),
            metadataTableDao
                .getChangeStreamNamePrefix()
                .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX));
    assertNull(MetadataTableEncoder.parseTokenFromRow(row));
    assertEquals(watermark, parseWatermarkFromRow(row));
  }

  @Test
  public void testReadWriteLowWatermark() {
    Instant t = Instant.now();
    metadataTableDao.updateDetectNewPartitionWatermark(t);
    assertEquals(t, metadataTableDao.readDetectNewPartitionsState().getWatermark());
    t = t.plus(Duration.standardMinutes(10));
    metadataTableDao.updateDetectNewPartitionWatermark(t);
    assertEquals(t, metadataTableDao.readDetectNewPartitionsState().getWatermark());
  }

  @Test
  public void testUpdateWatermark() {
    ByteStringRange partition = ByteStringRange.create("a", "b");
    Instant watermark = Instant.now();
    ChangeStreamContinuationToken token = ChangeStreamContinuationToken.create(partition, "1234");
    metadataTableDao.updateWatermark(partition, watermark, token);
    Row row =
        dataClient.readRow(
            metadataTableAdminDao.getTableId(),
            metadataTableDao.convertPartitionToStreamPartitionRowKey(partition));
    assertEquals(token.getToken(), MetadataTableEncoder.parseTokenFromRow(row));
    assertEquals(watermark, parseWatermarkFromRow(row));
  }

  @Test
  public void readAndWriteValidMissingPartitionsDuration() {
    HashMap<ByteStringRange, Instant> missingPartitionsDuration = new HashMap<>();
    missingPartitionsDuration.put(ByteStringRange.create("A", "B"), Instant.ofEpochMilli(100));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionsDuration);
    HashMap<ByteStringRange, Instant> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void readAndWriteInvalidMissingPartitionsDuration() {
    HashMap<ByteStringRange, Instant> missingPartitionsDuration = new HashMap<>();

    RowMutation rowMutation =
        RowMutation.create(
                metadataTableAdminDao.getTableId(),
                metadataTableAdminDao
                    .getChangeStreamNamePrefix()
                    .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX))
            .setCell(
                MetadataTableAdminDao.CF_MISSING_PARTITIONS,
                ByteString.copyFromUtf8(MetadataTableAdminDao.QUALIFIER_DEFAULT),
                ByteString.copyFromUtf8("Invalid serialization"));
    dataClient.mutateRow(rowMutation);

    // We should still be able to read the invalid serialization and return an empty map.
    HashMap<ByteStringRange, Instant> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void readMissingPartitionsWithoutDNPRow() {
    HashMap<ByteStringRange, Instant> missingPartitionsDuration = new HashMap<>();
    HashMap<ByteStringRange, Instant> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void readMissingPartitionsWithoutMissingPartitionsCell() {
    HashMap<ByteStringRange, Instant> missingPartitionsDuration = new HashMap<>();
    // Write DNP row but no missing partitions cell.
    metadataTableDao.updateDetectNewPartitionWatermark(Instant.now());
    HashMap<ByteStringRange, Instant> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void testReleaseStreamPartitionLock() throws InvalidProtocolBufferException {
    ByteStringRange partition = ByteStringRange.create("A", "B");
    String uuid = "1234";
    PartitionRecord partitionRecord =
        new PartitionRecord(
            partition, Instant.now(), uuid, Instant.now(), Collections.emptyList(), null);
    metadataTableDao.lockAndRecordPartition(partitionRecord);
    assertFalse(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertTrue(metadataTableDao.doHoldLock(partition, uuid));

    assertFalse(metadataTableDao.releaseStreamPartitionLockForDeletion(partition, "0000"));
    assertFalse(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertTrue(metadataTableDao.doHoldLock(partition, uuid));

    assertTrue(metadataTableDao.releaseStreamPartitionLockForDeletion(partition, uuid));
    assertTrue(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertFalse(metadataTableDao.doHoldLock(partition, uuid));
  }

  @Test
  public void testDeleteStreamPartitionRow() throws InvalidProtocolBufferException {
    ByteStringRange partition = ByteStringRange.create("A", "B");
    String uuid = "1234";
    PartitionRecord partitionRecord =
        new PartitionRecord(
            partition, Instant.now(), uuid, Instant.now(), Collections.emptyList(), null);
    assertTrue(metadataTableDao.lockAndRecordPartition(partitionRecord));
    metadataTableDao.updateWatermark(partition, Instant.now(), null);
    assertFalse(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertTrue(metadataTableDao.doHoldLock(partition, uuid));

    // Can't delete it because lock needs to be released first
    assertFalse(metadataTableDao.deleteStreamPartitionRow(partition));
    assertFalse(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertTrue(metadataTableDao.doHoldLock(partition, uuid));

    // Release and delete
    assertTrue(metadataTableDao.releaseStreamPartitionLockForDeletion(partition, uuid));
    assertTrue(metadataTableDao.deleteStreamPartitionRow(partition));
    assertTrue(metadataTableDao.readStreamPartitionsWithWatermark().isEmpty());
    assertFalse(metadataTableDao.doHoldLock(partition, uuid));
  }

  @Test
  public void testLockPartitionRecordsMetadata() {
    ByteStringRange partition = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken token =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "token1");
    PartitionRecord partitionRecord =
        new PartitionRecord(
            partition,
            Collections.singletonList(token),
            "1234",
            Instant.now(),
            Collections.emptyList(),
            Instant.now());
    metadataTableDao.lockAndRecordPartition(partitionRecord);
    assertTrue(metadataTableDao.doHoldLock(partition, "1234"));
    // Check that watermark and tokens are correctly written.
    Row row =
        dataClient.readRow(
            metadataTableAdminDao.getTableId(),
            metadataTableDao.convertPartitionToStreamPartitionRowKey(partition));
    assertTrue(
        row.getCells(
                MetadataTableAdminDao.CF_SHOULD_DELETE, MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .isEmpty());
    assertEquals(partitionRecord.getParentLowWatermark(), parseWatermarkFromRow(row));
    assertEquals(
        token.toByteString(),
        row.getCells(
                MetadataTableAdminDao.CF_INITIAL_TOKEN,
                ByteStringRange.serializeToByteString(token.getPartition()))
            .get(0)
            .getValue());
  }

  @Test
  public void mutateRowWithHardTimeoutErrorHandling()
      throws ExecutionException, InterruptedException, TimeoutException {
    BigtableDataClient mockClient = Mockito.mock(BigtableDataClient.class);
    MetadataTableDao daoWithMock =
        new MetadataTableDao(mockClient, "test-table", ByteString.copyFromUtf8("test"));
    ApiFuture<Void> mockFuture = mock(ApiFuture.class);
    when(mockClient.mutateRowAsync(any())).thenReturn(mockFuture);

    when(mockFuture.get(40, TimeUnit.SECONDS))
        .thenThrow(TimeoutException.class)
        .thenThrow(InterruptedException.class)
        .thenThrow(ExecutionException.class);
    assertThrows(
        RuntimeException.class,
        () -> daoWithMock.mutateRowWithHardTimeout(RowMutation.create("test", "test").deleteRow()));
    assertThrows(
        RuntimeException.class,
        () -> daoWithMock.mutateRowWithHardTimeout(RowMutation.create("test", "test").deleteRow()));
    assertThrows(
        RuntimeException.class,
        () -> daoWithMock.mutateRowWithHardTimeout(RowMutation.create("test", "test").deleteRow()));
  }
}
