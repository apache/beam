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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.gax.rpc.ServerStream;
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
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
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
  public void testLockPartitionRace() throws InterruptedException {
    ByteStringRange partition = ByteStringRange.create("", "");
    ByteString rowKey = metadataTableDao.convertPartitionToStreamPartitionRowKey(partition);
    // Class to try to lock the partition in a separate thread.
    class LockPartition implements Runnable {
      final String id;
      boolean locked = false;

      LockPartition(String id) {
        this.id = id;
      }

      @Override
      public void run() {
        try {
          // Sleep for a random amount before trying to lock the partition to add variability to the
          // race.
          int sleep = (int) (Math.random() * 1000);
          Thread.sleep(sleep);
          if (metadataTableDao.lockPartition(partition, id)) {
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
  public void testReadStreamPartitionsWithWatermark() throws InvalidProtocolBufferException {
    ByteStringRange partitionWithWatermark = ByteStringRange.create("a", "");
    Instant watermark = Instant.now();
    metadataTableDao.updateWatermark(partitionWithWatermark, watermark, null);

    // This should only return rows where the watermark has been set, not rows that have been locked
    // but have not yet set the first watermark
    ServerStream<Row> rowsWithWatermark =
        metadataTableDao.readFromMdTableStreamPartitionsWithWatermark();
    ArrayList<Row> metadataRows = new ArrayList<>();
    for (Row row : rowsWithWatermark) {
      metadataRows.add(row);
    }
    assertEquals(1, metadataRows.size());
    Instant metadataWatermark = MetadataTableEncoder.parseWatermarkFromRow(metadataRows.get(0));
    assertEquals(watermark, metadataWatermark);
    ByteStringRange rowKeyResponse =
        metadataTableDao.convertStreamPartitionRowKeyToPartition(metadataRows.get(0).getKey());
    assertEquals(partitionWithWatermark, rowKeyResponse);
  }

  @Test
  public void testNewPartitionsWriteRead() throws InvalidProtocolBufferException {
    // This test a split of ["", "") to ["", "a") and ["a", "")
    ByteStringRange parentPartition = ByteStringRange.create("", "");
    ByteStringRange partition1 = ByteStringRange.create("", "a");
    ChangeStreamContinuationToken changeStreamContinuationToken1 =
        ChangeStreamContinuationToken.create(partition1, "tk1");
    ByteStringRange partition2 = ByteStringRange.create("a", "");
    ChangeStreamContinuationToken changeStreamContinuationToken2 =
        ChangeStreamContinuationToken.create(partition2, "tk2");

    Instant lowWatermark = Instant.now();
    metadataTableDao.writeNewPartition(
        partition1, changeStreamContinuationToken1, parentPartition, lowWatermark);
    metadataTableDao.writeNewPartition(
        partition2, changeStreamContinuationToken2, parentPartition, lowWatermark);

    ServerStream<Row> rows = metadataTableDao.readNewPartitions();
    int rowsCount = 0;
    boolean matchedPartition1 = false;
    boolean matchedPartition2 = false;
    for (Row row : rows) {
      rowsCount++;
      ByteString newPartitionPrefix =
          metadataTableDao
              .getChangeStreamNamePrefix()
              .concat(MetadataTableAdminDao.NEW_PARTITION_PREFIX);
      ByteStringRange partition =
          ByteStringRange.toByteStringRange(row.getKey().substring(newPartitionPrefix.size()));
      if (partition.equals(partition1)) {
        matchedPartition1 = true;
      } else if (partition.equals(partition2)) {
        matchedPartition2 = true;
      }
    }
    assertTrue(matchedPartition1);
    assertTrue(matchedPartition2);
    assertEquals(2, rowsCount);
  }

  @Test
  public void testUpdateAndReadWatermark() throws InvalidProtocolBufferException {
    ByteStringRange partition1 = ByteStringRange.create("a", "b");
    Instant watermark1 = Instant.now();
    metadataTableDao.updateWatermark(partition1, watermark1, null);
    ByteStringRange partition2 = ByteStringRange.create("b", "c");
    Instant watermark2 = Instant.now();
    metadataTableDao.updateWatermark(partition2, watermark2, null);

    ServerStream<Row> rows = metadataTableDao.readFromMdTableStreamPartitionsWithWatermark();
    int rowsCount = 0;
    boolean matchedPartition1 = false;
    boolean matchedPartition2 = false;
    for (Row row : rows) {
      rowsCount++;
      ByteStringRange partition =
          metadataTableDao.convertStreamPartitionRowKeyToPartition(row.getKey());
      if (partition.equals(partition1)) {
        assertEquals(watermark1, MetadataTableEncoder.parseWatermarkFromRow(row));
        matchedPartition1 = true;
      } else if (partition.equals(partition2)) {
        assertEquals(watermark2, MetadataTableEncoder.parseWatermarkFromRow(row));
        matchedPartition2 = true;
      }
    }
    assertEquals(2, rowsCount);
    assertTrue(matchedPartition1);
    assertTrue(matchedPartition2);
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
    assertEquals(token.getToken(), MetadataTableEncoder.getTokenFromRow(row));
    assertEquals(watermark, MetadataTableEncoder.parseWatermarkFromRow(row));
  }

  @Test
  public void readAndWriteValidMissingPartitionsDuration() {
    HashMap<ByteStringRange, Long> missingPartitionsDuration = new HashMap<>();
    missingPartitionsDuration.put(ByteStringRange.create("A", "B"), 100L);
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionsDuration);
    HashMap<ByteStringRange, Long> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void readAndWriteInvalidMissingPartitionsDuration() {
    HashMap<ByteStringRange, Long> missingPartitionsDuration = new HashMap<>();

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
    HashMap<ByteStringRange, Long> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }

  @Test
  public void readMissingPartitionsWithoutDNPRow() {
    HashMap<ByteStringRange, Long> missingPartitionsDuration = new HashMap<>();
    HashMap<ByteStringRange, Long> actualMissingPartitionsDuration =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    assertEquals(missingPartitionsDuration, actualMissingPartitionsDuration);
  }
}
