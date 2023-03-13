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

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataTableDaoTest {

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
        changeStreamContinuationToken1, parentPartition, lowWatermark);
    metadataTableDao.writeNewPartition(
        changeStreamContinuationToken2, parentPartition, lowWatermark);

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
}
