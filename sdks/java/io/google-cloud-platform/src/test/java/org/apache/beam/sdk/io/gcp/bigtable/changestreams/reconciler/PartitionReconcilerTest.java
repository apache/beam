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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class PartitionReconcilerTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  private static final long MORE_THAN_FIVE_MINUTES_MILLI = 5 * 60 * 1000L + 1L;

  private MetadataTableDao metadataTableDao;

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
  }

  // [A-B) is missing because
  // [A-B) and [A-C) should merge. [A-B) received the CloseStream merge, immediately after,
  // and before [A-C) received the CloseStream merge, [A-B) and [A-C) split again. [A-C) never
  // needed to merge.
  @Test
  public void testNewMissingMergePartitionIsNotReconciled() {
    // No reconciliation should happen because the missing partition hasn't been waiting for more
    // than 5 minutes.
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));
    // Since [A-B) received the CloseStream merge, there should be an NewPartitions row for [A-C)
    // with [A-B) as a parent.
    partitionReconciler.addNewPartition(partitionAC, ByteString.copyFromUtf8("FakeRowKeyForAC"));
    HashMap<ByteStringRange, Set<ByteString>> partitionsToReconcile =
        partitionReconciler.getPartitionsToReconcile();
    assertEquals(0, partitionsToReconcile.size());
  }

  @Test
  public void testLongMissingMergePartitionIsReconciled() {
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");

    // Artificially create that partitionAB has been missing for more than 5 minutes.
    HashMap<ByteStringRange, Long> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(
        partitionAB, Instant.now().getMillis() - MORE_THAN_FIVE_MINUTES_MILLI);
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));
    // Since [A-B) received the CloseStream merge, there should be an NewPartitions row for [A-C)
    // with [A-B) as a parent.
    partitionReconciler.addNewPartition(partitionAC, ByteString.copyFromUtf8("FakeRowKeyForAC"));
    HashMap<ByteStringRange, Set<ByteString>> partitionsToReconcile =
        partitionReconciler.getPartitionsToReconcile();
    assertEquals(partitionsToReconcile.keySet(), Collections.singleton(partitionAB));
    assertEquals(
        partitionsToReconcile.get(partitionAB),
        Collections.singleton(ByteString.copyFromUtf8("FakeRowKeyForAC")));
  }

  // [A-B) merges into [A-C)
  // [B-C) merges into [A-D)
  // [C-D) merges into [A-D)
  @Test
  public void testMismatchedMergePartitionIsReconciled() {
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");

    // Artificially create that partitionAD has been missing for more than 5 minutes.
    HashMap<ByteStringRange, Long> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(
        partitionAD, Instant.now().getMillis() - MORE_THAN_FIVE_MINUTES_MILLI);
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao);
    // We are not missing A-B, B-C, C-D, from the metadata table view, we are simply missing A-D.
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAD));
    partitionReconciler.addNewPartition(partitionAC, ByteString.copyFromUtf8("FakeRowKeyForAC"));
    partitionReconciler.addNewPartition(partitionAD, ByteString.copyFromUtf8("FakeRowKeyForAD"));
    HashMap<ByteStringRange, Set<ByteString>> partitionsToReconcile =
        partitionReconciler.getPartitionsToReconcile();
    assertEquals(partitionsToReconcile.keySet(), Collections.singleton(partitionAD));
    assertEquals(
        partitionsToReconcile.get(partitionAD),
        new HashSet<>(
            Arrays.asList(
                ByteString.copyFromUtf8("FakeRowKeyForAC"),
                ByteString.copyFromUtf8("FakeRowKeyForAD"))));
  }

  // [A-B) merges into [A-D)
  // [B-D) splits into [A-C) and [C-D)
  // We're missing [A-C).
  // [A-B) and [B-D) were supposed to merge into [A-D)
  // Before [B-D) received the CloseStream merge, [A-D) actually splits into [A-C) and [C-D) which
  // is why [B-D) received CloseStream split into [A-C) and [C-D)
  // Now we need to merge [A-B) and [B-D) into [A-C). [C-D) part of the split was successful on its
  // own.
  @Test
  public void testMismatchedMergeSplitPartitionIsReconciled() {
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");

    // Artificially create that partitionAC has been missing for more than 5 minutes.
    HashMap<ByteStringRange, Long> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(
        partitionAC, Instant.now().getMillis() - MORE_THAN_FIVE_MINUTES_MILLI);
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao);
    // We are not missing A-B, B-D, from the metadata table view, we are simply missing A-C.
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAC));
    // A-B tried to merge into A-D
    partitionReconciler.addNewPartition(partitionAD, ByteString.copyFromUtf8("FakeRowKeyForAD"));
    // B-D tried to split/merge into A-C
    partitionReconciler.addNewPartition(partitionAC, ByteString.copyFromUtf8("FakeRowKeyForAC"));
    HashMap<ByteStringRange, Set<ByteString>> partitionsToReconcile =
        partitionReconciler.getPartitionsToReconcile();
    assertEquals(partitionsToReconcile.keySet(), Collections.singleton(partitionAC));
    assertEquals(
        partitionsToReconcile.get(partitionAC),
        new HashSet<>(
            Arrays.asList(
                ByteString.copyFromUtf8("FakeRowKeyForAD"),
                ByteString.copyFromUtf8("FakeRowKeyForAC"))));
  }
}
