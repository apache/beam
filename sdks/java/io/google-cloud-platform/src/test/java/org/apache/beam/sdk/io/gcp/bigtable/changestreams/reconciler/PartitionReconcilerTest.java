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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionReconcilerTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  private static final Duration MISSING_SHORT_PERIOD = Duration.standardSeconds(2 * 60 + 1);
  private static final Duration MISSING_LONG_PERIOD = Duration.standardSeconds(20 * 60 + 1);

  private MetadataTableDao metadataTableDao;

  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;

  private Instant lowWatermark;
  private Instant startTime;

  @Mock private ChangeStreamMetrics metrics;

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
    lowWatermark = Instant.now();
    // We set startTime to 30 min prior so if we reconcile without token, we use don't use an hour
    // prior but the startTime.
    startTime = lowWatermark.minus(Duration.standardMinutes(30));
  }

  // [A-B) is missing because
  // [A-B) and [A-C) should merge. [A-B) received the CloseStream merge, immediately after,
  // and before [A-C) received the CloseStream merge, [A-B) and [A-C) split again. [A-C) never
  // needed to merge.
  @Test
  public void testNewMissingMergePartitionIsNotReconciled() {
    // No reconciliation should happen because the missing partition hasn't been waiting for more
    // than 1 minute.
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken tokenAB = ChangeStreamContinuationToken.create(partitionAB, "AB");
    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));
    // Since [A-B) received the CloseStream merge, there should be an NewPartitions row for [A-C)
    // with [A-B) as a parent.
    NewPartition newPartitionAC =
        new NewPartition(partitionAC, Collections.singletonList(tokenAB), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAC);
    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);
    assertEquals(0, reconciledPartitions.size());
  }

  // [A-B) merging into [A-C)
  // [B-C) did not need to merge into [A-C)
  // Reconciler should, after 1 minute, output partition [A-B) with continuation token.
  @Test
  public void testLongMissingMergePartitionIsReconciled() {
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ChangeStreamContinuationToken tokenAB = ChangeStreamContinuationToken.create(partitionAB, "AB");

    // Artificially create that partitionAB has been missing for more than 1 minute.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAB, Instant.now().minus(MISSING_SHORT_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));
    // Since [A-B) received the CloseStream merge, there should be an NewPartitions row for [A-C)
    // with [A-B) as a parent.
    NewPartition newPartitionAC =
        new NewPartition(partitionAC, Collections.singletonList(tokenAB), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAC);
    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);

    PartitionRecord expectedRecord =
        new PartitionRecord(
            partitionAB,
            Collections.singletonList(tokenAB),
            lowWatermark,
            Collections.singletonList(newPartitionAC));
    assertEquals(1, reconciledPartitions.size());
    assertEquals(expectedRecord, reconciledPartitions.get(0));
  }

  // [A-B) merges into [A-C)
  // [B-C) merges into [A-D)
  // [C-D) merges into [A-D)
  @Test
  public void testMismatchedMergePartitionIsReconciled() {
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");

    ChangeStreamContinuationToken tokenAB =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "B"), "AB");
    ChangeStreamContinuationToken tokenBC =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "BC");
    ChangeStreamContinuationToken tokenCD =
        ChangeStreamContinuationToken.create(ByteStringRange.create("C", "D"), "CD");
    // Artificially create that partitionAD has been missing for more than 10 minutes.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAD, Instant.now().minus(MISSING_LONG_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    // We are not missing A-B, B-C, C-D, from the metadata table view, we are simply missing A-D.
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAD));
    // AB merging into AC
    NewPartition newPartitionAC =
        new NewPartition(partitionAC, Collections.singletonList(tokenAB), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAC);
    // BC and CD merging into AD
    NewPartition newPartitionAD =
        new NewPartition(partitionAD, Arrays.asList(tokenBC, tokenCD), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAD);

    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);
    // As part of reconciling we split up the parents into their own NewPartition, so they can be
    // cleaned up separately.
    NewPartition newPartitionADWithBC =
        new NewPartition(
            partitionAD, Collections.singletonList(tokenBC), newPartitionAD.getLowWatermark());
    NewPartition newPartitionADWithCD =
        new NewPartition(
            partitionAD, Collections.singletonList(tokenCD), newPartitionAD.getLowWatermark());
    PartitionRecord expectedRecord =
        new PartitionRecord(
            partitionAD,
            Arrays.asList(tokenAB, tokenBC, tokenCD),
            lowWatermark,
            Arrays.asList(newPartitionAC, newPartitionADWithBC, newPartitionADWithCD));
    assertEquals(1, reconciledPartitions.size());
    assertEquals(expectedRecord, reconciledPartitions.get(0));
  }

  // [A-B) merges into [A-D)
  // [B-D) splits into [A-C) and [C-D)
  // We're missing [A-C).
  // [A-B) and [B-D) were supposed to merge into [A-D)
  // Before [B-D) received the CloseStream merge, [A-D) actually splits into [A-C) and [C-D) which
  // is why [B-D) received CloseStream split into [A-C) and [C-D). Note that the tokens in the
  // CloseStream are for BC and CD. So when performing reconciliation, we're reconciling BC, not BD.
  // Reconciler should merge [A-B) and [B-C) into [A-C). [C-D) part of the split was successful.
  @Test
  public void testMismatchedMergeSplitPartitionIsReconciled() {
    ByteStringRange partitionAC = ByteStringRange.create("A", "C");
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");
    ChangeStreamContinuationToken tokenAB =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "B"), "AB");
    ChangeStreamContinuationToken tokenBC =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "C"), "BC");

    // Artificially create that partitionAC has been missing for more than 10 minutes.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAC, Instant.now().minus(MISSING_LONG_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    // We are not missing A-B, B-C, from the metadata table view, we are simply missing A-C.
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAC));
    // A-B tried to merge into A-D
    NewPartition newPartitionAD =
        new NewPartition(partitionAD, Collections.singletonList(tokenAB), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAD);
    // B-C tried to split/merge into A-C
    NewPartition newPartitionAC =
        new NewPartition(partitionAC, Collections.singletonList(tokenBC), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAC);
    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);

    PartitionRecord expectedRecord =
        new PartitionRecord(
            partitionAC,
            Arrays.asList(tokenAB, tokenBC),
            lowWatermark,
            Arrays.asList(newPartitionAD, newPartitionAC));
    assertEquals(1, reconciledPartitions.size());
    assertEquals(expectedRecord, reconciledPartitions.get(0));
  }

  // AB is missing without any tokens for more than 10 minutes, reconciler will restart the
  // partition with low watermark - 1 hour.
  @Test
  public void testMissingPartitionWithoutToken() {
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");

    // Artificially create that partitionAB has been missing for more than 10 minutes.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAB, Instant.now().minus(MISSING_LONG_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));

    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);

    PartitionRecord expectedRecord =
        new PartitionRecord(partitionAB, startTime, lowWatermark, Collections.emptyList());
    assertEquals(1, reconciledPartitions.size());
    assertEquals(expectedRecord, reconciledPartitions.get(0));
    assertTrue(metadataTableDao.readDetectNewPartitionMissingPartitions().isEmpty());
  }

  // We're missing AD, but we only have partition AB and CD. We should reconcile by outputting AB
  // and CD and then create a new partition for BC with start_time = low watermark - 1 hour
  @Test
  public void testMissingPartitionWithSomeToken() {
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");
    ByteStringRange partitionBC = ByteStringRange.create("B", "C");
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ChangeStreamContinuationToken tokenAB = ChangeStreamContinuationToken.create(partitionAB, "AB");
    ByteStringRange partitionCD = ByteStringRange.create("C", "D");
    ChangeStreamContinuationToken tokenCD = ChangeStreamContinuationToken.create(partitionCD, "CD");

    // Artificially create that partitionAD has been missing for more than 10 minutes.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAD, Instant.now().minus(MISSING_LONG_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAD));

    NewPartition newPartitionAD =
        new NewPartition(partitionAD, Arrays.asList(tokenAB, tokenCD), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAD);

    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);

    // As part of reconciling we split up the parents into their own NewPartition, so they can be
    // cleaned up separately.
    NewPartition newPartitionADWithAB =
        new NewPartition(
            partitionAD, Collections.singletonList(tokenAB), newPartitionAD.getLowWatermark());
    NewPartition newPartitionADWithCD =
        new NewPartition(
            partitionAD, Collections.singletonList(tokenCD), newPartitionAD.getLowWatermark());
    PartitionRecord expectedRecordAB =
        new PartitionRecord(
            partitionAB,
            Collections.singletonList(tokenAB),
            lowWatermark,
            Collections.singletonList(newPartitionADWithAB));
    PartitionRecord expectedRecordCD =
        new PartitionRecord(
            partitionCD,
            Collections.singletonList(tokenCD),
            lowWatermark,
            Collections.singletonList(newPartitionADWithCD));
    PartitionRecord expectedRecordBC =
        new PartitionRecord(partitionBC, startTime, lowWatermark, Collections.emptyList());
    assertEquals(3, reconciledPartitions.size());

    assertThat(
        reconciledPartitions,
        containsInAnyOrder(
            Arrays.asList(expectedRecordAB, expectedRecordBC, expectedRecordCD).toArray()));
  }

  // A partition that's missing for more than 10 minutes (DNP could have not run for more than 10
  // minutes) has continuation token should get reconciled with token.
  @Test
  public void testMissingPartitionWithTokenMoreThan10Minutes() {
    ByteStringRange partitionAD = ByteStringRange.create("A", "D");
    ByteStringRange partitionAB = ByteStringRange.create("A", "B");
    ChangeStreamContinuationToken tokenAB = ChangeStreamContinuationToken.create(partitionAB, "AB");

    // Artificially create that partitionAB has been missing for more than 10 minutes.
    HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
    missingPartitionDurations.put(partitionAB, Instant.now().minus(MISSING_LONG_PERIOD));
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    PartitionReconciler partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    partitionReconciler.addMissingPartitions(Collections.singletonList(partitionAB));

    NewPartition newPartitionAD =
        new NewPartition(partitionAD, Collections.singletonList(tokenAB), Instant.now());
    partitionReconciler.addIncompleteNewPartitions(newPartitionAD);

    List<PartitionRecord> reconciledPartitions =
        partitionReconciler.getPartitionsToReconcile(lowWatermark, startTime);
    assertTrue(metadataTableDao.readDetectNewPartitionMissingPartitions().isEmpty());
    assertEquals(1, reconciledPartitions.size());

    NewPartition newPartitionADWithAB =
        new NewPartition(
            partitionAD, Collections.singletonList(tokenAB), newPartitionAD.getLowWatermark());
    PartitionRecord expectedRecordAB =
        new PartitionRecord(
            partitionAB,
            Collections.singletonList(tokenAB),
            lowWatermark,
            Collections.singletonList(newPartitionADWithAB));
    assertEquals(reconciledPartitions, Collections.singletonList(expectedRecordAB));
  }
}
