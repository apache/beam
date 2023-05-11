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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
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
public class GenerateInitialPartitionsActionTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  @Mock private RestrictionTracker<OffsetRange, Long> tracker;
  @Mock private ChangeStreamDao changeStreamDao;
  @Mock private ChangeStreamMetrics metrics;
  @Mock private OutputReceiver<PartitionRecord> receiver;

  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private Instant startTime;
  private Instant endTime;
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
  }

  @Before
  public void setUp() throws Exception {
    String changeStreamId = UniqueIdGenerator.generateRowKeyPrefix();
    MetadataTableAdminDao metadataTableAdminDao =
        new MetadataTableAdminDao(
            adminClient, null, changeStreamId, MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME);
    metadataTableAdminDao.createMetadataTable();
    startTime = Instant.now();
    endTime = startTime.plus(Duration.standardSeconds(10));
    watermarkEstimator = new WatermarkEstimators.Manual(startTime);
  }

  @Test
  public void testGenerateInitialPartitionsFromStartTime() {
    when(tracker.tryClaim(0L)).thenReturn(true);

    ByteStringRange partition1 = ByteStringRange.create("", "b");
    ByteStringRange partition2 = ByteStringRange.create("b", "");
    List<ByteStringRange> partitionRecordList = Arrays.asList(partition1, partition2);
    when(changeStreamDao.generateInitialChangeStreamPartitions()).thenReturn(partitionRecordList);

    GenerateInitialPartitionsAction generateInitialPartitionsAction =
        new GenerateInitialPartitionsAction(metrics, changeStreamDao, endTime);
    assertEquals(
        ProcessContinuation.resume(),
        generateInitialPartitionsAction.run(receiver, tracker, watermarkEstimator, startTime));
    verify(receiver, times(2))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<PartitionRecord> actualPartitions = partitionRecordArgumentCaptor.getAllValues();

    assertEquals(partition1, actualPartitions.get(0).getPartition());
    assertEquals(startTime, actualPartitions.get(0).getStartTime());
    assertEquals(partition2, actualPartitions.get(1).getPartition());
    assertEquals(startTime, actualPartitions.get(1).getStartTime());
  }
}
