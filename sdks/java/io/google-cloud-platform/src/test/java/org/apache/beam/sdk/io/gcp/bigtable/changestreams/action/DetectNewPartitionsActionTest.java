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
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
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
}
