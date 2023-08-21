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
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
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
public class GenerateInitialPartitionsActionTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  @Mock private ChangeStreamDao changeStreamDao;
  @Mock private ChangeStreamMetrics metrics;
  @Mock private OutputReceiver<PartitionRecord> receiver;

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
    metadataTableAdminDao.cleanUpPrefix();
    startTime = Instant.now();
    endTime = startTime.plus(Duration.standardSeconds(10));
  }

  @Test
  public void testGenerateInitialPartitionsFromStartTime() {
    Range.ByteStringRange partition1 = Range.ByteStringRange.create("", "b");
    Range.ByteStringRange partition2 = Range.ByteStringRange.create("b", "");
    List<Range.ByteStringRange> partitionRecordList = Arrays.asList(partition1, partition2);
    when(changeStreamDao.generateInitialChangeStreamPartitions()).thenReturn(partitionRecordList);

    GenerateInitialPartitionsAction generateInitialPartitionsAction =
        new GenerateInitialPartitionsAction(metrics, changeStreamDao, endTime);
    generateInitialPartitionsAction.run(receiver, startTime);
    verify(receiver, times(2))
        .outputWithTimestamp(partitionRecordArgumentCaptor.capture(), eq(Instant.EPOCH));
    List<PartitionRecord> actualPartitions = partitionRecordArgumentCaptor.getAllValues();

    assertEquals(partition1, actualPartitions.get(0).getPartition());
    assertEquals(startTime, actualPartitions.get(0).getStartTime());
    assertEquals(partition2, actualPartitions.get(1).getPartition());
    assertEquals(startTime, actualPartitions.get(1).getStartTime());
  }
}
