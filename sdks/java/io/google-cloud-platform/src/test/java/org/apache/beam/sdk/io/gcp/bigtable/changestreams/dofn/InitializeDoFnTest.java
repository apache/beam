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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.ExistingPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.InitialPipelineState;
import org.apache.beam.sdk.transforms.DoFn;
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
public class InitializeDoFnTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  @Mock private DaoFactory daoFactory;
  @Mock private transient MetadataTableAdminDao metadataTableAdminDao;
  private transient MetadataTableDao metadataTableDao;
  @Mock private DoFn.OutputReceiver<InitialPipelineState> outputReceiver;
  private final String tableId = "table";

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
  public void setUp() throws IOException {
    String changeStreamName = "changeStreamName";
    metadataTableAdminDao =
        spy(new MetadataTableAdminDao(adminClient, null, changeStreamName, tableId));
    metadataTableAdminDao.createMetadataTable();
    metadataTableDao =
        new MetadataTableDao(
            dataClient, tableId, metadataTableAdminDao.getChangeStreamNamePrefix());
    when(daoFactory.getMetadataTableDao()).thenReturn(metadataTableDao);
    when(daoFactory.getChangeStreamName()).thenReturn(changeStreamName);
  }

  @Test
  public void testInitializeDefault() throws IOException {
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(
            daoFactory, startTime, BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    initializeDoFn.processElement(outputReceiver);
    verify(outputReceiver, times(1)).output(new InitialPipelineState(startTime, false));
  }

  @Test
  public void testInitializeStopWithExistingPipeline() throws IOException {
    metadataTableDao.updateDetectNewPartitionWatermark(Instant.now());
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(
            daoFactory, startTime, BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    initializeDoFn.processElement(outputReceiver);
    verify(outputReceiver, never()).output(any());
  }

  @Test
  public void testInitializeStopWithoutDNP() throws IOException {
    // DNP row doesn't exist, so we don't need to stop the pipeline. But some random data row with
    // the same prefix exists. We want to make sure we clean it up even in "STOP" option.
    dataClient.mutateRow(
        RowMutation.create(
                tableId,
                metadataTableAdminDao
                    .getChangeStreamNamePrefix()
                    .concat(ByteString.copyFromUtf8("existing_row")))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT, 123));
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(
            daoFactory, startTime, BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    initializeDoFn.processElement(outputReceiver);
    verify(outputReceiver, times(1)).output(new InitialPipelineState(startTime, false));
    assertNull(dataClient.readRow(tableId, metadataTableAdminDao.getChangeStreamNamePrefix()));
  }

  @Test
  public void testInitializeResumeWithoutDNP() throws IOException {
    dataClient.mutateRow(
        RowMutation.create(
                tableId,
                metadataTableAdminDao
                    .getChangeStreamNamePrefix()
                    .concat(ByteString.copyFromUtf8("existing_row")))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT, 123));
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(daoFactory, startTime, BigtableIO.ExistingPipelineOptions.RESUME_OR_NEW);
    initializeDoFn.processElement(outputReceiver);
    // We want to resume but there's no DNP row, so we resume from the startTime provided.
    verify(outputReceiver, times(1)).output(new InitialPipelineState(startTime, false));
  }

  @Test
  public void testInitializeResumeWithDNP() throws IOException {
    Instant resumeTime = Instant.now().minus(Duration.standardSeconds(10000));
    metadataTableDao.updateDetectNewPartitionWatermark(resumeTime);
    dataClient.mutateRow(
        RowMutation.create(
                tableId,
                metadataTableAdminDao
                    .getChangeStreamNamePrefix()
                    .concat(ByteString.copyFromUtf8("existing_row")))
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT, 123));
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(daoFactory, startTime, BigtableIO.ExistingPipelineOptions.RESUME_OR_NEW);
    initializeDoFn.processElement(outputReceiver);
    verify(outputReceiver, times(1)).output(new InitialPipelineState(resumeTime, true));
    assertNull(dataClient.readRow(tableId, metadataTableAdminDao.getChangeStreamNamePrefix()));
  }

  @Test
  public void testInitializeSkipCleanupWithoutDNP() throws IOException {
    ByteString metadataRowKey =
        metadataTableAdminDao
            .getChangeStreamNamePrefix()
            .concat(ByteString.copyFromUtf8("existing_row"));
    dataClient.mutateRow(
        RowMutation.create(tableId, metadataRowKey)
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT, 123));
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(daoFactory, startTime, ExistingPipelineOptions.SKIP_CLEANUP);
    initializeDoFn.processElement(outputReceiver);
    // Skip cleanup will always resume from startTime
    verify(outputReceiver, times(1)).output(new InitialPipelineState(startTime, false));
    // Existing metadata shouldn't be cleaned up
    assertNotNull(dataClient.readRow(tableId, metadataRowKey));
  }

  @Test
  public void testInitializeSkipCleanupWithDNP() throws IOException {
    Instant resumeTime = Instant.now().minus(Duration.standardSeconds(10000));
    metadataTableDao.updateDetectNewPartitionWatermark(resumeTime);
    ByteString metadataRowKey =
        metadataTableAdminDao
            .getChangeStreamNamePrefix()
            .concat(ByteString.copyFromUtf8("existing_row"));
    dataClient.mutateRow(
        RowMutation.create(tableId, metadataRowKey)
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT, 123));
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn =
        new InitializeDoFn(daoFactory, startTime, ExistingPipelineOptions.SKIP_CLEANUP);
    initializeDoFn.processElement(outputReceiver);
    // We don't want the pipeline to resume to avoid duplicates
    verify(outputReceiver, never()).output(any());
    // Existing metadata shouldn't be cleaned up
    assertNotNull(dataClient.readRow(tableId, metadataRowKey));
  }
}
