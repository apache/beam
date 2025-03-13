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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.it;

import com.google.api.gax.batching.Batcher;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.UpdateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.BigtableChangeStreamTestOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end tests of Bigtable Change Stream. */
@SuppressWarnings("FutureReturnValueIgnored")
@RunWith(JUnit4.class)
public class BigtableChangeStreamIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamIT.class);
  private static final String COLUMN_FAMILY1 = "CF";
  private static final String COLUMN_FAMILY2 = "CF2";
  private static final String COLUMN_QUALIFIER = "CQ";
  private static String projectId;
  private static String instanceId;
  private static String tableId;
  private static String appProfileId;
  private static String metadataTableId;
  private static BigtableTableAdminClient adminClient;
  private static BigtableDataClient dataClient;
  private static BigtableClientIntegrationTestOverride bigtableClientOverride;
  private static Batcher<RowMutationEntry, Void> mutationBatcher;
  private static BigtableChangeStreamTestOptions options;
  private transient TestPipeline pipeline;

  @BeforeClass
  public static void beforeClass() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(BigtableChangeStreamTestOptions.class);
    LOG.info("Pipeline options: {}", options);
    projectId = options.as(GcpOptions.class).getProject();
    instanceId = options.getBigtableChangeStreamInstanceId();

    long randomId = Instant.now().getMillis();
    tableId = "beam-change-stream-test-" + randomId;
    metadataTableId = "beam-change-stream-test-md-" + randomId;
    appProfileId = "default";

    bigtableClientOverride = new BigtableClientIntegrationTestOverride();
    LOG.info(bigtableClientOverride.toString());

    BigtableDataSettings.Builder dataSettingsBuilder = BigtableDataSettings.newBuilder();
    BigtableTableAdminSettings.Builder tableAdminSettingsBuilder =
        BigtableTableAdminSettings.newBuilder();
    dataSettingsBuilder.setProjectId(projectId);
    tableAdminSettingsBuilder.setProjectId(projectId);
    dataSettingsBuilder.setInstanceId(instanceId);
    tableAdminSettingsBuilder.setInstanceId(instanceId);
    dataSettingsBuilder.setAppProfileId(appProfileId);
    // TODO: Remove this later. But for now, disable direct path.
    dataSettingsBuilder
        .stubSettings()
        .setTransportChannelProvider(
            EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
                .setAttemptDirectPath(false)
                .build());

    bigtableClientOverride.updateDataClientSettings(dataSettingsBuilder);
    bigtableClientOverride.updateTableAdminClientSettings(tableAdminSettingsBuilder);

    // These clients are used to modify the table and write to it
    dataClient = BigtableDataClient.create(dataSettingsBuilder.build());
    adminClient = BigtableTableAdminClient.create(tableAdminSettingsBuilder.build());

    // Create change stream enabled table
    adminClient.createTable(
        CreateTableRequest.of(tableId)
            .addChangeStreamRetention(org.threeten.bp.Duration.ofDays(1))
            .addFamily(COLUMN_FAMILY1)
            .addFamily(COLUMN_FAMILY2));

    mutationBatcher = dataClient.newBulkMutationBatcher(tableId);
  }

  @Before
  public void before() {
    pipeline = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
  }

  @AfterClass
  public static void afterClass() {
    if (adminClient != null) {
      if (adminClient.exists(tableId)) {
        adminClient.updateTable(UpdateTableRequest.of(tableId).disableChangeStreamRetention());
        adminClient.deleteTable(tableId);
        adminClient.deleteTable(metadataTableId);
      }
      adminClient.close();
    }
    if (dataClient != null) {
      dataClient.close();
    }
  }

  @Test
  public void testReadBigtableChangeStream() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    String rowKey = "rowKeySetCell";
    RowMutationEntry setCellEntry =
        RowMutationEntry.create(rowKey).setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, "cell value 1");
    mutationBatcher.add(setCellEntry);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream).containsInAnyOrder(setCellEntry.toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDeleteRow() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    String rowKeyToDelete = "rowKeyToDelete";
    RowMutationEntry setCellMutationToDelete =
        RowMutationEntry.create(rowKeyToDelete)
            .setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, "cell value 1");
    RowMutationEntry deleteRowMutation = RowMutationEntry.create(rowKeyToDelete).deleteRow();
    mutationBatcher.add(setCellMutationToDelete);
    mutationBatcher.flush();
    mutationBatcher.add(deleteRowMutation);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream)
        .containsInAnyOrder(
            setCellMutationToDelete.toProto(),
            // Delete row becomes one deleteFamily per family
            RowMutationEntry.create(rowKeyToDelete)
                .deleteFamily(COLUMN_FAMILY1)
                .deleteFamily(COLUMN_FAMILY2)
                .toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDeleteColumnFamily() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    String cellValue = "cell value 1";
    String rowKeyMultiFamily = "rowKeyMultiFamily";
    RowMutationEntry setCells =
        RowMutationEntry.create(rowKeyMultiFamily)
            .setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, cellValue)
            .setCell(COLUMN_FAMILY2, COLUMN_QUALIFIER, cellValue);
    mutationBatcher.add(setCells);
    mutationBatcher.flush();
    RowMutationEntry deleteCF2 =
        RowMutationEntry.create(rowKeyMultiFamily).deleteFamily(COLUMN_FAMILY2);
    mutationBatcher.add(deleteCF2);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream).containsInAnyOrder(setCells.toProto(), deleteCF2.toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDeleteCell() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    String cellValue = "cell value 1";
    String rowKeyMultiCell = "rowKeyMultiCell";
    RowMutationEntry setCells =
        RowMutationEntry.create(rowKeyMultiCell)
            .setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, cellValue)
            .setCell(COLUMN_FAMILY1, "CQ2", cellValue);
    mutationBatcher.add(setCells);
    mutationBatcher.flush();
    RowMutationEntry deleteCQ2 =
        RowMutationEntry.create(rowKeyMultiCell)
            // need to set timestamp range to make change stream output match
            .deleteCells(
                COLUMN_FAMILY1,
                ByteString.copyFromUtf8("CQ2"),
                Range.TimestampRange.create(
                    startTime.getMillis() * 1000,
                    startTime.plus(Duration.standardMinutes(2)).getMillis() * 1000));
    mutationBatcher.add(deleteCQ2);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream).containsInAnyOrder(setCells.toProto(), deleteCQ2.toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testComplexMutation() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    String rowKey = "rowKeyComplex";
    // We'll delete this in the next mutation
    RowMutationEntry setCell =
        RowMutationEntry.create(rowKey).setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, "cell value 1");
    mutationBatcher.add(setCell);
    mutationBatcher.flush();
    RowMutationEntry complexMutation =
        RowMutationEntry.create(rowKey)
            .setCell(COLUMN_FAMILY1, "CQ2", "cell value 2")
            .setCell(COLUMN_FAMILY1, "CQ3", "cell value 3")
            // need to set timestamp range to make change stream output match
            .deleteCells(
                COLUMN_FAMILY1,
                ByteString.copyFromUtf8(COLUMN_QUALIFIER),
                Range.TimestampRange.create(
                    startTime.getMillis() * 1000,
                    startTime.plus(Duration.standardMinutes(2)).getMillis() * 1000));
    mutationBatcher.add(complexMutation);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream).containsInAnyOrder(setCell.toProto(), complexMutation.toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLargeMutation() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    // test set cell w size > 1MB so it triggers chunking
    char[] chars = new char[1024 * 1500];
    Arrays.fill(chars, '\u200B'); // zero-width space
    String largeString = String.valueOf(chars);
    String rowKeyLargeCell = "rowKeyLargeCell";
    RowMutationEntry setLargeCell =
        RowMutationEntry.create(rowKeyLargeCell)
            .setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, largeString);
    mutationBatcher.add(setLargeCell);
    mutationBatcher.flush();
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream).containsInAnyOrder(setLargeCell.toProto());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testManyMutations() throws InterruptedException {
    // Sleep to ensure clock discrepancy between client and server don't affect the start time.
    Thread.sleep(500);
    Instant startTime = Instant.now();
    Thread.sleep(500);
    // test set cell w size > 1MB so it triggers chunking
    char[] chars = new char[1024 * 3];
    Arrays.fill(chars, '\u200B'); // zero-width space
    String largeString = String.valueOf(chars);

    ImmutableList.Builder<RowMutationEntry> originalWrites = ImmutableList.builder();
    for (int i = 0; i < 100; ++i) {
      String rowKey = "rowKey" + i;
      // SetCell.
      RowMutationEntry setLargeCell =
          RowMutationEntry.create(rowKey).setCell(COLUMN_FAMILY1, COLUMN_QUALIFIER, largeString);
      // DeleteFamily.
      RowMutationEntry deleteFamily = RowMutationEntry.create(rowKey).deleteFamily(COLUMN_FAMILY1);
      // DeleteCells.
      RowMutationEntry deleteCells =
          RowMutationEntry.create(rowKey)
              // need to set timestamp range to make change stream output match
              .deleteCells(
                  COLUMN_FAMILY1,
                  ByteString.copyFromUtf8(COLUMN_QUALIFIER),
                  Range.TimestampRange.create(
                      startTime.getMillis() * 1000,
                      startTime.plus(Duration.standardMinutes(2)).getMillis() * 1000));
      // Apply the mutations.
      originalWrites.add(setLargeCell);
      mutationBatcher.add(setLargeCell);
      mutationBatcher.flush();

      originalWrites.add(deleteFamily);
      mutationBatcher.add(deleteFamily);
      mutationBatcher.flush();

      originalWrites.add(deleteCells);
      mutationBatcher.add(deleteCells);
      mutationBatcher.flush();
    }
    Instant endTime = Instant.now().plus(Duration.standardSeconds(10));

    PCollection<MutateRowsRequest.Entry> changeStream = buildPipeline(startTime, endTime);
    PAssert.that(changeStream)
        .containsInAnyOrder(
            originalWrites.build().stream()
                .map(RowMutationEntry::toProto)
                .collect(Collectors.toList()));
    pipeline.run().waitUntilFinish();
  }

  private PCollection<MutateRowsRequest.Entry> buildPipeline(Instant startTime, Instant endTime) {
    return pipeline
        .apply(
            BigtableTestUtils.buildTestPipelineInput(
                projectId,
                instanceId,
                tableId,
                appProfileId,
                metadataTableId,
                startTime,
                endTime,
                bigtableClientOverride))
        .apply(ParDo.of(new ConvertToEntry()));
  }

  private static class ConvertToEntry
      extends DoFn<KV<ByteString, ChangeStreamMutation>, MutateRowsRequest.Entry> {
    @ProcessElement
    public void processElement(
        @Element KV<ByteString, ChangeStreamMutation> element,
        OutputReceiver<MutateRowsRequest.Entry> out) {
      out.output(element.getValue().toRowMutationEntry().toProto());
    }
  }
}
