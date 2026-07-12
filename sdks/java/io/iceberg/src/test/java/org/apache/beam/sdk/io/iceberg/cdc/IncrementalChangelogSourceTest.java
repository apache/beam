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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link IncrementalChangelogSource}. */
@RunWith(JUnit4.class)
public class IncrementalChangelogSourceTest {
  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final org.apache.iceberg.Schema EVENT_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              required(3, "event_time", Types.TimestampType.withoutZone())),
          ImmutableSet.of(1));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void boundedSnapshotRangeEmitsOnlyRequestedSnapshotsWithProjectedSchema()
      throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    commitAppend(table, "s1.parquet", records(1L, "one"));
    commitAppend(table, "s2.parquet", records(2L, "two"));
    commitAppend(table, "s3.parquet", records(3L, "three"));
    commitAppend(table, "s4.parquet", records(4L, "four"));
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    IcebergScanConfig scanConfig =
        baseConfigBuilder(table, tableId)
            .setKeepFields(ImmutableList.of("id"))
            .setFromSnapshotInclusive(snapshots.get(1).snapshotId())
            .setToSnapshot(snapshots.get(2).snapshotId())
            .build();
    Schema projectedSchema = Schema.builder().addInt64Field("id").build();

    PCollection<Row> rows = pipeline.apply(new IncrementalChangelogSource(scanConfig));

    assertThat(rows.isBounded(), equalTo(IsBounded.BOUNDED));
    assertEquals(projectedSchema, rows.getSchema());
    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(projectedSchema).addValue(2L).build(),
            Row.withSchema(projectedSchema).addValue(3L).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void metadataColumnsAreAppendedToProjectedRecord() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tablePropertiesV3());
    DataFile file1 =
        commitAppend(
            table,
            testName.getMethodName() + "file1.parquet",
            Arrays.asList(
                record(1L, "one"), record(2L, "two"), record(3L, "three"), record(4L, "four")));

    DataFile file2 =
        warehouse.writeRecords(
            testName.getMethodName() + "file2.parquet",
            table.schema(),
            PartitionSpec.unpartitioned(),
            null,
            ImmutableList.of(record(3L, "three_new"), record(4L, "four_new")));
    table.newOverwrite().deleteFile(file1).addFile(file2).commit();
    table.refresh();

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    long snap1Id = snapshots.get(0).snapshotId();
    long snap1Seq = snapshots.get(0).sequenceNumber();
    long file1Seq = snapshots.get(0).sequenceNumber();
    long snap1FirstRowId = snapshots.get(0).firstRowId();

    long snap2Id = snapshots.get(1).snapshotId();
    long snap2Seq = snapshots.get(1).sequenceNumber();
    long file2Seq = snapshots.get(1).sequenceNumber();
    long snap2FirstRowId = snapshots.get(1).firstRowId();

    IcebergScanConfig scanConfig =
        baseConfigBuilder(table, tableId)
            .setKeepFields(ImmutableList.of("id"))
            .setFromSnapshotInclusive(snapshots.get(0).snapshotId())
            .setToSnapshot(snapshots.get(1).snapshotId())
            .setMetadataColumns(
                ImmutableList.of(
                    IcebergCdcMetadataColumns.CHANGE_TYPE,
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID,
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER,
                    IcebergCdcMetadataColumns.ROW_ID,
                    IcebergCdcMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER))
            .build();
    Schema outputSchema =
        Schema.builder()
            .addInt64Field("id")
            .addStringField(IcebergCdcMetadataColumns.CHANGE_TYPE)
            .addInt64Field(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID)
            .addInt64Field(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER)
            .addNullableField(IcebergCdcMetadataColumns.ROW_ID, Schema.FieldType.INT64)
            .addNullableField(
                IcebergCdcMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER, Schema.FieldType.INT64)
            .build();

    PCollection<Row> rows = pipeline.apply(new IncrementalChangelogSource(scanConfig));

    assertEquals(outputSchema, rows.getSchema());
    PAssert.that(rows)
        .containsInAnyOrder(
            // snapshot 1: insert data file 1
            row(
                1L,
                ChangelogOperation.INSERT,
                snap1Id,
                snap1Seq,
                snap1FirstRowId,
                file1Seq,
                outputSchema),
            row(
                2L,
                ChangelogOperation.INSERT,
                snap1Id,
                snap1Seq,
                snap1FirstRowId + 1,
                file1Seq,
                outputSchema),
            row(
                3L,
                ChangelogOperation.INSERT,
                snap1Id,
                snap1Seq,
                snap1FirstRowId + 2,
                file1Seq,
                outputSchema),
            row(
                4L,
                ChangelogOperation.INSERT,
                snap1Id,
                snap1Seq,
                snap1FirstRowId + 3,
                file1Seq,
                outputSchema),
            // snapshot 2: delete data file 1
            row(
                1L,
                ChangelogOperation.DELETE,
                snap2Id,
                snap2Seq,
                snap1FirstRowId,
                file1Seq,
                outputSchema),
            row(
                2L,
                ChangelogOperation.DELETE,
                snap2Id,
                snap2Seq,
                snap1FirstRowId + 1,
                file1Seq,
                outputSchema),
            row(
                3L,
                ChangelogOperation.UPDATE_BEFORE,
                snap2Id,
                snap2Seq,
                snap1FirstRowId + 2,
                file1Seq,
                outputSchema),
            row(
                4L,
                ChangelogOperation.UPDATE_BEFORE,
                snap2Id,
                snap2Seq,
                snap1FirstRowId + 3,
                file1Seq,
                outputSchema),
            // snapshot 2: insert data file 2
            row(
                3L,
                ChangelogOperation.UPDATE_AFTER,
                snap2Id,
                snap2Seq,
                snap2FirstRowId,
                file2Seq,
                outputSchema),
            row(
                4L,
                ChangelogOperation.UPDATE_AFTER,
                snap2Id,
                snap2Seq,
                snap2FirstRowId + 1,
                file2Seq,
                outputSchema));

    pipeline.run().waitUntilFinish();
  }

  private Row row(
      long id,
      ChangelogOperation operation,
      long snapshotId,
      long snapshotSequence,
      long rowId,
      long lastUpdatedSequence,
      Schema outputSchema) {
    return Row.withSchema(outputSchema)
        .addValues(id, operation.name(), snapshotId, snapshotSequence, rowId, lastUpdatedSequence)
        .build();
  }

  @Test
  public void streamingSnapshotRangeTerminatesWithoutDuplicates() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    commitAppend(table, "s1.parquet", records(1L, "one"));
    commitAppend(table, "s2.parquet", records(2L, "two"));
    commitAppend(table, "s3.parquet", records(3L, "three"));

    IcebergScanConfig scanConfig =
        baseConfigBuilder(table, tableId)
            .setStreaming(true)
            .setStartingStrategy(StartingStrategy.EARLIEST)
            .setToSnapshot(table.currentSnapshot().snapshotId())
            .build();
    Schema rowSchema = IcebergUtils.icebergSchemaToBeamSchema(CDC_SCHEMA);

    PCollection<Row> rows = pipeline.apply(new IncrementalChangelogSource(scanConfig));

    assertThat(rows.isBounded(), equalTo(IsBounded.UNBOUNDED));
    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(rowSchema).addValues(1L, "one").build(),
            Row.withSchema(rowSchema).addValues(2L, "two").build(),
            Row.withSchema(rowSchema).addValues(3L, "three").build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void overwriteUpdatePairsAreResolvedWithinSnapshotWindow() throws Exception {
    TableIdentifier tableId = tableId();
    Table table =
        warehouse.createTable(
            tableId,
            CDC_SCHEMA,
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2", TableProperties.SPLIT_SIZE, "1"));
    DataFile oldFile =
        commitAppend(
            table, "old.parquet", ImmutableList.of(record(1L, "before"), record(2L, "same")));
    DataFile newFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-new.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "after"), record(2L, "same")));
    table.newOverwrite().deleteFile(oldFile).addFile(newFile).commit();
    table.refresh();

    IcebergScanConfig scanConfig =
        baseConfigBuilder(table, tableId)
            .setFromSnapshotInclusive(table.currentSnapshot().snapshotId())
            .setToSnapshot(table.currentSnapshot().snapshotId())
            .build();

    PCollection<String> changes =
        pipeline
            .apply(new IncrementalChangelogSource(scanConfig))
            .apply("Format Changes", ParDo.of(new FormatValueKindAndRow()));

    PAssert.that(changes).containsInAnyOrder("UPDATE_BEFORE:1:before", "UPDATE_AFTER:1:after");

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void watermarkColumnRestampsProjectedRows() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, EVENT_SCHEMA, null, tableProperties());
    java.time.Instant firstEventInstant =
        java.time.Instant.ofEpochMilli(System.currentTimeMillis() - 1_000L);
    java.time.Instant secondEventInstant = firstEventInstant.plusMillis(5_000L);
    LocalDateTime firstEvent = LocalDateTime.ofInstant(firstEventInstant, ZoneOffset.UTC);
    LocalDateTime secondEvent = LocalDateTime.ofInstant(secondEventInstant, ZoneOffset.UTC);
    commitAppend(
        table,
        "events.parquet",
        ImmutableList.of(eventRecord(1L, "one", firstEvent), eventRecord(2L, "two", secondEvent)));

    IcebergScanConfig scanConfig =
        baseConfigBuilder(table, tableId)
            .setKeepFields(ImmutableList.of("id", "event_time"))
            .setWatermarkColumn("event_time")
            .setToSnapshot(table.currentSnapshot().snapshotId())
            .build();
    Schema projectedSchema =
        Schema.builder()
            .addInt64Field("id")
            .addLogicalTypeField(
                "event_time", org.apache.beam.sdk.schemas.logicaltypes.SqlTypes.DATETIME)
            .build();

    PCollection<Row> rows = pipeline.apply(new IncrementalChangelogSource(scanConfig));

    assertEquals(projectedSchema, rows.getSchema());
    PAssert.that(rows.apply(Reify.timestamps()))
        .containsInAnyOrder(
            TimestampedValue.of(
                Row.withSchema(projectedSchema).addValues(1L, firstEvent).build(),
                new Instant(firstEventInstant.toEpochMilli())),
            TimestampedValue.of(
                Row.withSchema(projectedSchema).addValues(2L, secondEvent).build(),
                new Instant(secondEventInstant.toEpochMilli())));

    pipeline.run().waitUntilFinish();
  }

  private TableIdentifier tableId() {
    return TableIdentifier.of("default", testName.getMethodName());
  }

  private IcebergScanConfig.Builder baseConfigBuilder(Table table, TableIdentifier tableId) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogName("name")
                .setCatalogProperties(
                    ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setUseCdc(true);
  }

  private static Map<String, String> tableProperties() {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
  }

  private static Map<String, String> tablePropertiesV3() {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, "3");
  }

  private DataFile commitAppend(Table table, String fileName, List<Record> records)
      throws IOException {
    DataFile file =
        warehouse.writeRecords(testName.getMethodName() + "-" + fileName, table.schema(), records);
    table.newFastAppend().appendFile(file).commit();
    table.refresh();
    return file;
  }

  private static List<Record> records(long id, String data) {
    return ImmutableList.of(record(id, data));
  }

  private static Record record(long id, String data) {
    return TestFixtures.createRecord(CDC_SCHEMA, ImmutableMap.of("id", id, "data", data));
  }

  private static Record eventRecord(long id, String data, LocalDateTime eventTime) {
    return TestFixtures.createRecord(
        EVENT_SCHEMA, ImmutableMap.of("id", id, "data", data, "event_time", eventTime));
  }

  private static final class FormatValueKindAndRow extends DoFn<Row, String> {
    @ProcessElement
    public void process(
        @Element Row row, ValueKind valueKind, OutputReceiver<String> outputReceiver) {
      outputReceiver.output(
          valueKind.name() + ":" + row.getInt64("id") + ":" + row.getString("data"));
    }
  }
}
