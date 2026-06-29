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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CdcReadUtils}. */
@RunWith(JUnit4.class)
public class CdcReadUtilsTest {
  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();

  @Test
  public void addedRowsFiltersPositionAndEqualityDeletesWithUnprojectedEqualityColumn()
      throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-added.parquet",
            table.schema(),
            ImmutableList.of(
                record(0L, "keep-0"),
                record(1L, "drop-by-pos"),
                record(2L, "drop-by-data"),
                record(3L, "keep-3")));

    DeleteFile positionDelete =
        writePositionDelete(table, dataFile, "cdc-added-pos-delete.parquet", 1L);
    DeleteFile equalityDelete =
        writeEqualityDelete(table, dataFile, "cdc-added-eq-delete.parquet", "drop-by-data");

    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.ADDED_ROWS,
            dataFile,
            ImmutableList.of(positionDelete, equalityDelete),
            ImmutableList.of(),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), true);

    assertEquals(ImmutableList.of(0L, 3L), idsOf(records));
  }

  @Test
  public void addedRowsUsesFullTableSchemaWhenProjectionDisabled() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-added-full-schema.parquet",
            table.schema(),
            ImmutableList.of(record(0L, "non-projected-value")));
    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.ADDED_ROWS,
            dataFile,
            ImmutableList.of(),
            ImmutableList.of(),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), false);

    // with projection disabled, all columns should be returned
    assertEquals(ImmutableList.of("non-projected-value"), dataValuesOf(records));
  }

  @Test
  public void deletedRowsExcludesRowsAlreadyHiddenByExistingDeletes() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-deleted-rows.parquet",
            table.schema(),
            ImmutableList.of(
                record(0L, "already-hidden-by-position"),
                record(1L, "new-position-delete"),
                record(2L, "already-hidden-by-equality"),
                record(3L, "new-equality-delete"),
                record(4L, "still-live")));

    DeleteFile existingPositionDelete =
        writePositionDelete(table, dataFile, "cdc-existing-pos-delete.parquet", 0L);
    DeleteFile existingEqualityDelete =
        writeEqualityDelete(
            table, dataFile, "cdc-existing-eq-delete.parquet", "already-hidden-by-equality");
    DeleteFile addedPositionDelete =
        writePositionDelete(table, dataFile, "cdc-added-pos-delete.parquet", 1L);
    DeleteFile addedEqualityDelete =
        writeEqualityDelete(table, dataFile, "cdc-added-eq-delete.parquet", "new-equality-delete");

    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.DELETED_ROWS,
            dataFile,
            ImmutableList.of(addedPositionDelete, addedEqualityDelete),
            ImmutableList.of(existingPositionDelete, existingEqualityDelete),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), true);

    assertEquals(ImmutableList.of(1L, 3L), idsOf(records));
  }

  @Test
  public void deletedRowsUsesFullTableSchemaWhenProjectionDisabled() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-deleted-rows-full-schema.parquet",
            table.schema(),
            ImmutableList.of(record(0L, "position-deleted-value"), record(1L, "still-live-value")));
    DeleteFile positionDelete =
        writePositionDelete(table, dataFile, "cdc-deleted-rows-full-schema-pos-delete.parquet", 0L);
    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.DELETED_ROWS,
            dataFile,
            ImmutableList.of(positionDelete),
            ImmutableList.of(),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), false);

    // with projection disabled, all columns should be returned
    assertEquals(ImmutableList.of("position-deleted-value"), dataValuesOf(records));
  }

  @Test
  public void deletedFileEmitsOnlyRowsNotAlreadyHiddenByExistingDeletes() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-deleted-file.parquet",
            table.schema(),
            ImmutableList.of(
                record(0L, "already-hidden-by-position"),
                record(1L, "still-live-one"),
                record(2L, "already-hidden-by-equality"),
                record(3L, "still-live-two")));

    DeleteFile existingPositionDelete =
        writePositionDelete(table, dataFile, "cdc-deleted-file-pos-delete.parquet", 0L);
    DeleteFile existingEqualityDelete =
        writeEqualityDelete(
            table, dataFile, "cdc-deleted-file-eq-delete.parquet", "already-hidden-by-equality");
    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.DELETED_FILE,
            dataFile,
            ImmutableList.of(),
            ImmutableList.of(existingPositionDelete, existingEqualityDelete),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), true);

    assertEquals(ImmutableList.of(1L, 3L), idsOf(records));
  }

  @Test
  public void deletedFileUsesFullTableSchemaWhenProjectionDisabled() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-deleted-file-full-schema.parquet",
            table.schema(),
            ImmutableList.of(record(0L, "deleted-file-value")));
    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.DELETED_FILE,
            dataFile,
            ImmutableList.of(),
            ImmutableList.of(),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), false);

    // with projection disabled, all columns should be returned
    assertEquals(ImmutableList.of("deleted-file-value"), dataValuesOf(records));
  }

  @Test
  public void deletedRowsHandlesNullEqualityDeletesWithoutPushdown() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    DataFile dataFile =
        warehouse.writeRecords(
            "cdc-null-equality-delete.parquet",
            table.schema(),
            ImmutableList.of(record(0L, "live"), record(1L, null), record(2L, "also-live")));
    DeleteFile nullEqualityDelete =
        writeEqualityDelete(table, dataFile, "cdc-null-eq-delete.parquet", null);
    SerializableChangelogTask task =
        task(
            SerializableChangelogTask.Type.DELETED_ROWS,
            dataFile,
            ImmutableList.of(nullEqualityDelete),
            ImmutableList.of(),
            table);

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig(table, tableId), true);

    assertEquals(ImmutableList.of(1L), idsOf(records));
  }

  private static Record record(long id, String data) {
    GenericRecord record = GenericRecord.create(CDC_SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private static DeleteFile writePositionDelete(
      Table table, DataFile dataFile, String filename, long... positions) throws IOException {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    PositionDeleteWriter<Record> writer =
        appenderFactory.newPosDeleteWriter(
            EncryptedFiles.plainAsEncryptedOutput(
                table.io().newOutputFile(dataFile.location() + "." + filename)),
            FileFormat.PARQUET,
            null);
    try (writer) {
      for (long position : positions) {
        writer.write(PositionDelete.<Record>create().set(dataFile.location(), position));
      }
    }
    return writer.toDeleteFile();
  }

  private static DeleteFile writeEqualityDelete(
      Table table, DataFile dataFile, String filename, @Nullable String data) throws IOException {
    org.apache.iceberg.Schema deleteSchema = table.schema().select("data");
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), new int[] {2}, deleteSchema, null);
    EqualityDeleteWriter<Record> writer =
        appenderFactory.newEqDeleteWriter(
            EncryptedFiles.plainAsEncryptedOutput(
                table.io().newOutputFile(dataFile.location() + "." + filename)),
            FileFormat.PARQUET,
            null);
    try (writer) {
      GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("data", data);
      writer.write(deleteRecord);
    }
    return writer.toDeleteFile();
  }

  private IcebergScanConfig scanConfig(Table table, TableIdentifier tableId) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogProperties(
                    ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setKeepFields(ImmutableList.of("id"))
        .build();
  }

  private static SerializableChangelogTask task(
      SerializableChangelogTask.Type type,
      DataFile dataFile,
      List<DeleteFile> addedDeletes,
      List<DeleteFile> existingDeletes,
      Table table) {
    return SerializableChangelogTask.builder()
        .setType(type)
        .setDataFile(dataFile, table.spec().partitionToPath(dataFile.partition()), true)
        .setAddedDeletes(serializableDeletes(addedDeletes, table))
        .setExistingDeletes(serializableDeletes(existingDeletes, table))
        .setSpecId(table.spec().specId())
        .setOperation(
            type == SerializableChangelogTask.Type.ADDED_ROWS
                ? ChangelogOperation.INSERT
                : ChangelogOperation.DELETE)
        .setOrdinal(0)
        .setCommitSnapshotId(1L)
        .setStart(0L)
        .setLength(dataFile.fileSizeInBytes())
        .setJsonExpression(ExpressionParser.toJson(Expressions.alwaysTrue()))
        .build();
  }

  private static List<SerializableDeleteFile> serializableDeletes(
      List<DeleteFile> deletes, Table table) {
    return deletes.stream()
        .map(
            delete ->
                SerializableDeleteFile.from(
                    delete, table.spec().partitionToPath(delete.partition()), true))
        .collect(Collectors.toList());
  }

  private static List<Long> idsOf(CloseableIterable<Record> records) {
    return ImmutableList.copyOf(records).stream()
        .map(record -> (Long) record.getField("id"))
        .collect(Collectors.toList());
  }

  private static List<String> dataValuesOf(CloseableIterable<Record> records) {
    return ImmutableList.copyOf(records).stream()
        .map(record -> (String) record.getField("data"))
        .collect(Collectors.toList());
  }
}
