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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReadFromChangelogs}. */
@RunWith(JUnit4.class)
public class ReadFromChangelogsTest {
  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "visible", Types.StringType.get()),
              Types.NestedField.optional(3, "hidden", Types.StringType.get())),
          ImmutableSet.of(1));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void unidirectionalTasksEmitProjectedRowsOnly() throws IOException {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    IcebergScanConfig scanConfig = scanConfig(table, tableId, ImmutableList.of("id", "visible"));

    DataFile addedFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-added.parquet",
            table.schema(),
            ImmutableList.of(record(10L, "added", "added-hidden")));
    DataFile deletedRowsFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-deleted-rows.parquet",
            table.schema(),
            ImmutableList.of(
                record(20L, "deleted-row", "deleted-row-hidden"),
                record(21L, "not-deleted", "not-deleted-hidden")));
    DeleteFile addedPositionDelete =
        writePositionDelete(table, deletedRowsFile, "deleted-rows-pos-delete.parquet", 0L);
    DataFile deletedFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-deleted-file.parquet",
            table.schema(),
            ImmutableList.of(record(30L, "deleted-file", "deleted-file-hidden")));

    List<SerializableChangelogTask> tasks =
        ImmutableList.of(
            task(
                SerializableChangelogTask.Type.ADDED_ROWS,
                addedFile,
                ImmutableList.of(),
                ImmutableList.of(),
                table,
                100L),
            task(
                SerializableChangelogTask.Type.DELETED_ROWS,
                deletedRowsFile,
                ImmutableList.of(addedPositionDelete),
                ImmutableList.of(),
                table,
                100L),
            task(
                SerializableChangelogTask.Type.DELETED_FILE,
                deletedFile,
                ImmutableList.of(),
                ImmutableList.of(),
                table,
                100L));

    ReadFromChangelogs.Output output =
        input(ImmutableList.of(KV.of(descriptor(), tasks)), ImmutableList.of())
            .apply(new ReadFromChangelogs(scanConfig));

    assertEquals(
        IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()),
        output.uniDirectionalRows().getSchema());
    PAssert.that(
            output.uniDirectionalRows().apply("Format Unidirectional", ParDo.of(new FormatRow())))
        .containsInAnyOrder(
            "INSERT:10:added:2", "DELETE:20:deleted-row:2", "DELETE:30:deleted-file:2");
    PAssert.that(output.biDirectionalInserts()).empty();
    PAssert.that(output.biDirectionalDeletes()).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void bidirectionalTasksKeepFullRowsForDownstreamResolution() throws IOException {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    IcebergScanConfig scanConfig = scanConfig(table, tableId, ImmutableList.of("id", "visible"));
    DataFile oldFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-old.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "old-hidden")));
    DataFile newFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-new.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "new-hidden")));
    List<SerializableChangelogTask> tasks =
        ImmutableList.of(
            task(
                SerializableChangelogTask.Type.DELETED_FILE,
                oldFile,
                ImmutableList.of(),
                ImmutableList.of(),
                table,
                200L),
            task(
                SerializableChangelogTask.Type.ADDED_ROWS,
                newFile,
                ImmutableList.of(),
                ImmutableList.of(),
                table,
                200L));

    ReadFromChangelogs.Output output =
        input(ImmutableList.of(), ImmutableList.of(KV.of(descriptor(200L, 200L, 1L, 1L), tasks)))
            .apply(new ReadFromChangelogs(scanConfig));

    PAssert.that(output.uniDirectionalRows()).empty();
    PAssert.that(
            output.biDirectionalDeletes().apply("Format Deletes", ParDo.of(new FormatKeyedRow())))
        .containsInAnyOrder("DELETE:200:200:1:shown:old-hidden:3");
    PAssert.that(
            output.biDirectionalInserts().apply("Format Inserts", ParDo.of(new FormatKeyedRow())))
        .containsInAnyOrder("INSERT:200:200:1:shown:new-hidden:3");

    pipeline.run().waitUntilFinish();
  }

  private PCollectionTuple input(
      List<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> unidirectional,
      List<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> largeBidirectional) {
    Schema rowIdBeamSchema =
        IcebergUtils.icebergSchemaToBeamSchema(
            TypeUtil.select(CDC_SCHEMA, CDC_SCHEMA.identifierFieldIds()));
    KvCoder<ChangelogDescriptor, List<SerializableChangelogTask>> coder =
        ChangelogScanner.coder(rowIdBeamSchema);
    PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> uni =
        unidirectional.isEmpty()
            ? pipeline.apply("Empty Unidirectional", Create.empty(coder))
            : pipeline.apply("Create Unidirectional", Create.of(unidirectional).withCoder(coder));
    PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> large =
        largeBidirectional.isEmpty()
            ? pipeline.apply("Empty Large Bidirectional", Create.empty(coder))
            : pipeline.apply(
                "Create Large Bidirectional", Create.of(largeBidirectional).withCoder(coder));
    return PCollectionTuple.of(ChangelogScanner.UNIDIRECTIONAL_TASKS, uni)
        .and(ChangelogScanner.LARGE_BIDIRECTIONAL_TASKS, large);
  }

  private TableIdentifier tableId() {
    return TableIdentifier.of("default", testName.getMethodName());
  }

  private IcebergScanConfig scanConfig(
      Table table, TableIdentifier tableId, List<String> keepFields) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogName("name")
                .setCatalogProperties(
                    ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setKeepFields(keepFields)
        .setUseCdc(true)
        .build();
  }

  private ChangelogDescriptor descriptor() {
    return ChangelogDescriptor.builder()
        .setTableIdentifierString(tableId().toString())
        .setSnapshotSequenceNumber(100L)
        .setCommitSnapshotId(100L)
        .build();
  }

  private ChangelogDescriptor descriptor(
      long sequenceNumber, long snapshotId, long lowerInclusive, long upperInclusive) {
    Schema pkSchema = Schema.builder().addInt64Field("id").build();
    return ChangelogDescriptor.builder()
        .setTableIdentifierString(tableId().toString())
        .setSnapshotSequenceNumber(sequenceNumber)
        .setCommitSnapshotId(snapshotId)
        .setOverlapLower(Row.withSchema(pkSchema).addValue(lowerInclusive).build())
        .setOverlapUpper(Row.withSchema(pkSchema).addValue(upperInclusive).build())
        .build();
  }

  private static Record record(long id, String visible, String hidden) {
    GenericRecord record = GenericRecord.create(CDC_SCHEMA);
    record.setField("id", id);
    record.setField("visible", visible);
    record.setField("hidden", hidden);
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
    try {
      for (long position : positions) {
        writer.write(PositionDelete.<Record>create().set(dataFile.location(), position));
      }
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static SerializableChangelogTask task(
      SerializableChangelogTask.Type type,
      DataFile dataFile,
      List<DeleteFile> addedDeletes,
      List<DeleteFile> existingDeletes,
      Table table,
      long snapshotId) {
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
        .setCommitSnapshotId(snapshotId)
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

  private static Map<String, String> tableProperties() {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
  }

  private static class FormatRow extends DoFn<Row, String> {
    @ProcessElement
    public void process(@Element Row row, ValueKind kind, OutputReceiver<String> out) {
      out.output(
          kind.name()
              + ":"
              + row.getInt64("id")
              + ":"
              + row.getString("visible")
              + ":"
              + row.getSchema().getFieldCount());
    }
  }

  private static class FormatKeyedRow extends DoFn<KV<CdcRowDescriptor, Row>, String> {
    @ProcessElement
    public void process(
        @Element KV<CdcRowDescriptor, Row> element, ValueKind kind, OutputReceiver<String> out) {
      Row row = element.getValue();
      CdcRowDescriptor descriptor = element.getKey();
      out.output(
          kind.name()
              + ":"
              + descriptor.getCommitSnapshotId()
              + ":"
              + descriptor.getSnapshotSequenceNumber()
              + ":"
              + descriptor.getPrimaryKey().getInt64("id")
              + ":"
              + row.getString("visible")
              + ":"
              + row.getString("hidden")
              + ":"
              + row.getSchema().getFieldCount());
    }
  }
}
