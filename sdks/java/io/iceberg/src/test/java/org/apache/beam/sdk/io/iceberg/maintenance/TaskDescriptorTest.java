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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Round-trip and payload-size tests for {@link TaskDescriptor} (C1 compaction). */
@RunWith(JUnit4.class)
public class TaskDescriptorTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void roundTripPreservesFileStartLengthSequenceAndDeletes() throws Exception {
    // Golden round-trip: descriptor -> task must reproduce the file identity, range, data sequence
    // number, and the applying delete files. A v2 table + a positional delete gives a non-null
    // sequence number and one delete on the whole-file task.
    TableIdentifier id = TableIdentifier.of("default", "td_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "2"));
    DataFile dataFile =
        warehouse.writeRecords(
            "d_" + System.nanoTime() + ".parquet", table.schema(), rows(1, 2, 3));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();
    addPositionalDelete(table, dataFile, 0L);

    FileScanTask task;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      task = it.iterator().next();
    }
    assertEquals("fixture: the task must carry the positional delete", 1, task.deletes().size());

    TaskDescriptor descriptor = TaskDescriptor.from(task, table.specs());
    FileScanTask reconstructed = descriptor.toScanTask(table.specs());

    assertEquals(task.file().location(), reconstructed.file().location());
    assertEquals(task.file().format(), reconstructed.file().format());
    assertEquals(task.file().recordCount(), reconstructed.file().recordCount());
    assertEquals(task.file().fileSizeInBytes(), reconstructed.file().fileSizeInBytes());
    assertEquals(task.file().specId(), reconstructed.file().specId());
    assertEquals(task.start(), reconstructed.start());
    assertEquals(task.length(), reconstructed.length());
    assertEquals(task.file().dataSequenceNumber().longValue(), descriptor.getDataSequenceNumber());
    assertEquals(1, reconstructed.deletes().size());
    assertEquals(task.deletes().get(0).location(), reconstructed.deletes().get(0).location());
  }

  @Test
  public void roundTripPreservesDeletionVectorTopLevelFields() throws Exception {
    // R11-7: a deletion vector's top-level fields — contentOffset, contentSizeInBytes,
    // referencedDataFile — are what the reader needs to locate the DV blob inside its Puffin file.
    // TaskDescriptor keeps delete-file stats precisely so these survive the ContentFileParser JSON
    // round-trip; assert them field-by-field on a REAL v3 DV, not merely that a delete round-trips
    // by location.
    TableIdentifier id = TableIdentifier.of("default", "tddv_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    DataFile dataFile =
        warehouse.writeRecords(
            "dv_" + System.nanoTime() + ".parquet", table.schema(), rows(1, 2, 3));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();
    addDeletionVector(table, dataFile, 0L);

    FileScanTask task;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      task = it.iterator().next();
    }
    assertEquals("fixture: the task must carry the deletion vector", 1, task.deletes().size());
    DeleteFile originalDv = task.deletes().get(0);
    // Sanity: a real DV carries a Puffin blob offset/size and references exactly this data file.
    assertNotNull(
        "fixture must be a deletion vector with a content offset", originalDv.contentOffset());
    assertNotNull("fixture DV must carry a content size", originalDv.contentSizeInBytes());
    assertEquals(dataFile.location(), originalDv.referencedDataFile());

    TaskDescriptor descriptor = TaskDescriptor.from(task, table.specs());
    DeleteFile reconstructedDv = descriptor.toScanTask(table.specs()).deletes().get(0);
    assertEquals(
        "contentOffset must round-trip",
        originalDv.contentOffset(),
        reconstructedDv.contentOffset());
    assertEquals(
        "contentSizeInBytes must round-trip",
        originalDv.contentSizeInBytes(),
        reconstructedDv.contentSizeInBytes());
    assertEquals(
        "referencedDataFile must round-trip",
        originalDv.referencedDataFile(),
        reconstructedDv.referencedDataFile());
    assertEquals("DV location must round-trip", originalDv.location(), reconstructedDv.location());
  }

  @Test
  public void roundTripPreservesRangeStartAndLength() throws Exception {
    // A start>0 row-group range must round-trip its exact start/length (the descriptor carries them
    // as scalars, not re-derived).
    TableIdentifier id = TableIdentifier.of("default", "tdr_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    List<Record> rows = new ArrayList<>();
    for (int i = 0; i < 800; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i + "-padding-0123456789abcdef0123456789abcdef");
      rows.add(r);
    }
    DataFile dataFile =
        warehouse.writeRecords(
            "mrg_" + System.nanoTime() + ".parquet",
            table.schema(),
            rows,
            ImmutableMap.<String, String>builder()
                .put("write.parquet.row-group-size-bytes", "8192")
                .put("parquet.enable.dictionary", "false")
                .put("write.parquet.page-size-bytes", "1024")
                .put("write.parquet.row-group-check-max-record-count", "100")
                .put("write.parquet.compression-codec", "uncompressed")
                .build());
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    List<FileScanTask> ranges;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      ranges = Lists.newArrayList(it.iterator().next().split(1L));
    }
    FileScanTask ranged =
        ranges.stream()
            .filter(t -> t.start() > 0)
            .findFirst()
            .orElseThrow(() -> new AssertionError("fixture must produce a start>0 range"));

    TaskDescriptor descriptor = TaskDescriptor.from(ranged, table.specs());
    FileScanTask reconstructed = descriptor.toScanTask(table.specs());
    assertEquals(ranged.start(), reconstructed.start());
    assertEquals(ranged.length(), reconstructed.length());
    assertEquals(ranged.file().location(), reconstructed.file().location());
    // A compaction read is unfiltered: the config filter selects files, not rows.
    assertEquals(org.apache.iceberg.expressions.Expressions.alwaysTrue(), reconstructed.residual());
  }

  @Test
  public void compactDescriptorPayloadShrinksVsFullScanTaskJson() throws Exception {
    // C1: the compact TaskDescriptor payload must be dramatically smaller than embedding a full
    // ScanTaskParser JSON (table schema + spec + residual) per range. With a wide (50-column)
    // schema the shrink is >=10x. Coder-encode the whole group and compare against the summed
    // old-style per-task JSON as the reference ceiling.
    Schema wide = wideSchema(50);
    TableIdentifier id = TableIdentifier.of("default", "c1size_" + System.nanoTime());
    Table table = warehouse.createTable(id, wide);
    int numFiles = 30;
    org.apache.iceberg.AppendFiles append = table.newAppend();
    for (int f = 0; f < numFiles; f++) {
      Record r = GenericRecord.create(wide);
      r.setField("id", (long) f);
      append.appendFile(
          warehouse.writeRecords(
              "w" + f + "_" + System.nanoTime() + ".parquet", wide, Lists.newArrayList(r)));
    }
    append.commit();
    table.refresh();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    assertEquals(numFiles, tasks.size());

    long oldBytes = 0;
    for (FileScanTask t : tasks) {
      oldBytes += ScanTaskParser.toJson(t).getBytes(StandardCharsets.UTF_8).length;
    }

    RewriteSubGroup group =
        RewriteSubGroup.builder()
            .setGlobalIndex(1)
            .setParentGroupIndex(0)
            .setParentSubgroupCount(1)
            .setFileScanTasks(tasks, table.specs())
            .setOutputSpecId(table.spec().specId())
            .setWriteMaxFileSize(Long.MAX_VALUE)
            .setStartingSnapshotId(table.currentSnapshot().snapshotId())
            .setStartingSequenceNumber(table.currentSnapshot().sequenceNumber())
            .setOperationId("op-test")
            .build();
    SchemaCoder<RewriteSubGroup> coder =
        SchemaRegistry.createDefault().getSchemaCoder(RewriteSubGroup.class);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(group, out);
    long newBytes = out.size();

    assertTrue(
        "compact descriptor payload ("
            + newBytes
            + " B) must be >=10x smaller than the old per-task ScanTaskParser JSON ("
            + oldBytes
            + " B)",
        newBytes * 10 <= oldBytes);
  }

  private static Schema wideSchema(int columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    fields.add(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int i = 1; i < columns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "col_" + i, Types.StringType.get()));
    }
    return new Schema(fields);
  }

  private static List<Record> rows(long... ids) {
    List<Record> recs = new ArrayList<>();
    for (long id : ids) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", id);
      r.setField("data", "row-" + id);
      recs.add(r);
    }
    return recs;
  }

  private void addPositionalDelete(Table table, DataFile dataFile, long position) throws Exception {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1L).format(FileFormat.PARQUET).build();
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    PositionDeleteWriter<Record> writer =
        appenderFactory.newPosDeleteWriter(outputFile, FileFormat.PARQUET, null);
    PositionDelete<Record> positionDelete = PositionDelete.create();
    try {
      positionDelete.set(dataFile.location().toString(), position, null);
      writer.write(positionDelete);
    } finally {
      writer.close();
    }
    DeleteFile deleteFile = writer.toDeleteFile();
    table.newRowDelta().addDeletes(deleteFile).commit();
    table.refresh();
  }

  /** Writes a v3 deletion vector deleting {@code position} in {@code dataFile} and commits it. */
  private void addDeletionVector(Table table, DataFile dataFile, long position) throws Exception {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 3, 3L).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try {
      writer.delete(dataFile.location().toString(), position, table.spec(), null);
    } finally {
      writer.close();
    }
    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
    table.refresh();
  }
}
