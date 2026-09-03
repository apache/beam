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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.io.RollingEqualityDeleteWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Writes one sorted {@code (destination, shard, window)} group, collapsing each primary key's
 * changes into at most one equality delete and one data row.
 *
 * <p>The group arrives sorted by {@link CdcSortKey}, so one key's records are contiguous and in
 * (sequence, kind) order. The writer holds a block (the sequence of records for the current key)
 * and flushes it when the key changes.
 *
 * <p><b>The block's last record is the key's final state</b>; its first record tells us whether
 * anything preceded this window.
 *
 * <ul>
 *   <li><b>Opens with INSERT</b>: the key was born this window, so no earlier commit holds it and
 *       no delete is written, even if the key dies again before the window ends.
 *   <li><b>Opens with anything else</b>: an earlier commit may hold the key, so a delete is written
 *       once an UPDATE_BEFORE or DELETE appears. A block of only UPDATE_AFTERs writes none.
 *   <li><b>Upsert mode</b>: always creates a delete.
 *   <li><b>Ends with INSERT or UPDATE_AFTER</b>: the final row is written. Otherwise, the key is
 *       gone and nothing is written.
 * </ul>
 *
 * <h3>Partition routing</h3>
 *
 * <p>The data row routes by the block's <b>last</b> record (the partition the key now lives in).
 * The equality delete routes by the block's <b>first</b> record (the partition the committed row
 * still lives in). Those differ whenever an update moved the row. {@code kindRank} ranks
 * UPDATE_BEFORE and DELETE ahead of the after-images at an equal sequence, so the block opens with
 * a before-image whenever the window's first change carries one, a guarantee that holds within one
 * commit window only. Upsert has no before-images, but it requires partition source columns to be
 * equality columns, so there every record of a block routes alike.
 *
 * <p>Hence the input contract for a table partitioned on non-key columns: every update must carry
 * its UPDATE_BEFORE. A block opening with an after-image can only route its delete to the partition
 * the row moved <i>to</i>, leaving the committed row unreachable in the one it moved from.
 *
 * <p>This never writes position deletes, and no deletion vectors on V3. Those exist to retract a
 * row that was already flushed when a later change superseded it. Collapsing means the superseded
 * row is never written at all.
 */
abstract class RecordDeltaTaskWriter {

  private final PartitionSpec spec;
  private final FileWriterFactory<Record> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Schema deleteSchema;

  /** Column position in the table schema of each {@link #deleteSchema} field. */
  private final int[] pkPos;

  private final boolean upsert;

  private final List<PartitionDeltaWriter> partitionWriters = new ArrayList<>();

  /** The previous record's sort key, for the unsorted-input tripwire in {@link #write}. */
  private byte @Nullable [] lastSortKey;

  /** The current block: sort key, opening and latest records/kinds, and delete-trigger flag. */
  private byte @Nullable [] blockKey;

  private @Nullable Record latestRecord;
  private @Nullable ValueKind latestKind;
  private @Nullable Record firstRecord;
  private @Nullable ValueKind firstKind;
  private boolean sawUbOrDelete;

  RecordDeltaTaskWriter(
      PartitionSpec spec,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Schema deleteSchema,
      boolean upsert) {
    this.spec = spec;
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.deleteSchema = deleteSchema;
    List<Types.NestedField> pkFields = deleteSchema.columns();
    this.pkPos = new int[pkFields.size()];
    List<Types.NestedField> allFields = schema.columns();
    for (int i = 0; i < pkFields.size(); i++) {
      int fieldId = pkFields.get(i).fieldId();
      int pos = -1;
      for (int j = 0; j < allFields.size(); j++) {
        if (allFields.get(j).fieldId() == fieldId) {
          pos = j;
          break;
        }
      }
      if (pos < 0) {
        throw new IllegalStateException(
            "Equality field "
                + pkFields.get(i).name()
                + " is not a top-level column of schema: "
                + schema);
      }
      this.pkPos[i] = pos;
    }
    this.upsert = upsert;
  }

  /** Routes a record to the {@link PartitionDeltaWriter} responsible for its partition. */
  abstract PartitionDeltaWriter route(Record row);

  /**
   * Buffers {@code row} into the current block, flushing the previous block first when {@code
   * sortKey} starts a new primary key.
   */
  public void write(byte[] sortKey, Record row, ValueKind kind) {
    // The collapse is only correct over sorted input, so a regressing key must not be accepted.
    if (lastSortKey != null && Arrays.compareUnsigned(sortKey, lastSortKey) < 0) {
      throw new IllegalStateException(
          "RecordDeltaTaskWriter received unsorted input: a record's sort key sorts below its "
              + "predecessor's within the group.");
    }
    lastSortKey = sortKey.clone();
    if (blockKey != null && !CdcSortKey.samePk(blockKey, sortKey)) {
      // we're encountering a new PK. flush the current one
      flushBlock();
    }
    if (blockKey == null) {
      blockKey = sortKey.clone();
      firstRecord = row;
      firstKind = kind;
    }
    if (kind == ValueKind.UPDATE_BEFORE || kind == ValueKind.DELETE) {
      sawUbOrDelete = true;
    }
    latestRecord = row;
    latestKind = kind;
  }

  /** Flushes the current block per the class javadoc's rule and resets the block state. */
  private void flushBlock() {
    Record row = checkStateNotNull(latestRecord);
    boolean deleteExistingRow;
    if (upsert) {
      deleteExistingRow = true; // any key may replace a row from an earlier commit
    } else if (firstKind == ValueKind.INSERT) {
      deleteExistingRow = false; // key born this window: no earlier commit holds it
    } else {
      // delete if we see a UPDATE_BEFORE/DELETE
      deleteExistingRow = sawUbOrDelete;
    }
    boolean writeRow = latestKind == ValueKind.INSERT || latestKind == ValueKind.UPDATE_AFTER;

    // The delete routes (and projects its key) by the block's first record: kindRank sorts
    // UPDATE_BEFORE/DELETE ahead of after-images at an equal sequence, so the block opens with a
    // before-image whenever the window's first change carries one.
    // Upsert drops before-images, but it also requires partition sources to be equality columns,
    // so there every record of the block routes alike.
    // The write routes by the latest record, the key's final state: the block is sorted by
    // sequence, with kindRank putting the after-image last at an equal sequence.
    if (deleteExistingRow) {
      Record first = checkStateNotNull(firstRecord);
      route(first).delete(projectKey(first));
    }
    if (writeRow) {
      route(row).write(row);
    }
    blockKey = null;
    latestRecord = null;
    latestKind = null;
    firstRecord = null;
    firstKind = null;
    sawUbOrDelete = false;
  }

  /** Flushes the last block, closes every file, and returns the completed files. */
  public WriteResult complete() throws IOException {
    if (blockKey != null) {
      flushBlock();
    }
    close();
    WriteResult.Builder result = WriteResult.builder();
    for (PartitionDeltaWriter writer : partitionWriters) {
      result.addDataFiles(writer.dataFiles());
      result.addDeleteFiles(writer.deleteFiles());
    }
    return result.build();
  }

  /** Closes every file and deletes it: a failed group must leave nothing behind. */
  public void abort() throws IOException {
    close();
    List<String> locations = new ArrayList<>();
    for (PartitionDeltaWriter writer : partitionWriters) {
      for (DataFile file : writer.dataFiles()) {
        locations.add(file.location());
      }
      for (DeleteFile file : writer.deleteFiles()) {
        locations.add(file.location());
      }
    }
    Tasks.foreach(locations).throwFailureWhenFinished().noRetry().run(io::deleteFile);
  }

  private void close() throws IOException {
    Tasks.foreach(partitionWriters)
        .throwFailureWhenFinished()
        .noRetry()
        .run(PartitionDeltaWriter::close, IOException.class);
  }

  /** Projects a full record onto a PK-only {@link Record} matching {@link #deleteSchema}. */
  private Record projectKey(Record row) {
    GenericRecord key = GenericRecord.create(deleteSchema);
    for (int i = 0; i < pkPos.length; i++) {
      key.set(i, row.get(pkPos[i], Object.class));
    }
    return key;
  }

  PartitionDeltaWriter newPartitionWriter(@Nullable PartitionKey partition) {
    PartitionDeltaWriter writer = new PartitionDeltaWriter(partition);
    partitionWriters.add(writer);
    return writer;
  }

  @SuppressWarnings("argument")
  private RollingDataWriter<Record> newDataWriter(@Nullable PartitionKey partition) {
    return new RollingDataWriter<>(writerFactory, fileFactory, io, targetFileSize, spec, partition);
  }

  @SuppressWarnings("argument")
  private RollingEqualityDeleteWriter<Record> newDeleteWriter(@Nullable PartitionKey partition) {
    return new RollingEqualityDeleteWriter<>(
        writerFactory, fileFactory, io, targetFileSize, spec, partition);
  }

  /** One partition's rolling data and equality-delete writers, each opened on first use. */
  protected class PartitionDeltaWriter {
    private final @Nullable PartitionKey partition;
    private @Nullable RollingDataWriter<Record> dataWriter;
    private @Nullable RollingEqualityDeleteWriter<Record> deleteWriter;

    PartitionDeltaWriter(@Nullable PartitionKey partition) {
      this.partition = partition;
    }

    void write(Record row) {
      @Nullable RollingDataWriter<Record> writer = dataWriter;
      if (writer == null) {
        writer = newDataWriter(partition);
        dataWriter = writer;
      }
      writer.write(row);
    }

    void delete(Record key) {
      @Nullable RollingEqualityDeleteWriter<Record> writer = deleteWriter;
      if (writer == null) {
        writer = newDeleteWriter(partition);
        deleteWriter = writer;
      }
      writer.write(key);
    }

    void close() throws IOException {
      try {
        if (dataWriter != null) {
          dataWriter.close();
        }
      } finally {
        if (deleteWriter != null) {
          deleteWriter.close();
        }
      }
    }

    List<DataFile> dataFiles() {
      return dataWriter == null ? ImmutableList.of() : dataWriter.result().dataFiles();
    }

    List<DeleteFile> deleteFiles() {
      return deleteWriter == null ? ImmutableList.of() : deleteWriter.result().deleteFiles();
    }
  }

  /** Record writer for an unpartitioned table. */
  static class UnpartitionedRecordDeltaWriter extends RecordDeltaTaskWriter {
    private final PartitionDeltaWriter writer;

    @SuppressWarnings("method.invocation")
    UnpartitionedRecordDeltaWriter(
        PartitionSpec spec,
        FileWriterFactory<Record> writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        Schema deleteSchema,
        boolean upsert) {
      super(spec, writerFactory, fileFactory, io, targetFileSize, schema, deleteSchema, upsert);
      this.writer = newPartitionWriter(null);
    }

    @Override
    PartitionDeltaWriter route(Record row) {
      return writer;
    }
  }

  /**
   * Partitioned table: a fanout delta writer per partition key, created lazily on first touch and
   * held open, because the group is sorted by PK and partitions interleave.
   */
  static class PartitionedRecordDeltaWriter extends RecordDeltaTaskWriter {
    private final PartitionKey partitionKey;
    private final InternalRecordWrapper wrapper;
    private final Map<PartitionKey, PartitionDeltaWriter> writers = Maps.newHashMap();

    PartitionedRecordDeltaWriter(
        PartitionSpec spec,
        FileWriterFactory<Record> writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        Schema deleteSchema,
        boolean upsert) {
      super(spec, writerFactory, fileFactory, io, targetFileSize, schema, deleteSchema, upsert);
      this.partitionKey = new PartitionKey(spec, schema);
      this.wrapper = new InternalRecordWrapper(schema.asStruct());
    }

    @Override
    PartitionDeltaWriter route(Record row) {
      partitionKey.partition(wrapper.wrap(row));

      @Nullable PartitionDeltaWriter writer = writers.get(partitionKey);
      if (writer == null) {
        // The shared partitionKey is mutated on every route() call; copy before keying the map.
        PartitionKey copiedKey = partitionKey.copy();
        writer = newPartitionWriter(copiedKey);
        writers.put(copiedKey, writer);
      }

      return writer;
    }
  }

  /** Builds a {@link RecordDeltaTaskWriter} writing under a specified {@code spec}. */
  static RecordDeltaTaskWriter create(
      Table table,
      PartitionSpec spec,
      Set<Integer> equalityFieldIds,
      boolean upsert,
      long targetFileSizeBytes,
      OutputFileFactory fileFactory,
      FileFormat dataFormat,
      FileFormat deleteFormat) {
    Schema deleteSchema = TypeUtil.select(table.schema(), Sets.newHashSet(equalityFieldIds));
    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .dataFileFormat(dataFormat)
            .deleteFileFormat(deleteFormat)
            .equalityFieldIds(Ints.toArray(equalityFieldIds))
            .equalityDeleteRowSchema(deleteSchema)
            .build();

    if (spec.isUnpartitioned()) {
      return new UnpartitionedRecordDeltaWriter(
          spec,
          writerFactory,
          fileFactory,
          table.io(),
          targetFileSizeBytes,
          table.schema(),
          deleteSchema,
          upsert);
    } else {
      return new PartitionedRecordDeltaWriter(
          spec,
          writerFactory,
          fileFactory,
          table.io(),
          targetFileSizeBytes,
          table.schema(),
          deleteSchema,
          upsert);
    }
  }

  /** The table's default data file format ({@code write.format.default}, Parquet fallback). */
  static FileFormat dataFileFormat(Table table) {
    return FileFormat.fromString(
        PropertyUtil.propertyAsString(
            table.properties(),
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
  }

  /** The equality-delete file format: {@code write.delete.format.default}, else the data format. */
  static FileFormat deleteFileFormat(Table table, FileFormat dataFormat) {
    return FileFormat.fromString(
        PropertyUtil.propertyAsString(
            table.properties(), TableProperties.DELETE_DEFAULT_FILE_FORMAT, dataFormat.name()));
  }
}
