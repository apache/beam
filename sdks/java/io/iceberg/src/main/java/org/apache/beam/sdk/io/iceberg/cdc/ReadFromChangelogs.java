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

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.icebergSchemaToBeamSchema;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.joda.time.Instant;

@DoFn.BoundedPerElement
public class ReadFromChangelogs<OutT>
    extends DoFn<KV<ChangelogDescriptor, List<SerializableChangelogTask>>, OutT> {
  public static final TupleTag<Row> UNIDIRECTIONAL_ROWS = new TupleTag<>();
  public static final TupleTag<KV<Row, Row>> KEYED_INSERTS = new TupleTag<>();
  public static final TupleTag<KV<Row, Row>> KEYED_DELETES = new TupleTag<>();

  private final Counter numAddedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numAddedRowsScanTasksCompleted");
  private final Counter numDeletedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedRowsScanTasksCompleted");
  private final Counter numDeletedDataFileScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedDataFileScanTasksCompleted");

  private final IcebergScanConfig scanConfig;
  private final boolean keyedOutput;
  private transient StructProjection recordIdProjection;
  private transient org.apache.iceberg.Schema recordIdSchema;
  private final Schema beamRowSchema;
  private final Schema rowAndSnapshotIDBeamSchema;
  private static final String SNAPSHOT_FIELD = "__beam__changelog__snapshot__id__";

  private ReadFromChangelogs(IcebergScanConfig scanConfig, boolean keyedOutput) {
    this.scanConfig = scanConfig;
    this.keyedOutput = keyedOutput;

    this.beamRowSchema = icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
    org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
    this.recordIdSchema = recordSchema.select(recordSchema.identifierFieldNames());
    this.recordIdProjection = StructProjection.create(recordSchema, recordIdSchema);

    this.rowAndSnapshotIDBeamSchema = rowAndSnapshotIDBeamSchema(scanConfig);
  }

  static ReadFromChangelogs<Row> of(IcebergScanConfig scanConfig) {
    return new ReadFromChangelogs<>(scanConfig, false);
  }

  static ReadFromChangelogs<KV<Row, Row>> withKeyedOutput(IcebergScanConfig scanConfig) {
    return new ReadFromChangelogs<>(scanConfig, true);
  }

  /**
   * Determines the keyed output coder, which depends on the requested projected schema and the
   * schema's identifier fields.
   */
  static KvCoder<Row, Row> keyedOutputCoder(IcebergScanConfig scanConfig) {
    org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
    Schema rowAndSnapshotIDBeamSchema = rowAndSnapshotIDBeamSchema(scanConfig);
    return KvCoder.of(
        SchemaCoder.of(rowAndSnapshotIDBeamSchema),
        SchemaCoder.of(icebergSchemaToBeamSchema(recordSchema)));
  }

  private static Schema rowAndSnapshotIDBeamSchema(IcebergScanConfig scanConfig) {
    org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
    org.apache.iceberg.Schema recordIdSchema =
        recordSchema.select(recordSchema.identifierFieldNames());
    Schema rowIdBeamSchema = icebergSchemaToBeamSchema(recordIdSchema);
    List<Schema.Field> fields =
        ImmutableList.<Schema.Field>builder()
            .add(Schema.Field.of(SNAPSHOT_FIELD, Schema.FieldType.INT64))
            .addAll(rowIdBeamSchema.getFields())
            .build();
    return new Schema(fields);
  }

  @Setup
  public void setup() {
    // StructProjection is not serializable, so we need to recompute it when the DoFn gets
    // deserialized
    org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
    this.recordIdSchema = recordSchema.select(recordSchema.identifierFieldNames());
    this.recordIdProjection = StructProjection.create(recordSchema, recordIdSchema);
  }

  @ProcessElement
  public void process(
      @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      MultiOutputReceiver out)
      throws IOException {
    // TODO: use TableCache
    Table table = scanConfig.getTable();
    table.refresh();

    List<SerializableChangelogTask> tasks = element.getValue();

    for (long l = tracker.currentRestriction().getFrom();
        l < tracker.currentRestriction().getTo();
        l++) {
      if (!tracker.tryClaim(l)) {
        return;
      }

      SerializableChangelogTask task = tasks.get((int) l);
      switch (task.getType()) {
        case ADDED_ROWS:
          processAddedRowsTask(task, table, out);
          break;
        case DELETED_ROWS:
          processDeletedRowsTask(task, table, out);
          break;
        case DELETED_FILE:
          processDeletedFileTask(task, table, out);
          break;
      }
    }
  }

  private void processAddedRowsTask(
      SerializableChangelogTask task, Table table, MultiOutputReceiver outputReceiver)
      throws IOException {
    try (CloseableIterable<Record> fullIterable = ReadUtils.createReader(task, table, scanConfig)) {
      DeleteFilter<Record> deleteFilter =
          ReadUtils.genericDeleteFilter(
              table, scanConfig, task.getDataFile().getPath(), task.getAddedDeletes());
      CloseableIterable<Record> filtered = deleteFilter.filter(fullIterable);

      for (Record rec : filtered) {
        outputRecord(
            "INSERT",
            rec,
            outputReceiver,
            task.getCommitSnapshotId(),
            task.getTimestampMillis(),
            KEYED_INSERTS);
      }
    }
    numAddedRowsScanTasksCompleted.inc();
  }

  private void processDeletedRowsTask(
      SerializableChangelogTask task, Table table, MultiOutputReceiver outputReceiver)
      throws IOException {
    DeleteFilter<Record> existingDeletesFilter =
        ReadUtils.genericDeleteFilter(
            table, scanConfig, task.getDataFile().getPath(), task.getExistingDeletes());
    DeleteReader<Record> newDeletesReader =
        ReadUtils.genericDeleteReader(
            table, scanConfig, task.getDataFile().getPath(), task.getAddedDeletes());

    try (CloseableIterable<Record> allRecords = ReadUtils.createReader(task, table, scanConfig)) {
      CloseableIterable<Record> liveRecords = existingDeletesFilter.filter(allRecords);
      CloseableIterable<Record> newlyDeletedRecords = newDeletesReader.read(liveRecords);

      for (Record rec : newlyDeletedRecords) {
        // TODO: output with DELETE kind
        outputRecord(
            "DELETE",
            rec,
            outputReceiver,
            task.getCommitSnapshotId(),
            task.getTimestampMillis(),
            KEYED_DELETES);
      }
    }
    numDeletedRowsScanTasksCompleted.inc();
  }

  private void processDeletedFileTask(
      SerializableChangelogTask task, Table table, MultiOutputReceiver outputReceiver)
      throws IOException {
    try (CloseableIterable<Record> fullIterable = ReadUtils.createReader(task, table, scanConfig)) {
      DeleteFilter<Record> deleteFilter =
          ReadUtils.genericDeleteFilter(
              table, scanConfig, task.getDataFile().getPath(), task.getExistingDeletes());
      CloseableIterable<Record> filtered = deleteFilter.filter(fullIterable);
      for (Record rec : filtered) {
        // TODO: output with DELETE kind
        outputRecord(
            "DELETE-DF",
            rec,
            outputReceiver,
            task.getCommitSnapshotId(),
            task.getTimestampMillis(),
            KEYED_DELETES);
      }
    }
    numDeletedDataFileScanTasksCompleted.inc();
  }

  private void outputRecord(
      String type,
      Record rec,
      MultiOutputReceiver outputReceiver,
      long snapshotId,
      long timestampMillis,
      TupleTag<KV<Row, Row>> keyedTag) {
    Row row = IcebergUtils.icebergRecordToBeamRow(beamRowSchema, rec);
    Instant timestamp = Instant.ofEpochMilli(timestampMillis);
    if (keyedOutput) { // slow path
      StructProjection recId = recordIdProjection.wrap(rec);
      // Create a Row ID consisting of:
      // 1. the task's commit snapshot ID
      // 2. the record ID column values
      // This is needed to sufficiently distinguish a record change
      Row id = structToBeamRow(snapshotId, recId, recordIdSchema, rowAndSnapshotIDBeamSchema);
      outputReceiver.get(keyedTag).outputWithTimestamp(KV.of(id, row), timestamp);
    } else { // fast path
      System.out.printf("[UNIDIRECTIONAL] -- %s(%s, %s)\n%s%n", type, snapshotId, timestamp, row);
      outputReceiver.get(UNIDIRECTIONAL_ROWS).outputWithTimestamp(row, timestamp);
    }
  }

  public static Row structToBeamRow(
      long snapshotId, StructLike struct, org.apache.iceberg.Schema schema, Schema beamSchema) {
    ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
    List<Types.NestedField> columns = schema.columns();
    for (Types.NestedField column : columns) {
      String name = column.name();
      Object value = schema.accessorForField(column.fieldId()).get(struct);
      values.put(name, value);
    }
    // Include snapshot ID as part of the row ID.
    // This is essential to ensure that the downstream ReconcileChanges compares rows
    // within the same operation.
    values.put(SNAPSHOT_FIELD, snapshotId);
    return Row.withSchema(beamSchema).withFieldValues(values.build()).build();
  }

  @GetSize
  public double getSize(
      @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element,
      @Restriction OffsetRange restriction) {
    // TODO(ahmedabu98): this is just the compressed byte size. find a way to make a better estimate
    long size = 0;

    for (long l = restriction.getFrom(); l < restriction.getTo(); l++) {
      SerializableChangelogTask task = element.getValue().get((int) l);
      size += task.getDataFile().getFileSizeInBytes();
      size +=
          task.getAddedDeletes().stream()
              .mapToLong(SerializableDeleteFile::getFileSizeInBytes)
              .sum();
      size +=
          task.getExistingDeletes().stream()
              .mapToLong(SerializableDeleteFile::getFileSizeInBytes)
              .sum();
    }

    return size;
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(
      @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element) {
    return new OffsetRange(0, element.getValue().size());
  }
}
