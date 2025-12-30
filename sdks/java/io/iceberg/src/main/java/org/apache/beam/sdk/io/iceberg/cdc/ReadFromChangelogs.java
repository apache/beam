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
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.BIDIRECTIONAL_CHANGES;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.UNIDIRECTIONAL_CHANGES;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

/**
 * A {@link PTransform} that processed {@link org.apache.iceberg.ChangelogScanTask}s. They come in
 * three types:
 *
 * <ol>
 *   <li><b>AddedRowsScanTask</b>: Indicates records have been inserted via a new DataFile.
 *   <li><b>DeletedRowsScanTask</b>: Indicates records have been deleted via a Position DeleteFile
 *       or Equality DeleteFile.
 *   <li><b>DeletedDataFileScanTask</b>: Indicates a whole DataFile has been deleted.
 * </ol>
 *
 * Each of these ChangelogScanTasks need to be processed differently. More details in the
 * corresponding methods:
 *
 * <ol>
 *   <li>{@link ReadDoFn#processAddedRowsTask(SerializableChangelogTask, Table,
 *       DoFn.MultiOutputReceiver)}
 *   <li>{@link ReadDoFn#processDeletedRowsTask(SerializableChangelogTask, Table,
 *       DoFn.MultiOutputReceiver)}
 *   <li>{@link ReadDoFn#processDeletedFileTask(SerializableChangelogTask, Table,
 *       DoFn.MultiOutputReceiver)}
 * </ol>
 */
class ReadFromChangelogs extends PTransform<PCollectionTuple, ReadFromChangelogs.CdcOutput> {
  private static final Counter numAddedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numAddedRowsScanTasksCompleted");
  private static final Counter numDeletedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedRowsScanTasksCompleted");
  private static final Counter numDeletedDataFileScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedDataFileScanTasksCompleted");

  private static final TupleTag<Row> UNIDIRECTIONAL_ROWS = new TupleTag<>();
  private static final TupleTag<KV<Row, Row>> KEYED_INSERTS = new TupleTag<>();
  private static final TupleTag<KV<Row, Row>> KEYED_DELETES = new TupleTag<>();

  private final IcebergScanConfig scanConfig;
  private final Schema rowAndSnapshotIDBeamSchema;
  // TODO: Any better way of doing this?
  private static final String SNAPSHOT_FIELD = "__beam__changelog__snapshot__id__";

  ReadFromChangelogs(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    this.rowAndSnapshotIDBeamSchema = rowAndSnapshotIDBeamSchema(scanConfig);
  }

  /** Computes the keyed output coder, which depends on the table's primary key spec. */
  private KvCoder<Row, Row> keyedOutputCoder(IcebergScanConfig scanConfig) {
    org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
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

  @Override
  public CdcOutput expand(PCollectionTuple input) {
    PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> uniDirectionalChanges =
        input.get(UNIDIRECTIONAL_CHANGES);
    PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> biDirectionalChanges =
        input.get(BIDIRECTIONAL_CHANGES);

    // === UNIDIRECTIONAL changes ===
    // (i.e. only deletes, or only inserts)
    // take the fast approach of just reading and emitting CDC records.
    PCollection<Row> uniDirectionalCdcRows =
        uniDirectionalChanges
            .apply(Redistribute.arbitrarily())
            .apply(
                "Read Uni-Directional Changes",
                ParDo.of(ReadDoFn.unidirectional(scanConfig))
                    .withOutputTags(UNIDIRECTIONAL_ROWS, TupleTagList.empty()))
            .get(UNIDIRECTIONAL_ROWS)
            .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));

    // === BIDIRECTIONAL changes ===
    // (i.e. a mix of deletes and inserts)
    // will need to be prepared for a downstream CoGBK shuffle to identify potential updates
    PCollectionTuple biDirectionalKeyedCdcRows =
        biDirectionalChanges
            .apply(Redistribute.arbitrarily())
            .apply(
                "Read Bi-Directional Changes",
                ParDo.of(ReadDoFn.bidirectional(scanConfig))
                    .withOutputTags(KEYED_INSERTS, TupleTagList.of(KEYED_DELETES)));

    // set a windowing strategy to maintain the earliest timestamp
    // this allows us to emit records afterward that may have larger reified timestamps
    Window<KV<Row, TimestampedValue<Row>>> windowingStrategy =
        Window.<KV<Row, TimestampedValue<Row>>>into(new GlobalWindows())
            .withTimestampCombiner(TimestampCombiner.EARLIEST);

    // Reify to preserve the element's timestamp. This is currently a no-op because we are
    // setting the ordinal's commit timestamp for all records.
    // But this will matter if user configures a watermark column to derive
    // timestamps from (not supported yet)
    KvCoder<Row, Row> keyedOutputCoder = keyedOutputCoder(scanConfig);
    PCollection<KV<Row, TimestampedValue<Row>>> keyedInsertsWithTimestamps =
        biDirectionalKeyedCdcRows
            .get(KEYED_INSERTS)
            .setCoder(keyedOutputCoder)
            .apply("Reify INSERT Timestamps", Reify.timestampsInValue())
            .apply("Re-window INSERTs", windowingStrategy);
    PCollection<KV<Row, TimestampedValue<Row>>> keyedDeletesWithTimestamps =
        biDirectionalKeyedCdcRows
            .get(KEYED_DELETES)
            .setCoder(keyedOutputCoder)
            .apply("Reify DELETE Timestamps", Reify.timestampsInValue())
            .apply("Re-window DELETEs", windowingStrategy);

    return new CdcOutput(
        input.getPipeline(),
        uniDirectionalCdcRows,
        keyedInsertsWithTimestamps,
        keyedDeletesWithTimestamps);
  }

  public static class CdcOutput implements POutput {
    private final Pipeline pipeline;
    private final PCollection<Row> uniDirectionalRows;
    private final PCollection<KV<Row, TimestampedValue<Row>>> keyedInserts;
    private final PCollection<KV<Row, TimestampedValue<Row>>> keyedDeletes;

    CdcOutput(
        Pipeline p,
        PCollection<Row> uniDirectionalRows,
        PCollection<KV<Row, TimestampedValue<Row>>> keyedInserts,
        PCollection<KV<Row, TimestampedValue<Row>>> keyedDeletes) {
      this.pipeline = p;
      this.uniDirectionalRows = uniDirectionalRows;
      this.keyedInserts = keyedInserts;
      this.keyedDeletes = keyedDeletes;
    }

    PCollection<Row> uniDirectionalRows() {
      return uniDirectionalRows;
    }

    PCollection<KV<Row, TimestampedValue<Row>>> keyedInserts() {
      return keyedInserts;
    }

    PCollection<KV<Row, TimestampedValue<Row>>> keyedDeletes() {
      return keyedDeletes;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          UNIDIRECTIONAL_ROWS,
          uniDirectionalRows,
          KEYED_INSERTS,
          keyedInserts,
          KEYED_DELETES,
          keyedDeletes);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  @DoFn.BoundedPerElement
  private static class ReadDoFn<OutT>
      extends DoFn<KV<ChangelogDescriptor, List<SerializableChangelogTask>>, OutT> {
    private final IcebergScanConfig scanConfig;
    private final boolean keyedOutput;
    private transient StructProjection recordIdProjection;
    private transient org.apache.iceberg.Schema recordIdSchema;
    private final Schema beamRowSchema;
    private final Schema rowAndSnapshotIDBeamSchema;

    /** Used for uni-directional changes. Records are output immediately as-is. */
    static ReadDoFn<Row> unidirectional(IcebergScanConfig scanConfig) {
      return new ReadDoFn<>(scanConfig, false);
    }

    /**
     * Used for bi-directional changes. Records are keyed by (primary key, snapshot ID) and sent to
     * a CoGBK.
     */
    static ReadDoFn<KV<Row, Row>> bidirectional(IcebergScanConfig scanConfig) {
      return new ReadDoFn<>(scanConfig, true);
    }

    private ReadDoFn(IcebergScanConfig scanConfig, boolean keyedOutput) {
      this.scanConfig = scanConfig;
      this.keyedOutput = keyedOutput;

      this.beamRowSchema = icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
      org.apache.iceberg.Schema recordSchema = scanConfig.getProjectedSchema();
      this.recordIdSchema = recordSchema.select(recordSchema.identifierFieldNames());
      this.recordIdProjection = StructProjection.create(recordSchema, recordIdSchema);

      this.rowAndSnapshotIDBeamSchema = rowAndSnapshotIDBeamSchema(scanConfig);
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

    /**
     * 1. Reads the added DataFile. 2. Filters out any matching deletes. This may happen if a
     * matching position delete file is committed in the same snapshot or if changes for multiple
     * snapshots are squashed together. 3. Outputs record.
     */
    private void processAddedRowsTask(
        SerializableChangelogTask task, Table table, MultiOutputReceiver outputReceiver)
        throws IOException {
      try (CloseableIterable<Record> fullIterable =
          ReadUtils.createReader(task, table, scanConfig)) {

        // TODO: AddedRowsScanTask comes with a datafile and potential deletes on that new datafile
        //  (that happened in the same commit).
        //  Should we:
        //  1. Only output the (non-deleted) inserted records?
        //  2. Or output all inserted records and also all deleted records?
        //  Currently we do 1 (only output what is actually 'inserted' in this commit).
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

    /**
     *
     *
     * <ol>
     *   <li>1. Fetches the referenced DataFile (that deletes will be applied to) and iterates over
     *       records.
     *   <li>2. Applies a filter to ignore any existing deletes.
     *   <li>3. Applies a filter to read only the new deletes.
     *   <li>4. Outputs records with delete row kind.
     * </ol>
     */
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

    /**
     *
     *
     * <ol>
     *   <li>1. Fetches the referenced DataFile (that deletes will be applied to) and iterates over
     *       records.
     *   <li>2. Applies a filter to ignore any existing deletes.
     *   <li>4. Outputs records with delete row kind.
     * </ol>
     */
    private void processDeletedFileTask(
        SerializableChangelogTask task, Table table, MultiOutputReceiver outputReceiver)
        throws IOException {
      try (CloseableIterable<Record> fullIterable =
          ReadUtils.createReader(task, table, scanConfig)) {
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

    /**
     * Outputs records to the appropriate downstream collection.
     *
     * <p>If this DoFn is configured for uni-directional changes, records are output directly to the
     * {@link ReadFromChangelogs#UNIDIRECTIONAL_ROWS} tag.
     *
     * <p>If this DoFn is configured for bi-directional changes, records will be keyed by their
     * Primary Key and commit snapshot ID, then output to either {@link
     * ReadFromChangelogs#KEYED_INSERTS} or {@link ReadFromChangelogs#KEYED_DELETES}.
     */
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
      // TODO(ahmedabu98): this is just the compressed byte size. find a way to make a better byte
      // size estimate
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
}
