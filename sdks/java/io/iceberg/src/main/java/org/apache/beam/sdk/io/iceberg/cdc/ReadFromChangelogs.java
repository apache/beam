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

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.icebergRecordToBeamRow;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.icebergSchemaToBeamSchema;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.structToRow;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.LARGE_BIDIRECTIONAL_TASKS;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.UNIDIRECTIONAL_TASKS;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.Type.ADDED_ROWS;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructProjection;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} that processes batches of {@link ChangelogScanTask}s and routes them
 * accordingly:
 *
 * <ul>
 *   <li>Records from Uni-directional batches are directly emitted, as INSERT or DELETE kind
 *   <li>Records from Bi-directional batches are compared against the Primary Key overlap range:
 *       <ul>
 *         <li>if outside the overlap, emit directly as INSERT or DELETE kind
 *         <li>if inside the overlap, key by (snapshot seq#, pk) and route to downstream {@link
 *             CoGroupByKey} and final resolution by {@link ResolveChanges}
 *       </ul>
 * </ul>
 *
 * <p>We first key bi-directional rows by (snapshot sequence number, primary key) before sending to
 * {@link CoGroupByKey} to ensure they stay isolated from other PKs or snapshots. Inserts are routed
 * to
 *
 * <p>A {@link ChangelogScanTask} comes in three types:
 *
 * <ol>
 *   <li><b>AddedRowsScanTask</b>: Indicates records have been inserted by a new DataFile.
 *   <li><b>DeletedRowsScanTask</b>: Indicates records have been deleted using a DeleteFile.
 *   <li><b>DeletedDataFileScanTask</b>: Indicates a whole DataFile has been deleted.
 * </ol>
 *
 * <p>Each of these types need to be processed differently. More details in {@link
 * CdcReadUtils#changelogRecordsForTask}.
 *
 * <p>CDC metadata has two entry points in this transform. Row metadata columns are requested from
 * the Iceberg reader by {@link CdcReadUtils} and travel inside intermediate rows until final output
 * assembly. Snapshot metadata columns come from the {@link ChangelogDescriptor} / {@link
 * CdcRowDescriptor} carried with each task or shuffled row, and {@code _change_type} comes from the
 * emitted change kind. Final user-visible rows are assembled by {@link CdcOutputUtils#outputRow},
 * which appends all requested metadata as top-level columns in the configured order.
 */
public class ReadFromChangelogs extends PTransform<PCollectionTuple, ReadFromChangelogs.Output> {
  private static final Counter numAddedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numAddedRowsScanTasksCompleted");
  private static final Counter numDeletedRowsScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedRowsScanTasksCompleted");
  private static final Counter numDeletedDataFileScanTasksCompleted =
      Metrics.counter(ReadFromChangelogs.class, "numDeletedDataFileScanTasksCompleted");

  private static final TupleTag<Row> UNIDIRECTIONAL_ROWS = new TupleTag<>();
  private static final TupleTag<KV<CdcRowDescriptor, Row>> BIDIRECTIONAL_INSERTS = new TupleTag<>();
  private static final TupleTag<KV<CdcRowDescriptor, Row>> BIDIRECTIONAL_DELETES = new TupleTag<>();

  private final IcebergScanConfig scanConfig;

  ReadFromChangelogs(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Override
  public org.apache.beam.sdk.io.iceberg.cdc.ReadFromChangelogs.Output expand(
      PCollectionTuple input) {
    Schema fullRowSchema =
        CdcOutputUtils.readBeamSchemaWithRowMetadata(
            scanConfig.getMetadataColumns(), scanConfig.getSchema());
    Schema projectedRowSchema =
        IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
    Schema outputRowSchema = CdcOutputUtils.outputSchema(scanConfig, projectedRowSchema);

    // === UNIDIRECTIONAL tasks ===
    // (i.e. only deletes, or only inserts)
    // take the fast approach of just reading and emitting CDC records
    PCollection<Row> uniDirectionalRows =
        input
            .get(UNIDIRECTIONAL_TASKS)
            .apply("Redistribute Uni-Directional Changes", Redistribute.arbitrarily())
            .apply(
                "Read Uni-Directional Changes",
                ParDo.of(ReadDoFn.unidirectional(scanConfig))
                    .withOutputTags(UNIDIRECTIONAL_ROWS, TupleTagList.empty()))
            .get(UNIDIRECTIONAL_ROWS)
            .setRowSchema(outputRowSchema);

    // === BIDIRECTIONAL tasks ===
    // (i.e. a task group containing a mix of deletes and inserts)
    // read and route records according to their PK (see class java doc)
    PCollectionTuple biDirectionalRows =
        input
            .get(LARGE_BIDIRECTIONAL_TASKS)
            .apply("Redistribute Large Bi-Directional Changes", Redistribute.arbitrarily())
            .apply(
                "Read Bi-Directional Changes",
                ParDo.of(ReadDoFn.bidirectional(scanConfig))
                    .withOutputTags(
                        BIDIRECTIONAL_INSERTS,
                        TupleTagList.of(BIDIRECTIONAL_DELETES).and(UNIDIRECTIONAL_ROWS)));
    // Collect pruned (non-overlapping) rows from bi-directional reader
    PCollection<Row> nonOverlappingRowsFromBiDirTasks =
        biDirectionalRows.get(UNIDIRECTIONAL_ROWS).setRowSchema(outputRowSchema);

    // Flatten uni-directional rows from both sources
    PCollection<Row> allUniDirectionalRows =
        PCollectionList.of(uniDirectionalRows)
            .and(nonOverlappingRowsFromBiDirTasks)
            .apply("Flatten Uni-Directional Rows", Flatten.pCollections());

    // Reify to preserve each record's timestamp (CoGBK overwrites timestamps with the window's
    // end-of-window)
    // Note: element timestamps are snapshot commit timestamp
    KvCoder<CdcRowDescriptor, Row> keyedOutputCoder =
        KvCoder.of(
            CdcRowDescriptor.coder(scanConfig.rowIdBeamSchema()), SchemaCoder.of(fullRowSchema));
    PCollection<KV<CdcRowDescriptor, Row>> keyedInsertsWithTimestamps =
        biDirectionalRows.get(BIDIRECTIONAL_INSERTS).setCoder(keyedOutputCoder);
    PCollection<KV<CdcRowDescriptor, Row>> keyedDeletesWithTimestamps =
        biDirectionalRows.get(BIDIRECTIONAL_DELETES).setCoder(keyedOutputCoder);

    return new org.apache.beam.sdk.io.iceberg.cdc.ReadFromChangelogs.Output(
        input.getPipeline(),
        allUniDirectionalRows,
        keyedInsertsWithTimestamps,
        keyedDeletesWithTimestamps);
  }

  public static class Output implements POutput {
    private final Pipeline pipeline;
    private final PCollection<Row> uniDirectionalRows;
    private final PCollection<KV<CdcRowDescriptor, Row>> biDirectionalInserts;
    private final PCollection<KV<CdcRowDescriptor, Row>> biDirectionalDeletes;

    Output(
        Pipeline p,
        PCollection<Row> uniDirectionalRows,
        PCollection<KV<CdcRowDescriptor, Row>> biDirectionalInserts,
        PCollection<KV<CdcRowDescriptor, Row>> biDirectionalDeletes) {
      this.pipeline = p;
      this.uniDirectionalRows = uniDirectionalRows;
      this.biDirectionalInserts = biDirectionalInserts;
      this.biDirectionalDeletes = biDirectionalDeletes;
    }

    PCollection<Row> uniDirectionalRows() {
      return uniDirectionalRows;
    }

    PCollection<KV<CdcRowDescriptor, Row>> biDirectionalInserts() {
      return biDirectionalInserts;
    }

    PCollection<KV<CdcRowDescriptor, Row>> biDirectionalDeletes() {
      return biDirectionalDeletes;
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
          BIDIRECTIONAL_INSERTS,
          biDirectionalInserts,
          BIDIRECTIONAL_DELETES,
          biDirectionalDeletes);
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
    private final Schema projectedBeamRowSchema;
    private final Schema outputBeamRowSchema;
    private final Schema fullBeamRowSchema;
    private transient @MonotonicNonNull OverlapRange overlap;
    private transient @MonotonicNonNull StructProjection outputProjector;
    private transient @MonotonicNonNull StructProjection pkProjector;

    /** Used for uni-directional changes. Records are output immediately as-is. */
    static ReadDoFn<Row> unidirectional(IcebergScanConfig scanConfig) {
      return new ReadDoFn<>(scanConfig, false);
    }

    /**
     * Used for bi-directional changes. Records are keyed by (snapshot sequence number, primary key)
     * and sent to a CoGBK.
     */
    static ReadDoFn<KV<CdcRowDescriptor, Row>> bidirectional(IcebergScanConfig scanConfig) {
      return new ReadDoFn<>(scanConfig, true);
    }

    private ReadDoFn(IcebergScanConfig scanConfig, boolean keyedOutput) {
      this.scanConfig = scanConfig;
      this.keyedOutput = keyedOutput;

      this.projectedBeamRowSchema =
          CdcOutputUtils.readBeamSchemaWithRowMetadata(
              scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema());
      this.outputBeamRowSchema =
          CdcOutputUtils.outputSchema(
              scanConfig, icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
      this.fullBeamRowSchema =
          CdcOutputUtils.readBeamSchemaWithRowMetadata(
              scanConfig.getMetadataColumns(), scanConfig.getSchema());
    }

    @Setup
    public void setup() {
      this.overlap = OverlapRange.forScanConfig(scanConfig);
    }

    @ProcessElement
    public void process(
        @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element,
        RestrictionTracker<OffsetRange, Long> tracker,
        MultiOutputReceiver out)
        throws IOException {
      Table table = TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());

      List<SerializableChangelogTask> tasks = element.getValue();
      ChangelogDescriptor descriptor = element.getKey();
      @Nullable Row overlapLower = descriptor.getOverlapLower();
      @Nullable Row overlapUpper = descriptor.getOverlapUpper();

      for (long l = tracker.currentRestriction().getFrom();
          l < tracker.currentRestriction().getTo();
          l++) {
        if (!tracker.tryClaim(l)) {
          return;
        }

        SerializableChangelogTask task = tasks.get((int) l);
        processTaskRecords(descriptor, task, overlapLower, overlapUpper, table, out);
      }
    }

    /**
     * Processes a ChangelogScanTask and routes records accordingly:
     *
     * <p>If this DoFn is configured with {@link #unidirectional}, we simply read records and output
     * directly to {@link #UNIDIRECTIONAL_ROWS}.
     *
     * <p>If this DoFn is configured with {@link #bidirectional}, we compare against the Primary Key
     * overlap range. If within the overlap, we key by (snapshotId, PK) and out to either {@link
     * #BIDIRECTIONAL_INSERTS} or {@link #BIDIRECTIONAL_DELETES}. Otherwise (not in overlap), we
     * output the record directly to {@link #UNIDIRECTIONAL_ROWS}.
     */
    private void processTaskRecords(
        ChangelogDescriptor descriptor,
        SerializableChangelogTask task,
        @Nullable Row overlapLowerRow,
        @Nullable Row overlapUpperRow,
        Table table,
        MultiOutputReceiver outputReceiver)
        throws IOException {
      OverlapRange ovl = checkStateNotNull(overlap);
      @Nullable StructLike overlapLower = ovl.toStructLike(overlapLowerRow);
      @Nullable StructLike overlapUpper = ovl.toStructLike(overlapUpperRow);

      boolean isInsert = task.getType() == ADDED_ROWS;
      TupleTag<KV<CdcRowDescriptor, Row>> taggedOutput =
          isInsert ? BIDIRECTIONAL_INSERTS : BIDIRECTIONAL_DELETES;
      ValueKind kind = isInsert ? ValueKind.INSERT : ValueKind.DELETE;
      long commitSnapshotId = descriptor.getCommitSnapshotId();
      long commitSnapshotSequenceNumber = descriptor.getSnapshotSequenceNumber();

      Schema readSchema = keyedOutput ? fullBeamRowSchema : projectedBeamRowSchema;
      try (CloseableIterable<Record> records =
          CdcReadUtils.changelogRecordsForTask(task, table, scanConfig, !keyedOutput)) {
        for (Record rec : records) {
          // uni-directional -- just output records (they are already projected by read pushdown)
          if (!keyedOutput) {
            Row row = icebergRecordToBeamRow(projectedBeamRowSchema, rec);
            outputReceiver
                .get(UNIDIRECTIONAL_ROWS)
                .builder(
                    CdcOutputUtils.outputRow(
                        scanConfig.getMetadataColumns(),
                        outputBeamRowSchema,
                        descriptor,
                        kind,
                        row))
                .setValueKind(kind)
                .output();
            continue;
          }

          // bi-directional -- compare overlap
          if (ovl.contains(rec, overlapLower, overlapUpper)) {
            // inside overlap -- read full row and output KV
            Row row = icebergRecordToBeamRow(readSchema, rec);
            Row pk = structToRow(scanConfig.rowIdBeamSchema(), pkProjector().wrap(rec));
            outputReceiver
                .get(taggedOutput)
                .builder(
                    KV.of(
                        CdcRowDescriptor.builder()
                            .setCommitSnapshotId(commitSnapshotId)
                            .setSnapshotSequenceNumber(commitSnapshotSequenceNumber)
                            .setPrimaryKey(pk)
                            .build(),
                        row))
                .setValueKind(kind)
                .output();

          } else {
            // outside overlap -- get projected record and output
            StructLike projected = outputProjector().wrap(rec);
            Row row = structToRow(projectedBeamRowSchema, projected);
            outputReceiver
                .get(UNIDIRECTIONAL_ROWS)
                .builder(
                    CdcOutputUtils.outputRow(
                        scanConfig.getMetadataColumns(),
                        outputBeamRowSchema,
                        descriptor,
                        kind,
                        row))
                .setValueKind(kind)
                .output();
          }
        }
      }

      trackMetrics(task.getType());
    }

    private StructProjection outputProjector() {
      if (outputProjector == null) {
        outputProjector =
            StructProjection.create(
                CdcOutputUtils.readSchemaWithRowMetadata(
                    scanConfig.getMetadataColumns(),
                    TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier())
                        .schema()),
                CdcOutputUtils.readSchemaWithRowMetadata(
                    scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema()));
      }
      return outputProjector;
    }

    private StructProjection pkProjector() {
      if (pkProjector == null) {
        pkProjector =
            StructProjection.create(
                CdcOutputUtils.readSchemaWithRowMetadata(
                    scanConfig.getMetadataColumns(),
                    TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier())
                        .schema()),
                scanConfig.recordIdSchema());
      }
      return pkProjector;
    }

    private void trackMetrics(SerializableChangelogTask.Type type) {
      switch (type) {
        case ADDED_ROWS:
          numAddedRowsScanTasksCompleted.inc();
          break;
        case DELETED_ROWS:
          numDeletedRowsScanTasksCompleted.inc();
          break;
        case DELETED_FILE:
          numDeletedDataFileScanTasksCompleted.inc();
          break;
      }
    }

    private String getKind(SerializableChangelogTask.Type taskType) {
      switch (taskType) {
        case ADDED_ROWS:
          return "INSERT";
        case DELETED_ROWS:
          return "DELETE";
        case DELETED_FILE:
        default:
          return "DELETE-DF";
      }
    }

    private static final int COMPRESSED_TO_DECODED_BYTES_ESTIMATE = 4;

    @GetSize
    public double getSize(
        @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element,
        @Restriction OffsetRange restriction) {
      // TODO(ahmedabu98): can we make this estimate more accurate?
      long size = 0;

      for (long l = restriction.getFrom(); l < restriction.getTo(); l++) {
        SerializableChangelogTask task = element.getValue().get((int) l);
        size += task.getDataFile().getFileSizeInBytes() * COMPRESSED_TO_DECODED_BYTES_ESTIMATE;
        size +=
            task.getAddedDeletes().stream()
                    .mapToLong(SerializableDeleteFile::getFileSizeInBytes)
                    .sum()
                * COMPRESSED_TO_DECODED_BYTES_ESTIMATE;
        size +=
            task.getExistingDeletes().stream()
                    .mapToLong(SerializableDeleteFile::getFileSizeInBytes)
                    .sum()
                * COMPRESSED_TO_DECODED_BYTES_ESTIMATE;
      }

      return size;
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(
        @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element) {
      return new OffsetRange(0, element.getValue().size());
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> out) {
      // Split into individual tasks for maximum initial parallelism
      for (long i = restriction.getFrom(); i < restriction.getTo(); i++) {
        out.output(new OffsetRange(i, i + 1));
      }
    }
  }
}
