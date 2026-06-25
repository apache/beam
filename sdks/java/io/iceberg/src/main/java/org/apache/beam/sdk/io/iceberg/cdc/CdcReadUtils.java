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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-side helpers specific to the CDC source. Keeps {@link ReadUtils} focused on the
 * general-purpose append-only read path; everything that takes a {@link SerializableChangelogTask},
 * references {@link DeleteReader}, or implements the delete-pushdown row-group skipping lives here.
 */
public final class CdcReadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CdcReadUtils.class);

  /**
   * Maximum size of an equality delete set to push down as a Parquet residual {@code IN}
   * expression. Matches {@link ParquetMetricsRowGroupFilter#IN_PREDICATE_LIMIT}.
   */
  private static final int IN_PREDICATE_LIMIT = 200;

  public static CloseableIterable<Record> createReader(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      Schema outputSchema) {
    return createReader(task, table, scanConfig, outputSchema, Expressions.alwaysTrue());
  }

  /**
   * Same as {@link #createReader(SerializableChangelogTask, Table, IcebergScanConfig, Schema)} but
   * ANDs {@code extraResidual} into the task's residual expression. The combined expression is
   * passed to Iceberg's Parquet reader, which uses it as a row-group-level filter (skips row groups
   * whose column statistics cannot match). The caller is still responsible for applying the
   * residual at the row level.
   *
   * <p>This is used to push extra predicates (e.g. an equality-delete {@code IN} expression) down
   * to the reader for cheap row-group skipping.
   */
  public static CloseableIterable<Record> createReader(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      Schema outputSchema,
      Expression extraResidual) {
    return createReader(
        task, table, scanConfig, outputSchema, extraResidual, task.getStart(), task.getLength());
  }

  /**
   * Same as {@link #createReader(SerializableChangelogTask, Table, IcebergScanConfig, Schema,
   * Expression)} but reads the byte range {@code [start, start + length)} of the DataFile.
   * Iceberg's Parquet reader selects the row groups whose starting offset falls within this range,
   * allowing us to prune row-groups by byte-range.
   *
   * <p>Callers are responsible for ensuring the requested range stays within the task's assigned
   * range, to avoid reading a section that is meant for another worker.
   */
  public static CloseableIterable<Record> createReader(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      Schema outputSchema,
      Expression extraResidual,
      long start,
      long length) {
    Expression baseResidual = task.getExpression(table.schema());
    Expression combined =
        extraResidual.op() == Expression.Operation.TRUE
            ? baseResidual
            : Expressions.and(baseResidual, extraResidual);
    return ReadUtils.createReader(
        table,
        scanConfig,
        outputSchema,
        checkStateNotNull(table.specs().get(task.getSpecId())),
        task.getDataFile().createDataFile(table.specs()),
        task.getDataFile().getFileSequenceNumber(),
        start,
        length,
        combined);
  }

  /** Returns a filter that skips records marked for deletion. */
  public static DeleteFilter<Record> genericDeleteFilter(
      Table table, Schema outputSchema, String dataFilePath, List<SerializableDeleteFile> deletes) {
    return new GenericDeleteFilter(
        table.io(),
        dataFilePath,
        table.schema(),
        outputSchema,
        deletes.stream()
            .map(sdf -> sdf.createDeleteFile(table.specs(), table.sortOrders()))
            .collect(Collectors.toList()));
  }

  /** Returns a delete reader that reuses delete structures already loaded by CDC planning. */
  public static DeleteReader<Record> genericDeleteReader(
      Table table,
      Schema outputSchema,
      String dataFilePath,
      List<SerializableDeleteFile> deletes,
      DeleteReader.PreloadedDeletes preloadedDeletes) {
    return new GenericDeleteReader(
        table.io(),
        dataFilePath,
        table.schema(),
        outputSchema,
        deletes.stream()
            .map(sdf -> sdf.createDeleteFile(table.specs(), table.sortOrders()))
            .collect(Collectors.toList()),
        preloadedDeletes);
  }

  /**
   * Opens the records that a CDC reader should process for a single {@link
   * SerializableChangelogTask}, applying the appropriate delete-filter / delete-reader chain for
   * the task's type:
   *
   * <ul>
   *   <li>{@code ADDED_ROWS}: Collect and return the records that became live in this commit:
   *       <ul>
   *         <li>1. Iterate over records in the added DataFile
   *         <li>2. Filter out records matched by any added deletes
   *       </ul>
   *   <li>{@code DELETED_ROWS}: Return records in the DataFile that are marked for deletion by new
   *       DeleteFiles, making sure to first ignore records that have already been marked by
   *       previous DeleteFiles:
   *       <ul>
   *         <li>1. Iterate over records in the referenced DataFile
   *         <li>2. Filter out records matched from existing deletes.
   *         <li>3. Filter out records NOT matched from added deletes
   *       </ul>
   *   <li>{@code DELETED_FILE} — every record in the DataFile that wasn't already deleted by {@code
   *       existingDeletes}.
   *       <ul>
   *         <li>1. Iterate over records in the referenced DataFile
   *         <li>2. Filter out records matched from existing deletes.
   *       </ul>
   * </ul>
   *
   * <p>Projection pushdown should not be used when reading bi-directional tasks because we need to
   * compare all record columns to accurately identify updates. Otherwise, user-configured
   * projection may drop a column that contains real updates. If this happens, the downstream
   * resolver will mistakenly determine the (delete, insert) pair to be a duplicate.
   *
   * <p>If CDC metadata columns are requested, this method only adds row-sourced metadata columns
   * ({@code _row_id}, {@code _last_updated_sequence_number}) to the Iceberg read schema. Changelog
   * context columns are added later by {@link CdcOutputUtils#outputRow}.
   */
  public static CloseableIterable<Record> changelogRecordsForTask(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      boolean useProjectedSchema) {
    String dataFilePath = task.getDataFile().getPath();
    Schema outputSchema =
        CdcOutputUtils.readSchemaWithRowMetadata(
            scanConfig.getMetadataColumns(),
            useProjectedSchema ? scanConfig.getRequiredSchema() : table.schema());
    switch (task.getType()) {
      case ADDED_ROWS:
        DeleteFilter<Record> addedDeletesFilter =
            genericDeleteFilter(table, outputSchema, dataFilePath, task.getAddedDeletes());
        return addedDeletesFilter.filter(
            createReader(task, table, scanConfig, addedDeletesFilter.requiredSchema()));
      case DELETED_FILE:
        DeleteFilter<Record> existingDeletesFilter =
            genericDeleteFilter(table, outputSchema, dataFilePath, task.getExistingDeletes());
        return existingDeletesFilter.filter(
            createReader(task, table, scanConfig, existingDeletesFilter.requiredSchema()));
      case DELETED_ROWS:
        return deletedRowsForTask(task, table, scanConfig, outputSchema);
      default:
        throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getType());
    }
  }

  /**
   * Builds the reader chain for a {@code DELETED_ROWS} task with row-group pushdown when possible.
   * This helps the reader skip entire row groups. For unskipped row groups, the reader should still
   * apply per-record position + equality checks at the row level.
   *
   * <p>We use two pushdown strategies, depending on the type of {@link DeleteFile} in the task
   * (Position Delete vs. Equality Delete). The two strategies can be combined if both {@link
   * DeleteFile} types are present.
   *
   * <ol>
   *   <li><b>Byte-range pushdown for Position Deletes:</b> pre-load the {@link
   *       PositionDeleteIndex}, read the Parquet footer, and compute a single contiguous byte range
   *       covering the row groups that contain at least one deleted position.
   *   <li><b>IN-expression pushdown for Equality Deletes:</b> build an Iceberg {@code IN}
   *       expression and pass it as a Parquet residual so the metrics row-group filter can skip
   *       non-matching row groups.
   * </ol>
   *
   * <p>If Position and Equality deletes are both present, both strategies are used to get one
   * contiguous range. We read only that range, skipping leading and trailing row groups that
   * contain no deletions.
   *
   * <p>Note: Equality pushdown is only used when all delete files share a single equality field.
   * Multi-column equality requires an exploded OR expression that Parquet's metrics filter handles
   * poorly.
   */
  private static CloseableIterable<Record> deletedRowsForTask(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      Schema outputSchema) {
    String dataFilePath = task.getDataFile().getPath();
    List<SerializableDeleteFile> addedDeletes = task.getAddedDeletes();

    // Split into position vs equality.
    List<DeleteFile> posFiles = new ArrayList<>();
    List<DeleteFile> eqFiles = new ArrayList<>();
    for (SerializableDeleteFile sd : addedDeletes) {
      DeleteFile df = sd.createDeleteFile(table.specs(), table.sortOrders());
      if (df.content() == FileContent.POSITION_DELETES) {
        posFiles.add(df);
      } else if (df.content() == FileContent.EQUALITY_DELETES) {
        eqFiles.add(df);
      }
    }

    // Strategy 1: byte-range pushdown around row groups with position deletes (+ eq
    // matches).
    DeleteReader.PreloadedDeletes preloadedDeletes = DeleteReader.PreloadedDeletes.empty();
    if (!posFiles.isEmpty()) {
      @Nullable
      PositionPushdownResult pushdown =
          tryPositionByteRangePushdown(
              task, table, scanConfig, outputSchema, posFiles, eqFiles, addedDeletes);
      if (pushdown != null) {
        if (pushdown.deletedRecords != null) {
          return pushdown.deletedRecords;
        }
        preloadedDeletes = pushdown.preloadedDeletes;
      }
      // fall through to the default chain on failure
    }

    // Strategy 2: equality IN-expression pushdown applied as a reader residual.
    // Only safe when no position deletes are present. when both exist, the
    // byte-range path above already incorporates the eq filter
    Expression eqResidual = Expressions.alwaysTrue();
    if (posFiles.isEmpty() && !eqFiles.isEmpty()) {
      EqualityPushdownResult eqPushdown = buildEqualityDeletePushdown(table, eqFiles);
      eqResidual = eqPushdown.applicable ? eqPushdown.residual : Expressions.alwaysTrue();
      preloadedDeletes = eqPushdown.preloadedDeletes(null);
    }

    DeleteFilter<Record> existingDeletesFilter =
        genericDeleteFilter(table, outputSchema, dataFilePath, task.getExistingDeletes());
    DeleteReader<Record> addedDeletesReader =
        genericDeleteReader(table, outputSchema, dataFilePath, addedDeletes, preloadedDeletes);
    Schema requiredSchema =
        TypeUtil.join(existingDeletesFilter.requiredSchema(), addedDeletesReader.requiredSchema());

    CloseableIterable<Record> records =
        createReader(task, table, scanConfig, requiredSchema, eqResidual);
    CloseableIterable<Record> liveRecords = existingDeletesFilter.filter(records);
    return addedDeletesReader.read(liveRecords);
  }

  /**
   * Path-A byte-range position-delete pushdown. Returns {@code null} if pushdown isn't applicable
   * or any step fails, signaling to the caller to fall back. Returns an empty iterable if every row
   * group is pruned.
   */
  private static @Nullable PositionPushdownResult tryPositionByteRangePushdown(
      SerializableChangelogTask task,
      Table table,
      IcebergScanConfig scanConfig,
      Schema outputSchema,
      List<DeleteFile> posFiles,
      List<DeleteFile> eqFiles,
      List<SerializableDeleteFile> addedDeletes) {
    String dataFilePath = task.getDataFile().getPath();

    // 1. pre-load the position index for this data file.
    PositionDeleteIndex posIndex;
    try {
      DeleteLoader loader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
      posIndex = loader.loadPositionDeletes(posFiles, dataFilePath);
    } catch (RuntimeException e) {
      LOG.info(
          "Failed to pre-load position deletes for {}; falling back to default reader chain.",
          dataFilePath,
          e);
      return null;
    }
    if (posIndex.isEmpty()) {
      // the pos-delete files don't actually target this data file (rare but possible
      // after metadata operations). Fall back so the eq pushdown does not run here either.
      return PositionPushdownResult.fallback(
          DeleteReader.PreloadedDeletes.of(posIndex, Collections.emptyMap()));
    }

    // 2. optional equality filter (used to extend the byte range to include row groups
    // whose stats match the equality IN values).
    @Nullable ParquetMetricsRowGroupFilter eqFilter = null;
    EqualityPushdownResult eqPushdown = EqualityPushdownResult.notApplicable();
    if (!eqFiles.isEmpty()) {
      eqPushdown = buildEqualityDeletePushdown(table, eqFiles);
      if (!eqPushdown.applicable) {
        // eq deletes are present but we can't safely identify which row groups they target.
        // A narrowed position-only range could drop eq-deleted rows, so fall back to the
        // default full-range reader. DeleteReader will still apply residual per record.
        return PositionPushdownResult.fallback(eqPushdown.preloadedDeletes(posIndex));
      }
      eqFilter = new ParquetMetricsRowGroupFilter(table.schema(), eqPushdown.residual);
    }
    DeleteReader.PreloadedDeletes preloadedDeletes = eqPushdown.preloadedDeletes(posIndex);

    // 3. read the footer and compute the task byte range covering every row group that
    // contains a position delete or matches the eq filter.
    long taskStart = task.getStart();
    long taskEnd = taskStart + task.getLength();
    long minStart = Long.MAX_VALUE;
    long maxEnd = Long.MIN_VALUE;

    try {
      long[] sortedDeletePositions = sortedDeletePositions(posIndex);
      InputFile inputFile = table.io().newInputFile(dataFilePath);
      try (ParquetFileReader reader = ParquetFileReader.open(asParquetInputFile(inputFile))) {
        ParquetMetadata footer = reader.getFooter();
        MessageType parquetSchema = footer.getFileMetaData().getSchema();

        // track cumulative row count ourselves. not all Parquet writers will include
        // it in BlockMetaData.getRowIndexOffset
        long cumulativeRows = 0;
        for (BlockMetaData rowGroup : footer.getBlocks()) {
          long rgStartPos = cumulativeRows;
          long rgEndPos = cumulativeRows + rowGroup.getRowCount();
          cumulativeRows = rgEndPos;

          long rgByteStart = rowGroup.getStartingPos();
          long rgByteEnd = rgByteStart + rowGroup.getCompressedSize();

          // skip row groups outside this task's range.
          if (rgByteEnd <= taskStart || rgByteStart >= taskEnd) {
            continue;
          }

          // if row group has a position and/or an equality delete, include it in the global range
          boolean rowGroupHasPosDelete = anyInRange(sortedDeletePositions, rgStartPos, rgEndPos);
          boolean rowGroupMatchesEq =
              eqFilter != null && eqFilter.shouldRead(parquetSchema, rowGroup);

          if (rowGroupHasPosDelete || rowGroupMatchesEq) {
            minStart = Math.min(minStart, rgByteStart);
            maxEnd = Math.max(maxEnd, rgByteEnd);
          }
        }
      }
    } catch (IOException | RuntimeException e) {
      LOG.info(
          "Failed to read Parquet footer for {}; falling back to default reader chain.",
          dataFilePath,
          e);
      return PositionPushdownResult.fallback(preloadedDeletes);
    }

    long readStart = Math.max(minStart, taskStart);
    long readEnd = Math.min(maxEnd, taskEnd);
    if (readStart >= readEnd) {
      // deletes don't target the portion of the DataFile covered by this read task.
      return PositionPushdownResult.of(CloseableIterable.empty(), preloadedDeletes);
    }

    // 4. Open the reader with the narrowed byte range. This range represents the union
    // of "has position delete" + "matches eq stats"
    DeleteFilter<Record> existingDeletesFilter =
        genericDeleteFilter(table, outputSchema, dataFilePath, task.getExistingDeletes());
    DeleteReader<Record> addedDeletesReader =
        genericDeleteReader(table, outputSchema, dataFilePath, addedDeletes, preloadedDeletes);
    Schema requiredSchema =
        TypeUtil.join(existingDeletesFilter.requiredSchema(), addedDeletesReader.requiredSchema());
    CloseableIterable<Record> records =
        createReader(
            task,
            table,
            scanConfig,
            requiredSchema,
            Expressions.alwaysTrue(),
            readStart,
            readEnd - readStart);
    CloseableIterable<Record> liveRecords = existingDeletesFilter.filter(records);
    return PositionPushdownResult.of(addedDeletesReader.read(liveRecords), preloadedDeletes);
  }

  /** Materializes a sorted long[] of the positions in {@code posIndex} for binary-search lookup. */
  private static long[] sortedDeletePositions(PositionDeleteIndex posIndex) {
    long cardinality = posIndex.cardinality();
    if (cardinality > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Position delete index cardinality exceeds Integer.MAX_VALUE: " + cardinality);
    }
    long[] arr = new long[(int) cardinality];
    int[] idx = {0};
    posIndex.forEach(p -> arr[idx[0]++] = p);
    // forEach is ordered for the bitmap-backed implementation, but the interface doesn't
    // promise it, so sort defensively. Cheap relative to the I/O it gates.
    Arrays.sort(arr);
    return arr;
  }

  /** Returns true iff {@code sortedDeletes} contains any value in {@code [start, end)}. */
  private static boolean anyInRange(long[] sortedDeletes, long startInclusive, long endExclusive) {
    if (sortedDeletes.length == 0) {
      return false;
    }
    int i = Arrays.binarySearch(sortedDeletes, startInclusive);
    if (i < 0) {
      i = -i - 1; // insertion point
    }
    return i < sortedDeletes.length && sortedDeletes[i] < endExclusive;
  }

  /**
   * Returns an {@code IN} expression suitable as a Parquet residual for the given equality-delete
   * files, or {@link Expressions#alwaysTrue()} if pushdown is not applicable. See {@link
   * #deletedRowsForTask} for the applicability rules.
   */
  private static EqualityPushdownResult buildEqualityDeletePushdown(
      Table table, List<DeleteFile> eqFiles) {
    // All eq delete files in this task must share a single equality field id.
    Set<Integer> sharedIds = null;
    for (DeleteFile df : eqFiles) {
      Set<Integer> ids = new HashSet<>(df.equalityFieldIds());
      if (sharedIds == null) {
        sharedIds = ids;
      } else if (!sharedIds.equals(ids)) {
        return EqualityPushdownResult.notApplicable();
      }
    }
    if (sharedIds == null || sharedIds.size() != 1) {
      return EqualityPushdownResult.notApplicable();
    }

    int fieldId = Iterables.getOnlyElement(sharedIds);
    Types.NestedField field = table.schema().findField(fieldId);
    if (field == null) {
      return EqualityPushdownResult.notApplicable();
    }
    Schema deleteSchema = TypeUtil.select(table.schema(), sharedIds);

    DeleteLoader loader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
    StructLikeSet set;
    try {
      set = loader.loadEqualityDeletes(eqFiles, deleteSchema);
    } catch (RuntimeException e) {
      LOG.info(
          "Failed to pre-load equality deletes for pushdown; falling back to per-record check.", e);
      return EqualityPushdownResult.notApplicable();
    }

    Map<Set<Integer>, StructLikeSet> preloadedSets = new HashMap<>();
    preloadedSets.put(sharedIds, set);

    if (set.size() > IN_PREDICATE_LIMIT) {
      return EqualityPushdownResult.notApplicable(preloadedSets);
    }
    Class<?> javaClass = field.type().typeId().javaClass();
    List<Object> values = new ArrayList<>(set.size());
    for (StructLike s : set) {
      @Nullable Object v = s.get(0, javaClass);
      if (v == null) {
        // Nulls don't match an IN-expression. pushing down would drop those deletions.
        return EqualityPushdownResult.notApplicable(preloadedSets);
      }
      values.add(v);
    }
    if (values.isEmpty()) {
      return EqualityPushdownResult.notApplicable(preloadedSets);
    }
    return EqualityPushdownResult.applicable(Expressions.in(field.name(), values), preloadedSets);
  }

  private static final class PositionPushdownResult {
    private final @Nullable CloseableIterable<Record> deletedRecords;
    private final DeleteReader.PreloadedDeletes preloadedDeletes;

    private static PositionPushdownResult of(
        CloseableIterable<Record> deletedRecords, DeleteReader.PreloadedDeletes preloadedDeletes) {
      return new PositionPushdownResult(deletedRecords, preloadedDeletes);
    }

    private static PositionPushdownResult fallback(DeleteReader.PreloadedDeletes preloadedDeletes) {
      return new PositionPushdownResult(null, preloadedDeletes);
    }

    private PositionPushdownResult(
        @Nullable CloseableIterable<Record> records,
        DeleteReader.PreloadedDeletes preloadedDeletes) {
      this.deletedRecords = records;
      this.preloadedDeletes = preloadedDeletes;
    }
  }

  private static final class EqualityPushdownResult {
    private static final EqualityPushdownResult NOT_APPLICABLE =
        new EqualityPushdownResult(Expressions.alwaysTrue(), Collections.emptyMap(), false);

    private final Expression residual;
    private final Map<Set<Integer>, StructLikeSet> preloadedSets;
    private final boolean applicable;

    private static EqualityPushdownResult applicable(
        Expression residual, Map<Set<Integer>, StructLikeSet> preloadedSets) {
      return new EqualityPushdownResult(residual, preloadedSets, true);
    }

    private static EqualityPushdownResult notApplicable() {
      return NOT_APPLICABLE;
    }

    private static EqualityPushdownResult notApplicable(
        Map<Set<Integer>, StructLikeSet> preloadedSets) {
      if (preloadedSets.isEmpty()) {
        return NOT_APPLICABLE;
      }
      return new EqualityPushdownResult(Expressions.alwaysTrue(), preloadedSets, false);
    }

    private EqualityPushdownResult(
        Expression residual, Map<Set<Integer>, StructLikeSet> preloadedSets, boolean applicable) {
      this.residual = residual;
      this.preloadedSets = preloadedSets;
      this.applicable = applicable;
    }

    private DeleteReader.PreloadedDeletes preloadedDeletes(
        @Nullable PositionDeleteIndex positionDeleteIndex) {
      return DeleteReader.PreloadedDeletes.of(positionDeleteIndex, preloadedSets);
    }
  }

  public static class GenericDeleteFilter extends DeleteFilter<Record> {
    private final FileIO io;
    private final InternalRecordWrapper asStructLike;

    @SuppressWarnings("method.invocation")
    public GenericDeleteFilter(
        FileIO io,
        String dataFilePath,
        Schema tableSchema,
        Schema requiredSchema,
        List<DeleteFile> deleteFiles) {
      super(dataFilePath, deleteFiles, tableSchema, requiredSchema);
      this.io = io;
      this.asStructLike = new InternalRecordWrapper(requiredSchema().asStruct());
    }

    @Override
    protected StructLike asStructLike(Record record) {
      return asStructLike.wrap(record);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return io.newInputFile(location);
    }
  }

  public static class GenericDeleteReader extends DeleteReader<Record> {
    private final FileIO io;
    private final InternalRecordWrapper asStructLike;

    @SuppressWarnings("method.invocation")
    public GenericDeleteReader(
        FileIO io,
        String dataFilePath,
        Schema tableSchema,
        Schema requiredSchema,
        List<DeleteFile> deleteFiles,
        DeleteReader.PreloadedDeletes preloadedDeletes) {
      super(dataFilePath, deleteFiles, tableSchema, requiredSchema, true, preloadedDeletes);
      this.io = io;
      this.asStructLike = new InternalRecordWrapper(requiredSchema().asStruct());
    }

    @Override
    protected StructLike asStructLike(Record record) {
      return asStructLike.wrap(record);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return io.newInputFile(location);
    }
  }

  /**
   * Adapter from Iceberg's {@link InputFile} to Parquet's {@link org.apache.parquet.io.InputFile},
   * for callers that need to open a Parquet file directly (e.g. to read the footer for row-group
   * pruning decisions). Iceberg has an equivalent internal {@code ParquetIO} but it's
   * package-private.
   */
  public static org.apache.parquet.io.InputFile asParquetInputFile(InputFile icebergFile) {
    return new IcebergParquetInputFile(icebergFile);
  }

  private static final class IcebergParquetInputFile implements org.apache.parquet.io.InputFile {
    private final InputFile delegate;

    IcebergParquetInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      return new IcebergParquetSeekableStream(delegate.newStream());
    }
  }

  private static final class IcebergParquetSeekableStream extends DelegatingSeekableInputStream {
    private final SeekableInputStream delegate;

    IcebergParquetSeekableStream(SeekableInputStream delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws java.io.IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws java.io.IOException {
      delegate.seek(newPos);
    }
  }
}
