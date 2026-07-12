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
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.Type.ADDED_ROWS;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Resolves a small bi-directional changelog group entirely in memory. This is the equivalent of
 * {@link ReadFromChangelogs} + {@link CoGroupByKey} + {@link ResolveChanges}.
 *
 * <p>All tasks in a changelog group belong to the same Iceberg {@link Snapshot}. The upstream
 * {@link ChangelogScanner} routes here only when the total size of the bi-directional group fits
 * within {@link TableProperties#SPLIT_SIZE}.
 *
 * <p>The incoming batch's overlap region has already been computed in the scanning phase by {@link
 * ChangelogScanner}. In this DoFn, we just process each task and route records:
 *
 * <ul>
 *   <li>Records whose PK falls <b>outside</b> the overlap range cannot have an opposing-side match,
 *       so they are emitted directly with {@code INSERT} or {@code DELETE} kind.
 *   <li>Records whose PK falls <b>inside</b> the overlap range are stashed in a {@link
 *       StructLikeMap} keyed by PK, then resolved by {@link CdcResolver}.
 * </ul>
 */
class LocalResolveDoFn extends DoFn<KV<ChangelogDescriptor, List<SerializableChangelogTask>>, Row> {
  private final IcebergScanConfig scanConfig;
  private final org.apache.beam.sdk.schemas.Schema projectedBeamSchema;
  private final org.apache.beam.sdk.schemas.Schema outputBeamSchema;

  private transient @MonotonicNonNull OverlapRange overlap;
  private transient @MonotonicNonNull List<Types.NestedField> nonPkFields;
  private transient @MonotonicNonNull StructProjection projector;

  LocalResolveDoFn(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    this.projectedBeamSchema =
        CdcOutputUtils.readBeamSchemaWithRowMetadata(
            scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema());
    this.outputBeamSchema =
        CdcOutputUtils.outputSchema(
            scanConfig, icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
  }

  @Setup
  public void setup() {
    Schema tableSchema =
        TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier()).schema();
    Schema fullReadSchema =
        CdcOutputUtils.readSchemaWithRowMetadata(scanConfig.getMetadataColumns(), tableSchema);
    this.overlap = OverlapRange.forScanConfig(scanConfig);
    Set<String> pkFieldNames = new HashSet<>(overlap.recordIdSchema().identifierFieldNames());
    // The dedup logic only inspects non-PK fields, so precompute them once.
    List<Types.NestedField> nonPk = new ArrayList<>();
    for (Types.NestedField f : tableSchema.columns()) {
      if (!pkFieldNames.contains(f.name())) {
        nonPk.add(f);
      }
    }
    this.nonPkFields = nonPk;
    this.projector =
        StructProjection.create(
            fullReadSchema,
            CdcOutputUtils.readSchemaWithRowMetadata(
                scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema()));
  }

  @ProcessElement
  public void process(
      @Element KV<ChangelogDescriptor, List<SerializableChangelogTask>> element,
      OutputReceiver<Row> out)
      throws IOException {
    ChangelogDescriptor descriptor = element.getKey();
    Table table = TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());
    OverlapRange ovl = checkStateNotNull(overlap);

    // {PK: (inserts | deletes)} for in-overlap records that need resolution.
    // Records outside the overlap are emitted directly
    StructLikeMap<PkGroup> pkGroups = StructLikeMap.create(ovl.recordIdSchema().asStruct());

    @Nullable StructLike overlapLower = ovl.toStructLike(descriptor.getOverlapLower());
    @Nullable StructLike overlapUpper = ovl.toStructLike(descriptor.getOverlapUpper());
    for (SerializableChangelogTask task : element.getValue()) {
      readAndRoute(descriptor, task, table, overlapLower, overlapUpper, pkGroups, out);
    }

    resolveAndEmit(
        descriptor,
        pkGroups,
        CdcOutputUtils.readSchemaWithRowMetadata(scanConfig.getMetadataColumns(), table.schema()),
        out);
  }

  /**
   * Processes a {@link SerializableChangelogTask} and routes each record.
   *
   * <ul>
   *   <li>Out of overlap: emit directly
   *   <li>Inside overlap: stash in {@code pkGroups} to resolve in {@link #resolveAndEmit}
   * </ul>
   */
  private void readAndRoute(
      ChangelogDescriptor descriptor,
      SerializableChangelogTask task,
      Table table,
      @Nullable StructLike overlapLower,
      @Nullable StructLike overlapUpper,
      StructLikeMap<PkGroup> pkGroups,
      OutputReceiver<Row> out)
      throws IOException {
    OverlapRange ovl = checkStateNotNull(overlap);
    boolean isInsert = task.getType() == ADDED_ROWS;
    try (CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(task, table, scanConfig, false)) {
      for (Record rec : records) {
        if (ovl.contains(rec, overlapLower, overlapUpper)) { // needs resolution
          StructLike pk = StructLikeUtil.copy(ovl.recordIdProjection());
          PkGroup group = pkGroups.computeIfAbsent(pk, k -> new PkGroup());
          if (isInsert) {
            group.inserts.add(rec);
          } else {
            group.deletes.add(rec);
          }
        } else { // safe to emit directly
          emit(descriptor, rec, isInsert ? ValueKind.INSERT : ValueKind.DELETE, out);
        }
      }
    }
  }

  /** Resolves each PK group using {@link CdcResolver}. */
  private void resolveAndEmit(
      ChangelogDescriptor descriptor,
      StructLikeMap<PkGroup> pkGroups,
      Schema fullSchema,
      OutputReceiver<Row> out) {
    CdcResolver<Record> resolver = new RecordResolver(checkStateNotNull(nonPkFields), fullSchema);
    for (PkGroup group : pkGroups.values()) {
      resolver.resolve(
          group.deletes,
          group.inserts,
          (kind, rec) -> {
            emit(descriptor, rec, kind, out);
          });
    }
  }

  /** Resolver specialization that hashes Iceberg Record non-PK fields. */
  private static final class RecordResolver extends CdcResolver<Record> {
    private final List<Types.NestedField> nonPkFields;
    private final Comparator<StructLike> nonPkComparator;
    private final StructProjection left;
    private final StructProjection right;

    RecordResolver(List<Types.NestedField> nonPkFields, Schema recSchema) {
      this.nonPkFields = nonPkFields;
      Set<Integer> nonPkFieldIds =
          nonPkFields.stream().map(Types.NestedField::fieldId).collect(Collectors.toSet());
      this.left = StructProjection.create(recSchema, nonPkFieldIds);
      this.right = StructProjection.create(recSchema, nonPkFieldIds);
      this.nonPkComparator =
          Comparators.forType(TypeUtil.select(recSchema, nonPkFieldIds).asStruct());
    }

    @Override
    protected int nonPkHash(Record rec) {
      int hash = 1;
      for (Types.NestedField field : nonPkFields) {
        hash = 31 * hash + Objects.hashCode(rec.getField(field.name()));
      }
      return hash;
    }

    @Override
    protected boolean nonPkEquals(Record delete, Record insert) {
      return nonPkComparator.compare(left.wrap(delete), right.wrap(insert)) == 0;
    }
  }

  /** Prune to get the final projected record then output as a Beam Row. */
  private void emit(
      ChangelogDescriptor descriptor, Record rec, ValueKind kind, OutputReceiver<Row> out) {
    StructLike projected = checkStateNotNull(projector).wrap(rec);
    Row record = IcebergUtils.structToRow(projectedBeamSchema, projected);
    out.builder(
            CdcOutputUtils.outputRow(
                scanConfig.getMetadataColumns(), outputBeamSchema, descriptor, kind, record))
        .setValueKind(kind)
        .output();
  }

  /** Two parallel lists of inserts/deletes that share a primary key. */
  private static final class PkGroup {
    final List<Record> inserts = new ArrayList<>();
    final List<Record> deletes = new ArrayList<>();
  }
}
