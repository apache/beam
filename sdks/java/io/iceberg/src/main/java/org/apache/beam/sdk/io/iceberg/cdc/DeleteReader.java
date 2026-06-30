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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimaps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a {@link org.apache.iceberg.DataFile} and returns records marked deleted by the given
 * {@link DeleteFile}s.
 *
 * <p>This is mostly a copy of {@link org.apache.iceberg.data.DeleteFilter}, but flipping the logic
 * to output deleted records instead of filtering them out.
 */
public abstract class DeleteReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteReader.class);

  private final String filePath;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final PreloadedDeletes preloadedDeletes;
  private final Schema requiredSchema;
  private final Accessor<StructLike> posAccessor;
  private volatile @Nullable DeleteLoader deleteLoader = null;
  private @Nullable PositionDeleteIndex deleteRowPositions = null;
  private @Nullable List<Predicate<T>> isInDeleteSets = null;

  protected DeleteReader(
      String filePath,
      List<DeleteFile> deletes,
      Schema tableSchema,
      Schema expectedSchema,
      boolean needRowPosCol,
      PreloadedDeletes preloadedDeletes) {
    this.filePath = filePath;
    this.preloadedDeletes = preloadedDeletes;

    ImmutableList.Builder<DeleteFile> posDeleteBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> eqDeleteBuilder = ImmutableList.builder();
    for (DeleteFile delete : deletes) {
      switch (delete.content()) {
        case POSITION_DELETES:
          LOG.debug("Adding position delete file {} to reader", delete.location());
          posDeleteBuilder.add(delete);
          break;
        case EQUALITY_DELETES:
          LOG.debug("Adding equality delete file {} to reader", delete.location());
          eqDeleteBuilder.add(delete);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown delete file content: " + delete.content());
      }
    }

    this.posDeletes = posDeleteBuilder.build();
    this.eqDeletes = eqDeleteBuilder.build();
    this.requiredSchema =
        fileProjection(tableSchema, expectedSchema, posDeletes, eqDeletes, needRowPosCol);
    this.posAccessor = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
  }

  public Schema requiredSchema() {
    return requiredSchema;
  }

  protected abstract StructLike asStructLike(T record);

  protected abstract InputFile getInputFile(String location);

  protected InputFile loadInputFile(DeleteFile deleteFile) {
    return getInputFile(deleteFile.location());
  }

  protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
  }

  protected DeleteLoader newDeleteLoader() {
    return new BaseDeleteLoader(this::loadInputFile);
  }

  private DeleteLoader deleteLoader() {
    if (deleteLoader == null) {
      synchronized (this) {
        if (deleteLoader == null) {
          this.deleteLoader = newDeleteLoader();
        }
      }
    }

    return deleteLoader;
  }

  /**
   * Returns records that are deleted by <b>either</b> the position deletes <b>or</b> the equality
   * deletes attached to this reader — i.e. the union of the two delete predicates.
   *
   * <p>Each delete-type predicate is built independently and defaults to "false" (no contribution
   * to the union) when its side has no delete files. Both predicates are then OR-combined and
   * applied in a single pass over {@code records}. This guarantees that:
   *
   * <ul>
   *   <li>A task with only position deletes emits all records whose position is in the index.
   *   <li>A task with only equality deletes emits all records matching any equality delete value.
   *   <li>A task with both emits the union of the two (without duplication).
   * </ul>
   */
  public CloseableIterable<T> read(CloseableIterable<T> records) {
    Predicate<T> isPosDeleted =
        posDeletes.isEmpty() ? t -> false : positionDeletePredicate(deletedRowPositions());
    Predicate<T> isEqDeleted = applyEqDeletes().stream().reduce(Predicate::or).orElse(t -> false);
    return CloseableIterable.filter(records, isPosDeleted.or(isEqDeleted));
  }

  private Predicate<T> positionDeletePredicate(PositionDeleteIndex positionIndex) {
    return record -> positionIndex.isDeleted(pos(record));
  }

  private List<Predicate<T>> applyEqDeletes() {
    if (isInDeleteSets != null) {
      return isInDeleteSets;
    }

    isInDeleteSets = Lists.newArrayList();
    if (eqDeletes.isEmpty()) {
      return isInDeleteSets;
    }

    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
      filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
        filesByDeleteIds.asMap().entrySet()) {
      Set<Integer> ids = entry.getKey();
      Iterable<DeleteFile> deletes = entry.getValue();

      Schema deleteSchema = TypeUtil.select(requiredSchema, ids);

      // a projection to select and reorder fields of the file schema to match the delete rows
      StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

      StructLikeSet deleteSet = preloadedDeletes.equalityDeleteSet(ids);
      if (deleteSet == null) {
        deleteSet = deleteLoader().loadEqualityDeletes(deletes, deleteSchema);
      }
      StructLikeSet deleteSetForPredicate = deleteSet;
      Predicate<T> isInDeleteSet =
          record -> deleteSetForPredicate.contains(projectRow.wrap(asStructLike(record)));
      checkStateNotNull(isInDeleteSets).add(isInDeleteSet);
    }

    return checkStateNotNull(isInDeleteSets);
  }

  public PositionDeleteIndex deletedRowPositions() {
    if (deleteRowPositions == null) {
      deleteRowPositions = preloadedDeletes.positionDeleteIndex();
      if (deleteRowPositions == null && !posDeletes.isEmpty()) {
        deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
      }
    }

    return checkStateNotNull(deleteRowPositions);
  }

  /** Delete data already loaded by a planning/pushdown path for one task read. */
  public static final class PreloadedDeletes {
    private static final PreloadedDeletes EMPTY =
        new PreloadedDeletes(null, Collections.emptyMap());

    private final @Nullable PositionDeleteIndex positionDeleteIndex;
    private final Map<Set<Integer>, StructLikeSet> equalityDeleteSets;

    public static PreloadedDeletes empty() {
      return EMPTY;
    }

    public static PreloadedDeletes of(
        @Nullable PositionDeleteIndex positionDeleteIndex,
        Map<Set<Integer>, StructLikeSet> equalityDeleteSets) {
      if (positionDeleteIndex == null && equalityDeleteSets.isEmpty()) {
        return EMPTY;
      }
      return new PreloadedDeletes(positionDeleteIndex, equalityDeleteSets);
    }

    private PreloadedDeletes(
        @Nullable PositionDeleteIndex positionDeleteIndex,
        Map<Set<Integer>, StructLikeSet> equalityDeleteSets) {
      this.positionDeleteIndex = positionDeleteIndex;
      Map<Set<Integer>, StructLikeSet> copied = new HashMap<>();
      for (Map.Entry<Set<Integer>, StructLikeSet> entry : equalityDeleteSets.entrySet()) {
        copied.put(Collections.unmodifiableSet(Sets.newHashSet(entry.getKey())), entry.getValue());
      }
      this.equalityDeleteSets = Collections.unmodifiableMap(copied);
    }

    public @Nullable PositionDeleteIndex positionDeleteIndex() {
      return positionDeleteIndex;
    }

    public @Nullable StructLikeSet equalityDeleteSet(Set<Integer> equalityFieldIds) {
      return equalityDeleteSets.get(equalityFieldIds);
    }
  }

  private static Schema fileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes,
      boolean needRowPosCol) {
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

    Set<Integer> requiredIds = Sets.newLinkedHashSet();
    if (needRowPosCol && !posDeletes.isEmpty()) {
      requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    for (DeleteFile eqDelete : eqDeletes) {
      requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    Set<Integer> missingIds =
        Sets.newLinkedHashSet(
            Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
      return requestedSchema;
    }

    // TODO: support adding nested columns. this will currently fail when finding nested columns to
    // add
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
      if (fieldId == MetadataColumns.ROW_POSITION.fieldId()
          || fieldId == MetadataColumns.IS_DELETED.fieldId()) {
        continue; // add _pos and _deleted at the end
      }

      Types.NestedField field = tableSchema.asStruct().field(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find required field for ID %s", fieldId);

      columns.add(field);
    }

    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
      columns.add(MetadataColumns.ROW_POSITION);
    }

    if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
      columns.add(MetadataColumns.IS_DELETED);
    }

    return new Schema(columns);
  }
}
