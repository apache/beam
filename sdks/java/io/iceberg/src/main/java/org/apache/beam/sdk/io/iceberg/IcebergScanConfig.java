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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets.newHashSet;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.TypeUtil;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;

@AutoValue
public abstract class IcebergScanConfig implements Serializable {
  private transient @MonotonicNonNull Table cachedTable;
  private transient org.apache.iceberg.@MonotonicNonNull Schema cachedProjectedSchema;
  private transient org.apache.iceberg.@MonotonicNonNull Schema cachedRequiredSchema;
  private transient @MonotonicNonNull Expression cachedFilter;

  public enum ScanType {
    TABLE,
    BATCH
  }

  @Pure
  public abstract ScanType getScanType();

  @Pure
  public abstract IcebergCatalogConfig getCatalogConfig();

  @Pure
  public abstract String getTableIdentifier();

  @Pure
  public Table getTable() {
    if (cachedTable == null) {
      cachedTable = TableCache.get(getCatalogConfig(), TableIdentifier.parse(getTableIdentifier()));
    }
    return cachedTable;
  }

  @Pure
  public abstract Schema getSchema();

  @VisibleForTesting
  static org.apache.iceberg.Schema resolveSchema(
      org.apache.iceberg.Schema schema, @Nullable List<String> keep, @Nullable List<String> drop) {
    return resolveSchema(schema, keep, drop, null);
  }

  @VisibleForTesting
  static org.apache.iceberg.Schema resolveSchema(
      org.apache.iceberg.Schema schema,
      @Nullable List<String> keep,
      @Nullable List<String> drop,
      @Nullable Set<String> fieldsInFilter) {
    ImmutableList.Builder<String> selectedFieldsBuilder = ImmutableList.builder();
    if (keep != null && !keep.isEmpty()) {
      selectedFieldsBuilder.addAll(keep);
    } else if (drop != null && !drop.isEmpty()) {
      List<String> paths = new ArrayList<>(TypeUtil.indexNameById(schema.asStruct()).values());
      Collections.sort(paths);
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        boolean isParent = i + 1 < paths.size() && paths.get(i + 1).startsWith(path + ".");
        boolean isDrop = drop.stream().anyMatch(d -> path.equals(d) || path.startsWith(d + "."));
        if (!isParent && !isDrop) {
          selectedFieldsBuilder.add(path);
        }
      }
    } else {
      // default: include all columns
      return schema;
    }

    if (fieldsInFilter != null && !fieldsInFilter.isEmpty()) {
      fieldsInFilter.stream()
          .map(f -> schema.caseInsensitiveFindField(f).name())
          .forEach(selectedFieldsBuilder::add);
    }
    ImmutableList<String> selectedFields = selectedFieldsBuilder.build();
    return selectedFields.isEmpty() ? schema : schema.select(selectedFields);
  }

  /** Returns the projected Schema after applying column pruning. */
  public org.apache.iceberg.Schema getProjectedSchema() {
    if (cachedProjectedSchema == null) {
      cachedProjectedSchema = resolveSchema(getTable().schema(), getKeepFields(), getDropFields());
    }
    return cachedProjectedSchema;
  }

  /**
   * Returns a Schema that includes all the fields required for a successful read. This includes
   * explicitly selected fields and fields referenced in the filter statement.
   */
  public org.apache.iceberg.Schema getRequiredSchema() {
    if (cachedRequiredSchema == null) {
      cachedRequiredSchema =
          resolveSchema(
              getTable().schema(),
              getKeepFields(),
              getDropFields(),
              FilterUtils.getReferencedFieldNames(getFilterString()));
    }
    return cachedRequiredSchema;
  }

  @Pure
  @Nullable
  public Evaluator getEvaluator(org.apache.iceberg.Schema requiredSchema) {
    @Nullable Expression filter = getFilter();
    if (filter == null) {
      return null;
    }
    return new Evaluator(requiredSchema.asStruct(), filter);
  }

  @Pure
  @Nullable
  public Expression getFilter() {
    if (cachedFilter == null) {
      cachedFilter = FilterUtils.convert(getFilterString(), getTable().schema());
    }
    return cachedFilter;
  }

  @Pure
  public abstract @Nullable String getFilterString();

  @Pure
  public abstract @Nullable Boolean getCaseSensitive();

  @Pure
  public abstract ImmutableMap<String, String> getOptions();

  @Pure
  public abstract @Nullable Long getSnapshot();

  @Pure
  public abstract @Nullable Long getTimestamp();

  @Pure
  public abstract @Nullable Long getFromSnapshotInclusive();

  @Pure
  public abstract @Nullable String getFromSnapshotRefInclusive();

  @Pure
  public abstract @Nullable Long getFromSnapshotExclusive();

  @Pure
  public abstract @Nullable String getFromSnapshotRefExclusive();

  @Pure
  public abstract @Nullable Long getToSnapshot();

  @Pure
  public abstract @Nullable String getToSnapshotRef();

  @Pure
  public abstract @Nullable Long getFromTimestamp();

  @Pure
  public abstract @Nullable Long getToTimestamp();

  @Pure
  public abstract @Nullable StartingStrategy getStartingStrategy();

  @Pure
  public abstract boolean getUseCdc();

  @Pure
  public abstract @Nullable Boolean getStreaming();

  @Pure
  public abstract @Nullable Duration getPollInterval();

  @Pure
  public abstract @Nullable String getTag();

  @Pure
  public abstract @Nullable String getBranch();

  @Pure
  public abstract @Nullable List<String> getKeepFields();

  @Pure
  public abstract @Nullable List<String> getDropFields();

  @Pure
  public abstract List<String> getMetadataColumns();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergScanConfig.Builder()
        .setScanType(ScanType.TABLE)
        .setFilterString(null)
        .setCaseSensitive(null)
        .setOptions(ImmutableMap.of())
        .setSnapshot(null)
        .setTimestamp(null)
        .setFromSnapshotInclusive(null)
        .setFromSnapshotRefInclusive(null)
        .setFromSnapshotExclusive(null)
        .setFromSnapshotRefExclusive(null)
        .setToSnapshot(null)
        .setToSnapshotRef(null)
        .setFromTimestamp(null)
        .setToTimestamp(null)
        .setUseCdc(false)
        .setStreaming(null)
        .setPollInterval(null)
        .setStartingStrategy(null)
        .setTag(null)
        .setBranch(null)
        .setMetadataColumns(ImmutableList.of());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setScanType(ScanType type);

    public abstract Builder setCatalogConfig(IcebergCatalogConfig catalog);

    public abstract Builder setTableIdentifier(String tableIdentifier);

    public Builder setTableIdentifier(TableIdentifier tableIdentifier) {
      return this.setTableIdentifier(tableIdentifier.toString());
    }

    public Builder setTableIdentifier(String... names) {
      return setTableIdentifier(TableIdentifier.of(names));
    }

    public abstract Builder setSchema(Schema schema);

    public abstract Builder setFilterString(@Nullable String filter);

    public abstract Builder setCaseSensitive(@Nullable Boolean caseSensitive);

    public abstract Builder setOptions(ImmutableMap<String, String> options);

    public abstract Builder setSnapshot(@Nullable Long snapshot);

    public abstract Builder setTimestamp(@Nullable Long timestamp);

    public abstract Builder setFromSnapshotInclusive(@Nullable Long fromInclusive);

    public abstract Builder setFromSnapshotRefInclusive(@Nullable String ref);

    public abstract Builder setFromSnapshotExclusive(@Nullable Long fromExclusive);

    public abstract Builder setFromSnapshotRefExclusive(@Nullable String ref);

    public abstract Builder setToSnapshot(@Nullable Long snapshot);

    public abstract Builder setToSnapshotRef(@Nullable String ref);

    public abstract Builder setFromTimestamp(@Nullable Long timestamp);

    public abstract Builder setToTimestamp(@Nullable Long timestamp);

    public abstract Builder setStartingStrategy(@Nullable StartingStrategy strategy);

    public abstract Builder setUseCdc(boolean useCdc);

    public abstract Builder setStreaming(@Nullable Boolean streaming);

    public abstract Builder setPollInterval(@Nullable Duration pollInterval);

    public abstract Builder setTag(@Nullable String tag);

    public abstract Builder setBranch(@Nullable String branch);

    public abstract Builder setKeepFields(@Nullable List<String> fields);

    public abstract Builder setDropFields(@Nullable List<String> fields);

    public abstract Builder setMetadataColumns(List<String> metadataColumns);

    public abstract IcebergScanConfig build();
  }

  @VisibleForTesting
  abstract Builder toBuilder();

  void validate(Table table) {
    @Nullable List<String> keep = getKeepFields();
    @Nullable List<String> drop = getDropFields();
    if (keep != null || drop != null) {
      checkArgument(
          keep == null || drop == null, error("only one of 'keep' or 'drop' can be set."));

      Set<String> fieldsSpecified;
      String param;
      if (keep != null) {
        param = "keep";
        fieldsSpecified = newHashSet(checkNotNull(keep));
      } else { // drop != null
        param = "drop";
        fieldsSpecified = newHashSet(checkNotNull(drop));
      }
      fieldsSpecified.removeIf(name -> table.schema().findField(name) != null);

      checkArgument(
          fieldsSpecified.isEmpty(),
          error(String.format("'%s' specifies unknown field(s): %s", param, fieldsSpecified)));
    }

    // TODO(#34168, ahmedabu98): fill these gaps for the existing batch source
    if (!getUseCdc()) {
      List<String> invalidOptions = new ArrayList<>();
      if (MoreObjects.firstNonNull(getStreaming(), false)) {
        invalidOptions.add("streaming");
      }
      if (getPollInterval() != null) {
        invalidOptions.add("poll_interval_seconds");
      }
      if (getFromTimestamp() != null) {
        invalidOptions.add("from_timestamp");
      }
      if (getToTimestamp() != null) {
        invalidOptions.add("to_timestamp");
      }
      if (getFromSnapshotInclusive() != null) {
        invalidOptions.add("from_snapshot");
      }
      if (getToSnapshot() != null) {
        invalidOptions.add("to_snapshot");
      }
      if (getStartingStrategy() != null) {
        invalidOptions.add("starting_strategy");
      }
      if (!getMetadataColumns().isEmpty()) {
        invalidOptions.add("metadata_columns");
      }
      if (!invalidOptions.isEmpty()) {
        throw new IllegalArgumentException(
            error(
                "the following options are currently only available when "
                    + "reading with Managed.ICEBERG_CDC: "
                    + invalidOptions));
      }
    } else {
      Set<Integer> primaryKeyIds = new HashSet<>(table.schema().identifierFieldIds());
      checkState(
          !primaryKeyIds.isEmpty(),
          "Cannot read CDC records as the table schema does not specified any primary key fields.");
      Set<Integer> projectedFieldIds = TypeUtil.getProjectedIds(getProjectedSchema());
      primaryKeyIds.removeAll(projectedFieldIds);
      checkArgument(
          primaryKeyIds.isEmpty(),
          "When reading CDC records, the projected schema must not drop primary key fields. "
              + "The specified configuration drops the following PK fields: %s",
          primaryKeyIds);
      validateMetadataColumns(table);
    }

    if (getStartingStrategy() != null) {
      checkArgument(
          getFromTimestamp() == null && getFromSnapshotInclusive() == null,
          error(
              "'from_timestamp' and 'from_snapshot' are not allowed when 'starting_strategy' is set"));
    }
    checkArgument(
        getFromTimestamp() == null || getFromSnapshotInclusive() == null,
        error("only one of 'from_timestamp' or 'from_snapshot' can be set"));
    checkArgument(
        getToTimestamp() == null || getToSnapshot() == null,
        error("only one of 'to_timestamp' or 'to_snapshot' can be set"));

    if (getPollInterval() != null) {
      checkArgument(
          Boolean.TRUE.equals(getStreaming()),
          error("'poll_interval_seconds' can only be set when streaming is true"));
    }
  }

  private void validateMetadataColumns(Table table) {
    List<String> metadataColumns = getMetadataColumns();
    if (metadataColumns.isEmpty()) {
      return;
    }

    Set<String> uniqueMetadataColumns = new LinkedHashSet<>(metadataColumns);
    checkArgument(
        uniqueMetadataColumns.size() == metadataColumns.size(),
        error("metadata_columns contains duplicate entries: %s"),
        metadataColumns);

    List<String> unsupportedMetadataColumns = new ArrayList<>();
    for (String metadataColumn : metadataColumns) {
      if (!IcebergCdcMetadataColumns.isSupportedColumn(metadataColumn)) {
        unsupportedMetadataColumns.add(metadataColumn);
      }
    }
    checkArgument(
        unsupportedMetadataColumns.isEmpty(),
        error("unsupported metadata_columns: %s. Supported values are: %s"),
        unsupportedMetadataColumns,
        IcebergCdcMetadataColumns.SUPPORTED_COLUMNS);

    for (String metadataColumn : metadataColumns) {
      checkArgument(
          getProjectedSchema().findField(metadataColumn) == null,
          error("metadata column '%s' conflicts with a projected data column"),
          metadataColumn);
    }

    boolean includesRowLineage =
        metadataColumns.stream().anyMatch(IcebergCdcMetadataColumns::isRowMetadataColumn);
    if (includesRowLineage) {
      checkArgument(
          TableUtil.formatVersion(table) >= 3,
          error("row lineage metadata columns %s are only available for Iceberg format v3+ tables"),
          metadataColumns);
    }
  }

  private String error(String message) {
    return "Invalid source configuration: " + message;
  }
}
