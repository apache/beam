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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Serializable, worker-side configuration for the CDC sink, threaded through every stage of the
 * write path: destination/key assignment, commit windowing, delta-file writing, and commit.
 *
 * <p>Construct via {@link #builder()}. Every field but {@link #getSinkId()} has a default; {@link
 * #getSinkId()} has none here since it is supplied by the public sink API (typically a fresh UUID
 * unless the caller pins one explicitly) before {@link Builder#build()} is called.
 *
 * <p>{@link #validate()} checks structural invariants only (value bounds, mutually exclusive
 * options, reserved names). It is pure and side-effect-free: no I/O, no catalog or table access.
 */
@AutoValue
abstract class CdcWriteConfig implements Serializable {

  /**
   * Default value for {@link #getSequenceNumberColumn()}. Pinned to {@link
   * IcebergCdcMetadataColumns#COMMIT_SNAPSHOT_SEQUENCE_NUMBER}, the column name the Iceberg CDC
   * read source populates, so a source-to-sink pipeline works with no explicit wiring.
   */
  static final String DEFAULT_SEQUENCE_NUMBER_COLUMN =
      IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER;

  /**
   * Default value for {@link #getNumShards()}: a starting point of roughly one shard per worker, to
   * be raised to about the pipeline's write parallelism.
   *
   * <p>Sharding is the sink's write-parallelism knob, but unlike Flink's equivalent (writer
   * parallelism, which the operator has already sized to their cluster), it is a fixed number that
   * most users never revisit. It also multiplies straight into file count: every shard that touches
   * a partition writes a file per commit window, so {@code num_shards x touched partitions x
   * windows per day} files. At 64 shards and a 1-minute triggering frequency that is ~92k files a
   * day for a single unpartitioned table, whatever the actual data rate. On a <b>partitioned</b>
   * table {@link #getShardsPerPartition()} caps the per-partition factor without lowering this
   * number, trading per-partition write parallelism for proportionally fewer files.
   *
   * <p>The two failure modes are not symmetric, which is what decides the default. Too few shards
   * shows up immediately and legibly, as a write bottleneck with a growing commit backlog, and is
   * fixed by raising this number. Too many shards shows up as nothing at all until reads and
   * compaction get slow, months later, by which time the small files are already written. So the
   * default errs low and the guidance (see {@code package-info}) tells operators to raise it toward
   * their worker parallelism.
   */
  static final int DEFAULT_NUM_SHARDS = 16;

  /** Default value for {@link #getSorterMemoryMB()}. */
  static final int DEFAULT_SORTER_MEMORY_MB = 100;

  /**
   * Columns that define a row's identity (the Iceberg equality-delete fields).
   *
   * @return the configured equality columns, or {@code null} to use the destination table's
   *     identifier fields (the default).
   */
  abstract @Nullable List<String> getEqualityColumns();

  /**
   * The column holding the per-primary-key monotonic sequence number used to order a single key's
   * changes. Defaults to {@value #DEFAULT_SEQUENCE_NUMBER_COLUMN}.
   */
  abstract String getSequenceNumberColumn();

  /**
   * If set, the change kind is read from this string column instead of the element's native {@link
   * ValueKind}. The column is stripped from the data row and never written to Iceberg.
   *
   * @return the change-type column name, or {@code null} to use the element's native {@link
   *     ValueKind}.
   */
  abstract @Nullable String getChangeTypeColumn();

  /**
   * Optional mapping from {@link #getChangeTypeColumn()} values to {@link ValueKind} names (e.g.
   * Debezium {@code {"c": "INSERT", "u": "UPDATE_AFTER", "d": "DELETE"}}). If {@code null}, {@link
   * #getChangeTypeColumn()} values must already be {@link ValueKind} names.
   *
   * <p>Only the map's values are constrained (each must name a {@link ValueKind} constant); the
   * keys are arbitrary source-system codes. Requires {@link #getChangeTypeColumn()} to also be set.
   * See {@link #validate()}.
   */
  abstract @Nullable Map<String, String> getChangeTypeMap();

  /**
   * The number of deterministic primary-key-hash shards (logical write buckets) per destination:
   * the sink's write-parallelism knob. Must be {@code >= 1}. Defaults to {@value
   * #DEFAULT_NUM_SHARDS}; set it to about your pipeline's write parallelism, and see {@link
   * #DEFAULT_NUM_SHARDS} for why the default errs low.
   */
  abstract int getNumShards();

  /**
   * The maximum number of shards a single partition's rows may occupy on a <b>partitioned</b>
   * destination, always resolved to a concrete value at construction ({@code num_shards}, no cap,
   * when the user left it unset, so downstream code never sees null). A {@code (destination,
   * window)} then writes about {@code min(shards_per_partition, distinct keys)} files per touched
   * partition per file kind, and per-partition write parallelism is capped at this value: {@code 1}
   * pins each partition to a single writer, {@code num_shards} is plain primary-key sharding.
   * Ignored (no partition plan is built) for an unpartitioned destination, which always shards by
   * primary key. See {@link WriteCdcRows#withShardsPerPartition} for the trade-off.
   */
  abstract int getShardsPerPartition();

  /**
   * The in-memory buffer size (MB) for the sorter that orders each shard's records by primary key,
   * then sequence number, then change kind, before writing. Must be {@code >= 1}. Defaults to
   * {@value #DEFAULT_SORTER_MEMORY_MB}.
   */
  abstract int getSorterMemoryMB();

  /**
   * If {@code true}, {@code UPDATE_BEFORE} records are dropped and {@code INSERT}/{@code
   * UPDATE_AFTER} are applied as upserts (equality-delete-then-insert on the primary key). Defaults
   * to {@code false}.
   */
  abstract boolean getUpsert();

  /**
   * If set, a destination that has committed at least once emits a periodic empty token-refresh
   * commit while idle, keeping this sink's committed-through token snapshot recent. Disabled
   * ({@code null}) by default.
   */
  abstract @Nullable Long getTokenHeartbeatMillis();

  /**
   * A stable identifier for this sink, used to namespace the idempotency tokens written to each
   * commit's Iceberg snapshot summary.
   */
  abstract String getSinkId();

  /**
   * Extra user properties to add to every commit's Iceberg snapshot summary. Keys prefixed with
   * {@code beam.cdc.} are reserved for the sink's own idempotency/diagnostic tokens; see {@link
   * #validate()}.
   */
  abstract @Nullable Map<String, String> getSnapshotProperties();

  /**
   * If {@code true}, a poison record (unknown change type, missing/null sequence number, null
   * equality value, an unresolvable destination) is diverted to the sink's failed-rows output
   * instead of failing the pipeline. Defaults to {@code false} (fail-fast).
   */
  abstract boolean getErrorHandling();

  static Builder builder() {
    return new AutoValue_CdcWriteConfig.Builder()
        .setSequenceNumberColumn(DEFAULT_SEQUENCE_NUMBER_COLUMN)
        .setNumShards(DEFAULT_NUM_SHARDS)
        .setShardsPerPartition(DEFAULT_NUM_SHARDS)
        .setSorterMemoryMB(DEFAULT_SORTER_MEMORY_MB)
        .setUpsert(false)
        .setErrorHandling(false);
  }

  void validate() {
    checkArgument(getNumShards() >= 1, "num_shards must be >= 1, got %s", getNumShards());
    checkArgument(
        getShardsPerPartition() >= 1 && getShardsPerPartition() <= getNumShards(),
        "shards_per_partition must be between 1 and num_shards (%s); got %s",
        getNumShards(),
        getShardsPerPartition());
    checkArgument(
        getSorterMemoryMB() >= 1, "sorter_memory_mb must be >= 1, got %s", getSorterMemoryMB());

    @Nullable List<String> equalityColumns = getEqualityColumns();
    checkArgument(
        equalityColumns == null || !equalityColumns.isEmpty(),
        "equality_columns must be non-empty or unset (leave unset to use the table's identifier "
            + "fields).");

    checkArgument(
        !getSequenceNumberColumn().equals(getChangeTypeColumn()),
        "sequence_number_column and change_type_column must be distinct, both are '%s'.",
        getSequenceNumberColumn());

    @Nullable Map<String, String> changeTypeMap = getChangeTypeMap();
    checkArgument(
        changeTypeMap == null || getChangeTypeColumn() != null,
        "change_type_map requires change_type_column to also be set (it defines the source "
            + "values mapped for that column).");
    if (changeTypeMap != null) {
      for (String value : changeTypeMap.values()) {
        checkArgument(
            isValueKindName(value),
            "change_type_map value '%s' is not a valid ValueKind name; must be one of %s.",
            value,
            Arrays.toString(ValueKind.values()));
      }
    }

    @Nullable Long heartbeatMillis = getTokenHeartbeatMillis();
    checkArgument(
        heartbeatMillis == null || heartbeatMillis > 0,
        "token heartbeat (withTokenHeartbeat / token_heartbeat_seconds) must be > 0 when set, "
            + "got %s ms",
        heartbeatMillis);

    @Nullable Map<String, String> snapshotProperties = getSnapshotProperties();
    if (snapshotProperties != null) {
      for (String key : snapshotProperties.keySet()) {
        checkArgument(
            !key.startsWith("beam.cdc."),
            "snapshot_properties key '%s' uses the reserved 'beam.cdc.' prefix; choose a "
                + "different key.",
            key);
      }
    }
  }

  /**
   * Returns {@code source} minus the control columns ({@link #getSequenceNumberColumn()} always,
   * {@link #getChangeTypeColumn()} when configured). This is the single strip rule shared by the
   * coder derivation, table-schema validation, and table auto-creation, so the three always agree.
   */
  Schema stripControlColumns(Schema source) {
    String sequenceNumberColumn = getSequenceNumberColumn();
    @Nullable String changeTypeColumn = getChangeTypeColumn();
    Schema.Builder builder = Schema.builder();
    for (Schema.Field field : source.getFields()) {
      String name = field.getName();
      if (name.equals(sequenceNumberColumn) || name.equals(changeTypeColumn)) {
        continue;
      }
      builder.addField(field);
    }
    return builder.build();
  }

  private static boolean isValueKindName(String value) {
    for (ValueKind kind : ValueKind.values()) {
      if (kind.name().equals(value)) {
        return true;
      }
    }
    return false;
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setEqualityColumns(@Nullable List<String> equalityColumns);

    abstract Builder setSequenceNumberColumn(String sequenceNumberColumn);

    abstract Builder setChangeTypeColumn(@Nullable String changeTypeColumn);

    abstract Builder setChangeTypeMap(@Nullable Map<String, String> changeTypeMap);

    abstract Builder setNumShards(int numShards);

    abstract Builder setShardsPerPartition(int shardsPerPartition);

    abstract Builder setSorterMemoryMB(int sorterMemoryMB);

    abstract Builder setUpsert(boolean upsert);

    abstract Builder setTokenHeartbeatMillis(@Nullable Long tokenHeartbeatMillis);

    abstract Builder setSinkId(String sinkId);

    abstract Builder setSnapshotProperties(@Nullable Map<String, String> snapshotProperties);

    abstract Builder setErrorHandling(boolean errorHandling);

    abstract CdcWriteConfig build();
  }
}
