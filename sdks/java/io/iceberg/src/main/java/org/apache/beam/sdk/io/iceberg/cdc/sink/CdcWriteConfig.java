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
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration for the CDC sink. */
@AutoValue
abstract class CdcWriteConfig implements Serializable {
  static final String DEFAULT_SEQUENCE_NUMBER_COLUMN =
      IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER;

  /**
   * Every shard that touches a partition writes a file per commit window, so {@code num_shards x
   * touched partitions x windows per day} files. On a <b>partitioned</b> table {@link
   * #getShardsPerPartition()} caps the per-partition factor without lowering this number, trading
   * per-partition write parallelism for proportionally fewer files.
   */
  static final int DEFAULT_NUM_SHARDS = 16;

  static final int DEFAULT_SORTER_MEMORY_MB = 100;

  /**
   * Columns that define a row's identity (the Iceberg equality-delete fields). If unspecified, will
   * try to use the destination table's identifier fields.
   */
  abstract @Nullable List<String> getEqualityColumns();

  /**
   * The column holding the per-primary-key monotonic sequence number used to order a single key's
   * changes. Defaults to {@value #DEFAULT_SEQUENCE_NUMBER_COLUMN}.
   */
  abstract String getSequenceNumberColumn();

  /**
   * If set, the change kind is read from this string column instead of the element's native {@link
   * ValueKind}. Values expected to be one of {@code INSERT}, {@code UPDATE_BEFORE}, {@code
   * UPDATE_AFTER}, or {@code DELETE}. If the column contains different values, use {@link
   * #getChangeTypeMap()} to set mapping from those custom values to the expected ones. The column
   * is stripped from the data row and never written to Iceberg.
   */
  abstract @Nullable String getChangeTypeColumn();

  /**
   * Optional mapping from {@link #getChangeTypeColumn()} values to {@link ValueKind} names (e.g.
   * {@code {"c": "INSERT", "u": "UPDATE_AFTER", "d": "DELETE"}}). If {@code null}, {@link
   * #getChangeTypeColumn()} values must already be {@link ValueKind} names.
   */
  abstract @Nullable Map<String, String> getChangeTypeMap();

  /**
   * The number of deterministic primary-key-hash shards (logical write buckets) per destination.
   * Defaults to {@value #DEFAULT_NUM_SHARDS}; set it to about your pipeline's write parallelism.
   */
  abstract int getNumShards();

  /**
   * The maximum number of shards a single partition's rows may occupy on a <b>partitioned</b>
   * destination. A {@code (destination, window)} writes about {@code min(shards_per_partition,
   * distinct keys)} files per touched partition, and per-partition write parallelism is capped at
   * this value. {@code 1} pins each partition to a single writer, {@code num_shards} is plain
   * primary-key sharding. Ignored for an unpartitioned destination, which always shards by primary
   * key.
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
   * to {@code false}, which will expect the source to provide the before-image in order to
   * correctly apply updates.
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
   * {@code beam.cdc.} are reserved for the sink's own idempotency/diagnostic tokens.
   */
  abstract @Nullable Map<String, String> getSnapshotProperties();

  /**
   * If {@code true}, an invalid record (unknown change type, missing/null sequence number, null
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
