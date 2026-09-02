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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergWriteResult;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The top-level CDC sink transform: applies a collection of change records to one or more Iceberg
 * V2+ tables:
 *
 * <pre>{@code
 * changeRecords.apply(IcebergIO.writeCdcRows(catalogConfig)
 *     .to(tableId)
 *     .withSequenceNumberColumn("seq")
 *     .withTriggeringFrequency(Duration.standardMinutes(1)));
 * }</pre>
 *
 * <h3>Input contract</h3>
 *
 * <p>Each input {@link Row} is one change record. Its change kind is its native {@link ValueKind},
 * but can be overridden with a string column value using {@link #withChangeTypeColumn}, optionally
 * translated via {@link #withChangeTypeMap}. Each row <b>must</b> also carry a per-key monotonic
 * (long) sequence number specified by {@link #withSequenceNumberColumn}, which orders a single
 * primary key's changes; the column is required in the input schema as a non-nullable {@code
 * INT64}; set defaults upstream. These control columns are stripped from the input rows before
 * writing to the table.
 *
 * <p><b>Ordering requirement (not validated):</b> for a given primary key, the input element's
 * event-time must be non-decreasing with its (sequence number, kind rank): a higher-sequence change
 * never carries an earlier event time, and equal-sequence records (an update's before and after
 * images) carry equal event times, so neither half lands in an earlier commit window. A violation
 * can corrupt final table state (a lower-sequence equality delete deleting a higher-sequence row
 * committed in a later snapshot).
 *
 * <h3>Semantics</h3>
 *
 * <p>The sink commits one new snapshot to the destination table per commit window, in ascending
 * window order, with an idempotency token written to each snapshot's summary. A retried or
 * restarted commit finds the token and skips already-committed windows, making the commit
 * effectively-once on runners that honor {@code @RequiresStableInput}. Records whose grouped pane
 * fires late (watermark is past their commit window's end) are diverted to the DLQ output,
 * accessible with {@link IcebergWriteResult#getDeadLetterRows()}.
 *
 * <p>Each dead-lettered record nests the data row under {@code record}, beside {@code change_type},
 * {@code sequence_number}, and {@code destination}.
 *
 * <h3>Sink id</h3>
 *
 * <p>The sink creates a fresh unique id by default. All commits in a single pipeline run share the
 * same id. Commits get stamped with the sink id and a window-end millis token. Set {@link
 * #withSinkId} explicitly (and keep it stable) for cross-relaunch idempotency. Don't reuse sink ids
 * for batch runs though because all commits fall under a single global window, so a second load
 * with the same sink id will recognize the same window-end millis token and skip the commit. A
 * batch load's sink id must likewise not be carried into a streaming continuation.
 */
@Internal
@AutoValue
public abstract class WriteCdcRows extends PTransform<PCollection<Row>, IcebergWriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteCdcRows.class);

  public static final Duration DEFAULT_ALLOWED_LATENESS = Duration.standardHours(6);

  abstract IcebergCatalogConfig getCatalogConfig();

  abstract @Nullable TableIdentifier getTableIdentifier();

  abstract @Nullable DynamicDestinations getDynamicDestinations();

  abstract @Nullable List<String> getEqualityColumns();

  abstract String getSequenceNumberColumn();

  abstract @Nullable String getChangeTypeColumn();

  abstract @Nullable Map<String, String> getChangeTypeMap();

  abstract int getNumShards();

  abstract @Nullable Integer getShardsPerPartition();

  abstract int getSorterMemoryMB();

  abstract boolean getUpsert();

  abstract @Nullable Long getTokenHeartbeatMillis();

  abstract boolean getErrorHandlingEnabled();

  abstract @Nullable Map<String, String> getSnapshotProperties();

  /** The sink id namespacing the {@code beam.cdc.} snapshot-summary tokens. */
  abstract String getSinkId();

  abstract @Nullable Duration getTriggeringFrequency();

  abstract @Nullable Duration getAllowedLateness();

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setCatalogConfig(IcebergCatalogConfig catalogConfig);

    abstract Builder setTableIdentifier(TableIdentifier tableIdentifier);

    abstract Builder setDynamicDestinations(DynamicDestinations destinations);

    abstract Builder setEqualityColumns(List<String> equalityColumns);

    abstract Builder setSequenceNumberColumn(String sequenceNumberColumn);

    abstract Builder setChangeTypeColumn(String changeTypeColumn);

    abstract Builder setChangeTypeMap(Map<String, String> changeTypeMap);

    abstract Builder setNumShards(int numShards);

    abstract Builder setShardsPerPartition(Integer shardsPerPartition);

    abstract Builder setSorterMemoryMB(int sorterMemoryMB);

    abstract Builder setUpsert(boolean upsert);

    abstract Builder setTokenHeartbeatMillis(@Nullable Long tokenHeartbeatMillis);

    abstract Builder setErrorHandlingEnabled(boolean errorHandlingEnabled);

    abstract Builder setSnapshotProperties(Map<String, String> snapshotProperties);

    abstract Builder setSinkId(String sinkId);

    abstract Builder setTriggeringFrequency(Duration triggeringFrequency);

    abstract Builder setAllowedLateness(Duration allowedLateness);

    abstract WriteCdcRows build();
  }

  public static WriteCdcRows of(IcebergCatalogConfig catalogConfig) {
    return new AutoValue_WriteCdcRows.Builder()
        .setCatalogConfig(catalogConfig)
        .setSequenceNumberColumn(CdcWriteConfig.DEFAULT_SEQUENCE_NUMBER_COLUMN)
        .setNumShards(CdcWriteConfig.DEFAULT_NUM_SHARDS)
        .setSorterMemoryMB(CdcWriteConfig.DEFAULT_SORTER_MEMORY_MB)
        .setUpsert(false)
        .setErrorHandlingEnabled(false)
        .setSinkId(UUID.randomUUID().toString())
        .build();
  }

  /** Writes to a single table. Mutually exclusive with {@link #to(DynamicDestinations)}. */
  public WriteCdcRows to(TableIdentifier tableIdentifier) {
    return toBuilder().setTableIdentifier(tableIdentifier).build();
  }

  /**
   * Writes to multiple tables. Mutually exclusive with {@link #to(TableIdentifier)}.
   *
   * <p>The sink projects the raw input row (minus the control columns) against each destination
   * table's schema.
   */
  public WriteCdcRows to(DynamicDestinations destinations) {
    return toBuilder().setDynamicDestinations(destinations).build();
  }

  /**
   * Columns that define a row's identity (the Iceberg equality-delete fields). Defaults to the
   * destination table's identifier (primary-key) fields. Tables may be partitioned on non-key
   * columns; partition source columns must be equality columns only under {@link #withUpsert} or a
   * {@link #withShardsPerPartition} cap.
   */
  public WriteCdcRows withEqualityColumns(List<String> columns) {
    return toBuilder().setEqualityColumns(columns).build();
  }

  /**
   * The column holding the per-primary-key monotonic sequence number used to order a single key's
   * changes. Must be declared as a non-nullable {@code INT64} in the input schema. The column is
   * stripped from the written rows. Defaults to {@value
   * CdcWriteConfig#DEFAULT_SEQUENCE_NUMBER_COLUMN}.
   */
  public WriteCdcRows withSequenceNumberColumn(String column) {
    return toBuilder().setSequenceNumberColumn(column).build();
  }

  /**
   * When set, reads the change kind from this column instead of the element's native {@link
   * ValueKind}. Must be declared as a non-nullable {@code STRING} in the input schema. The column
   * is stripped from the written rows.
   */
  public WriteCdcRows withChangeTypeColumn(String column) {
    return toBuilder().setChangeTypeColumn(column).build();
  }

  /**
   * Mapping from {@link #withChangeTypeColumn} values to {@link ValueKind} names (e.g. for
   * Debezium: {@code {"c": "INSERT", "u": "UPDATE_AFTER", "d": "DELETE"}}). Requires {@link
   * #withChangeTypeColumn} to also be set.
   */
  public WriteCdcRows withChangeTypeMap(Map<String, String> changeTypeMap) {
    return toBuilder().setChangeTypeMap(changeTypeMap).build();
  }

  /**
   * The number of deterministic primary-key-hash shards per destination. Controls the sink's
   * write-parallelism knob. Defaults to {@value CdcWriteConfig#DEFAULT_NUM_SHARDS}.
   *
   * <p>Too low may cause a write bottleneck with a growing commit backlog, too high increases the
   * sink's output file count ({@code num_shards x touched partitions} files per commit window).
   *
   * <p>On a partitioned table, a commit window writes up to this many files per touched partition.
   * {@link #withShardsPerPartition} lowers that per-partition count while leaving this number
   * unchanged, so the file count drops without lowering the ceiling on total write parallelism.
   * Each individual partition is then written by at most that many shards.
   */
  public WriteCdcRows withNumShards(int numShards) {
    return toBuilder().setNumShards(numShards).build();
  }

  /**
   * Caps how many shards one partition may occupy (default: {@code num_shards}, i.e. all shards may
   * write to all partitions). A lower cap reduces write parallelism per partition but also
   * concentrates the writes in fewer files. A cap of {@code 1} pins each partition to a single
   * writer. Ignored for unpartitioned tables. A cap below {@code num_shards} requires every
   * partition source column to be an equality column, making the partition (and with it the shard)
   * a pure function of the primary key.
   */
  public WriteCdcRows withShardsPerPartition(int shardsPerPartition) {
    return toBuilder().setShardsPerPartition(shardsPerPartition).build();
  }

  /**
   * The in-memory buffer size (MB) for the sorter that orders each shard's records by primary key,
   * then sequence number, then change kind, before writing; groups larger than this spill to disk.
   * Must be {@code >= 1}. Defaults to {@value CdcWriteConfig#DEFAULT_SORTER_MEMORY_MB}.
   */
  public WriteCdcRows withSorterMemoryMB(int sorterMemoryMB) {
    Preconditions.checkArgument(
        sorterMemoryMB >= 1, "sorter_memory_mb must be >= 1, got %s", sorterMemoryMB);
    return toBuilder().setSorterMemoryMB(sorterMemoryMB).build();
  }

  /**
   * If {@code true}, only the after-image of each change ({@code INSERT}/{@code UPDATE_AFTER}) is
   * required; {@code UPDATE_BEFORE} records are dropped and {@code INSERT}/{@code UPDATE_AFTER} are
   * applied as upserts (equality-delete-then-insert on the primary key). Defaults to {@code false}.
   * Requires every partition source column to be an equality column: with before-images dropped, a
   * row that moved partitions could never be deleted from its old one.
   */
  public WriteCdcRows withUpsert(boolean upsert) {
    return toBuilder().setUpsert(upsert).build();
  }

  /**
   * Enables a periodic empty token-refresh (heartbeat) commit for each destination with a
   * committed-through token, whether committed in this run or recovered from the table: while idle,
   * the committer re-writes the token into a fresh snapshot every {@code interval}, so the
   * token-bearing snapshot stays recent and is less likely to be lost to {@code expire_snapshots}
   * before the sink resumes. Disabled by default, and ignored for bounded (batch) input.
   *
   * <p>With heartbeat enabled prefer cancel-and-resubmit over drain: the self-re-arming
   * processing-time timer can keep a drain from completing.
   */
  public WriteCdcRows withTokenHeartbeat(Duration interval) {
    Preconditions.checkArgument(
        interval != null && interval.getMillis() > 0, "token heartbeat interval must be positive");
    return toBuilder().setTokenHeartbeatMillis(interval.getMillis()).build();
  }

  /**
   * Enables per-record error handling. Poison records (unknown change type, missing/null sequence
   * number, null equality value, an unresolvable destination) are diverted to {@link
   * IcebergWriteResult#getFailedRows()} (schema {@code failed_row ROW + error_message STRING}).
   * Defaults to off where the DoFn throws an error instead. Diversion covers poison detectable in
   * the assignment stage; a failure later in the pipeline fails and retries its bundle as usual.
   */
  public WriteCdcRows withErrorHandling() {
    return toBuilder().setErrorHandlingEnabled(true).build();
  }

  /**
   * Extra user properties to add to every commit's Iceberg snapshot summary. Keys prefixed with
   * {@code beam.cdc.} are reserved for the sink's own idempotency/diagnostic tokens and are
   * rejected at construction.
   */
  public WriteCdcRows withSnapshotProperties(Map<String, String> snapshotProperties) {
    return toBuilder().setSnapshotProperties(snapshotProperties).build();
  }

  /**
   * A stable identifier for this sink, used to namespace the idempotency tokens written to each
   * commit's Iceberg snapshot summary ({@code beam.cdc.sink-id}, {@code
   * beam.cdc.committed-through-ms.<sinkId>}). Mostly useful when re-using the same {@code sinkId}
   * across subsequent streaming relaunches to maintain exactly-once commit idempotency from the
   * same source. If unset, a fresh UUID is used per launch.
   *
   * <p>Also useful in batch to make runs idempotent for the same {@code sinkId}. A stable {@code
   * sinkId} should only be used once though. In batch, the idempotency token is always
   * global-window end, so re-using it will lead to no-op for subsequent loads (they will be skipped
   * at commit time). Do not carry a <i>batch</i> job's {@code sinkId} into a <i>streaming</i> job,
   * as it would skip every commit window forever.
   */
  public WriteCdcRows withSinkId(String sinkId) {
    return toBuilder().setSinkId(sinkId).build();
  }

  /** The size of each event-time commit window (streaming only). */
  public WriteCdcRows withTriggeringFrequency(Duration triggeringFrequency) {
    return toBuilder().setTriggeringFrequency(triggeringFrequency).build();
  }

  /**
   * How far behind the watermark an element's event-time may be before it is dropped entirely,
   * otherwise routed to {@link IcebergWriteResult#getDeadLetterRows()} as a late firing. Defaults
   * to {@link #DEFAULT_ALLOWED_LATENESS} (6 hours) if unset.
   */
  public WriteCdcRows withAllowedLateness(Duration allowedLateness) {
    return toBuilder().setAllowedLateness(allowedLateness).build();
  }

  private CdcWriteConfig cdcWriteConfig() {
    @Nullable Integer shardsPerPartition = getShardsPerPartition();
    return CdcWriteConfig.builder()
        .setEqualityColumns(getEqualityColumns())
        .setSequenceNumberColumn(getSequenceNumberColumn())
        .setChangeTypeColumn(getChangeTypeColumn())
        .setChangeTypeMap(getChangeTypeMap())
        .setNumShards(getNumShards())
        .setShardsPerPartition(shardsPerPartition == null ? getNumShards() : shardsPerPartition)
        .setSorterMemoryMB(getSorterMemoryMB())
        .setUpsert(getUpsert())
        .setTokenHeartbeatMillis(getTokenHeartbeatMillis())
        .setSinkId(getSinkId())
        .setSnapshotProperties(getSnapshotProperties())
        .setErrorHandling(getErrorHandlingEnabled())
        .build();
  }

  /** Construction-time validation. */
  private void validate(PCollection<Row> input, CdcWriteConfig config) {
    Preconditions.checkArgument(
        1
            == Stream.of(getTableIdentifier(), getDynamicDestinations())
                .filter(Predicates.notNull())
                .count(),
        "Must set exactly one of to(TableIdentifier) or to(DynamicDestinations).");

    // Structural config invariants (value bounds, mutually exclusive options, reserved names).
    config.validate();

    boolean unbounded = input.isBounded() == PCollection.IsBounded.UNBOUNDED;
    @Nullable Duration triggeringFrequency = getTriggeringFrequency();
    if (unbounded) {
      if (triggeringFrequency == null) {
        throw new IllegalArgumentException(
            "Streaming CDC writes require withTriggeringFrequency(...)");
      }
      Preconditions.checkArgument(
          triggeringFrequency.isLongerThan(Duration.ZERO),
          "triggering frequency must be positive for streaming CDC writes, got %s",
          triggeringFrequency);
    }

    @Nullable String changeTypeColumn = config.getChangeTypeColumn();
    if (changeTypeColumn != null) {
      if (!input.getSchema().hasField(changeTypeColumn)) {
        throw new IllegalArgumentException(
            "CDC input schema is missing the change-type column '"
                + changeTypeColumn
                + "' named by change_type_column. Check the spelling, or drop change_type_column "
                + "to use native change kind. Input schema: "
                + input.getSchema());
      }
      Schema.FieldType changeTypeType = input.getSchema().getField(changeTypeColumn).getType();
      if (changeTypeType.getTypeName() != Schema.TypeName.STRING) {
        throw new IllegalArgumentException(
            "CDC change-type column '"
                + changeTypeColumn
                + "' must be STRING, but the input schema declares it "
                + changeTypeType
                + ".");
      }
      if (changeTypeType.getNullable()) {
        throw new IllegalArgumentException(
            "CDC change-type column '"
                + changeTypeColumn
                + "' must be non-nullable, but the input schema declares it nullable.");
      }
    }

    String seqCol = config.getSequenceNumberColumn();
    if (!input.getSchema().hasField(seqCol)) {
      throw new IllegalArgumentException(
          "CDC input schema is missing the sequence-number column '"
              + seqCol
              + "', required to order each primary key's changes. Input schema: "
              + input.getSchema());
    }
    Schema.FieldType seqType = input.getSchema().getField(seqCol).getType();
    if (seqType.getTypeName() != Schema.TypeName.INT64) {
      throw new IllegalArgumentException(
          "CDC sequence-number column '"
              + seqCol
              + "' must be INT64, but the input schema declares it "
              + seqType
              + ".");
    }
    if (seqType.getNullable()) {
      throw new IllegalArgumentException(
          "CDC sequence-number column '"
              + seqCol
              + "' must be non-nullable, but the input schema declares it nullable.");
    }

    // The idle token-refresh heartbeat is a streaming-only concept.
    if (!unbounded && config.getTokenHeartbeatMillis() != null) {
      LOG.warn(
          "Token heartbeat is ignored for bounded input. Batch loads will commit once and exit.");
    }
  }

  @Override
  public IcebergWriteResult expand(PCollection<Row> input) {
    CdcWriteConfig config = cdcWriteConfig();
    validate(input, config);

    @Nullable DynamicDestinations destinations = getDynamicDestinations();
    if (destinations == null) {
      destinations =
          DynamicDestinations.singleTable(
              checkStateNotNull(getTableIdentifier()), input.getSchema());
    }

    boolean unbounded = input.isBounded() == PCollection.IsBounded.UNBOUNDED;
    @Nullable Duration allowedLateness = getAllowedLateness();

    String filePrefix = UUID.randomUUID().toString();

    // Stage 1: destination + shard/sort-key assignment, and handle error rows
    PCollectionTuple assigned =
        input.apply(
            "AssignCdcKeys",
            new AssignCdcKeys(getCatalogConfig(), config, destinations, filePrefix));
    PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> keyed =
        assigned.get(AssignCdcKeys.KEYED);
    // getFailedRows() is null when error handling is disabled
    @Nullable
    PCollection<Row> failedRows =
        getErrorHandlingEnabled() ? assigned.get(AssignCdcKeys.FAILED) : null;

    // Stage 2: create commit windows, group by (destination, shard) and split late rows to the
    // replayable dead-letter, and sort groups by (pk, seq, kind)
    CommitWindows.Result windowed =
        keyed.apply(
            "CommitWindows",
            new CommitWindows(
                config,
                getTriggeringFrequency(),
                allowedLateness != null ? allowedLateness : DEFAULT_ALLOWED_LATENESS));

    // Stage 3: write delta files and emit their serialized metadata, one element per shard.
    PCollection<ShardDeltaFiles> written =
        windowed
            .getSortedGroups()
            .apply(
                "WriteDeltas",
                new WriteDeltas(getCatalogConfig(), config, destinations, filePrefix));

    // Commit stage: ordered, idempotent commit
    PCollection<KV<String, SnapshotInfo>> snapshots =
        written.apply(
            "CommitDeltas",
            new CommitDeltas(
                getCatalogConfig(),
                getSinkId(),
                config.getSnapshotProperties(),
                unbounded ? config.getTokenHeartbeatMillis() : null,
                filePrefix));

    return IcebergWriteResult.cdc(
        input.getPipeline(), snapshots, windowed.getDeadLetterRows(), failedRows);
  }
}
