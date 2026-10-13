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

import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.io.iceberg.cdc.sink.WriteCdcRows;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * SchemaTransform implementation for {@link IcebergIO#writeRows} and, in {@code merge-on-read}
 * mode, {@link IcebergIO#writeCdcRows}. Outputs a {@code PCollection<Row>} representing the
 * snapshots created in the process; merge-on-read writes add a {@code dead_letter} output of late
 * records.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {

  static final String INPUT_TAG = "input";
  static final String SNAPSHOTS_TAG = "snapshots";
  static final String DEAD_LETTER_TAG = "dead_letter";
  static final String ERRORS_TAG = "errors";

  static final Schema OUTPUT_SCHEMA =
      Schema.builder()
          .addStringField("table")
          .addFields(SnapshotInfo.getSchema().getFields())
          .build();

  @Override
  public String description() {
    return "Writes Beam Rows to Iceberg, appending them by default. Set mode to 'merge-on-read' to "
        + "apply them as a stream of row-level changes (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE) "
        + "by primary key instead.\n"
        + "Returns a 'snapshots' PCollection representing the snapshots produced in the process, "
        + "with the following schema:\n"
        + "{\"table\" (str), \"operation\" (str), \"summary\" (map[str, str]), \"manifestListLocation\" (str)}\n"
        + "Merge-on-read mode also returns a 'dead_letter' PCollection representing late data, and an "
        + "'errors' PCollection representing invalid records.";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_IcebergWriteSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription(
        "A fully-qualified table identifier. You may also provide a template to write to multiple dynamic destinations,"
            + " for example: `dataset.my_{col1}_{col2.nested}_table`.")
    public abstract String getTable();

    @SchemaFieldDescription("Name of the catalog containing the table.")
    public abstract @Nullable String getCatalogName();

    @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
    public abstract @Nullable Map<String, String> getCatalogProperties();

    @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
    public abstract @Nullable Map<String, String> getConfigProperties();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the frequency at which snapshots are produced.")
    public abstract @Nullable Integer getTriggeringFrequencySeconds();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the limit for lifting bundles into the direct write path.")
    public abstract @Nullable Integer getDirectWriteByteLimit();

    @SchemaFieldDescription(
        "Controls how rows are written. 'append' (default) appends every row as new data. "
            + "'merge-on-read' treats each row as a change (INSERT, UPDATE_BEFORE, UPDATE_AFTER, "
            + "or DELETE) applied to the table by primary key.")
    public abstract @Nullable String getMode();

    @SchemaFieldDescription(
        "Merge-on-read only. The required column name representing the monotonic sequence number used to "
            + "order a single key's changes. Defaults to '_commit_snapshot_sequence_number'. This column will be "
            + "stripped from the data row before writing to Iceberg.")
    public abstract @Nullable String getSequenceNumberColumn();

    @SchemaFieldDescription(
        "Merge-on-read only. The optional column name representing the row's change type (INSERT, "
            + "UPDATE_BEFORE, UPDATE_AFTER,  or DELETE). This column will be stripped from the data row "
            + "before writing to Iceberg. If unset, the sink will use the element's native ValueKind")
    public abstract @Nullable String getChangeTypeColumn();

    @SchemaFieldDescription(
        "Merge-on-read only. Optional map from a change_type_column value to the canonical change "
            + "type name (see above).")
    public abstract @Nullable Map<String, String> getChangeTypeMap();

    @SchemaFieldDescription(
        "Merge-on-read only. If true, only the after-image of each change (INSERT/UPDATE_AFTER) "
            + "is applied, as an upsert; UPDATE_BEFORE records are dropped. Default: false.")
    public abstract @Nullable Boolean getUpsert();

    @SchemaFieldDescription(
        "A list of field names to keep in the input record. All other fields are dropped before writing. "
            + "Is mutually exclusive with 'drop' and 'only'. In merge-on-read mode the control columns are "
            + "dropped unless listed here.")
    public abstract @Nullable List<String> getKeep();

    @SchemaFieldDescription(
        "A list of field names to drop from the input record before writing. "
            + "Is mutually exclusive with 'keep' and 'only'. In merge-on-read mode the control columns are "
            + "always dropped.")
    public abstract @Nullable List<String> getDrop();

    @SchemaFieldDescription(
        "The name of a single record field that should be written. "
            + "Is mutually exclusive with 'keep' and 'drop'.")
    public abstract @Nullable String getOnly();

    @SchemaFieldDescription(
        "Fields used to create a partition spec that is applied when tables are created. For a field 'foo', "
            + "the available partition transforms are:\n\n"
            + "- `foo`\n"
            + "- `truncate(foo, N)`\n"
            + "- `bucket(foo, N)`\n"
            + "- `hour(foo)`\n"
            + "- `day(foo)`\n"
            + "- `month(foo)`\n"
            + "- `year(foo)`\n"
            + "- `void(foo)`\n\n"
            + "For more information on partition transforms, please visit https://iceberg.apache.org/spec/#partition-transforms.")
    public abstract @Nullable List<String> getPartitionFields();

    @SchemaFieldDescription(
        "Iceberg table properties to be set on the table when it is created.\n"
            + "For more information on table properties,"
            + " please visit https://iceberg.apache.org/docs/latest/configuration/#table-properties.")
    public abstract @Nullable Map<String, String> getTableProperties();

    @SchemaFieldDescription(
        "Fields used to set the table's sort order, applied when the table is created. "
            + "Each entry has the form `<term> [asc|desc] [nulls first|nulls last]`, where `<term>` "
            + "is a field name or one of the partition transforms (e.g. `bucket(col, 4)`, `day(ts)`). "
            + "Direction defaults to ascending; null order defaults to nulls-first for ascending and "
            + "nulls-last for descending. Note: this sets the table's declared sort order as metadata; "
            + "it does not cause Beam to physically sort records before writing.\n"
            + "For more information on sort orders, please visit https://iceberg.apache.org/spec/#sort-orders.")
    public abstract @Nullable List<String> getSortFields();

    @SchemaFieldDescription(
        "Defines distribution of write data. Supported distributions:"
            + "\n- none: don't shuffle rows (default)"
            + "\n- hash: shuffle rows by partition key before writing data")
    public abstract @Nullable String getDistributionMode();

    @SchemaFieldDescription(
        "Enables dynamic sharding to automatically adjust the number of parallel writers "
            + "based on data volume. It handles data skew "
            + "by further sub-dividing partitions into multiple shards to prevent bottlenecks "
            + "during high-throughput writes. Only available with 'hash' distribution mode.")
    public abstract @Nullable Boolean getAutosharding();

    @SchemaFieldDescription(
        "Properties applied to the underlying file writer (e.g. Parquet write properties like "
            + "'write.parquet.bloom-filter-enabled.column.<col>').")
    public abstract @Nullable Map<String, String> getWriteProperties();

    @SchemaFieldDescription(
        "Enables expirable side-input caching of Iceberg table metadata across workers to reduce catalog load.")
    public abstract @Nullable Boolean getUseSideInputTableCache();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the interval in seconds at which table metadata is refreshed from the catalog.")
    public abstract @Nullable Integer getTableCacheRefreshIntervalSeconds();

    @SchemaFieldDescription(
        "For a batch pipeline, sets the maximum number of table metadata specs to cache in memory. "
            + "Tables exceeding this limit fall back to worker-local catalog loading.")
    public abstract @Nullable Integer getMaximumTableCacheSize();

    @SchemaFieldDescription(
        "Sets the number of parallel buckets/workers used to query the Iceberg catalog during refreshes. Defaults to 1.")
    public abstract @Nullable Integer getTableCachePollingBuckets();

    @SchemaFieldDescription(
        "Columns defining row identity (equality-delete fields). Defaults to the destination table's "
            + "identifier (primary-key) fields. Required if the table doesn't exist yet. "
            + "Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable List<String> getEqualityColumns();

    @SchemaFieldDescription(
        "The number of deterministic primary-key-hash shards per destination, i.e. the max "
            + "write parallelism per destination. Too low may bottleneck writes, and too high may "
            + "produce more files. Defaults to 16. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Integer getNumShards();

    @SchemaFieldDescription(
        "Maximum number of shards a single partition's rows may occupy. Lower values "
            + "write fewer files per commit, but also reduces per-partition write parallelism. A value "
            + "of 1 pins each partition to one writer. Ignored for unpartitioned tables. Must be between 1 "
            + "and `num_shards`; defaults to `num_shards`. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Integer getShardsPerPartition();

    @SchemaFieldDescription(
        "How long a late record may lag behind the watermark before it is "
            + "dropped entirely, rather than routed to the dead_letter output. Defaults to 21600 "
            + "(6 hours). Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Integer getAllowedLatenessSeconds();

    @SchemaFieldDescription(
        "A stable identifier for this sink, used to namespace the idempotency tokens "
            + "written to each commit's Iceberg snapshot summary. Defaults to a unique per-write UUID. "
            + "Set it explicitly (and keep it stable across relaunches) for exactly-once commits across "
            + "relaunches of a particular streaming write. A batch load with a stable sink_id "
            + "commits only once (later batch loads with the same sink_id are skipped). "
            + "Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable String getSinkId();

    @SchemaFieldDescription(
        "Streaming only. If set, the sink will emit a periodic empty token-refresh commit while idle, "
            + "so its thread of `sink_id` stamped snapshot stays recent and is less likely to be "
            + "lost to `expire_snapshots`. Disabled by default. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Integer getTokenHeartbeatSeconds();

    @SchemaFieldDescription(
        "Extra key/value properties to add to every commit's Iceberg snapshot summary. "
            + "Keys prefixed with 'beam.cdc.' are reserved and rejected. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Map<String, String> getSnapshotProperties();

    @SchemaFieldDescription(
        "Whether and where to output per-record invalid rows (null or missing sequence "
            + "value, unknown change type, null equality value, unresolvable destination). Fails the pipeline "
            + "if unset (default). Distinct from the `dead_letter` output, which is for late-but-valid "
            + "rows. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable ErrorHandling getErrorHandling();

    @SchemaFieldDescription(
        "The in-memory buffer size (MB) for the pre-write sort; groups larger than this "
            + "spill to disk. Must be >= 1. Defaults to 100. Currently only supported in 'merge-on-read' mode.")
    public abstract @Nullable Integer getSorterMemoryMb();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogName(String catalogName);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Builder setConfigProperties(Map<String, String> confProperties);

      public abstract Builder setTriggeringFrequencySeconds(Integer triggeringFrequencySeconds);

      public abstract Builder setDirectWriteByteLimit(Integer directWriteByteLimit);

      public abstract Builder setKeep(List<String> keep);

      public abstract Builder setDrop(List<String> drop);

      public abstract Builder setOnly(String only);

      public abstract Builder setPartitionFields(List<String> partitionFields);

      public abstract Builder setTableProperties(Map<String, String> tableProperties);

      public abstract Builder setSortFields(List<String> sortFields);

      public abstract Builder setDistributionMode(String mode);

      public abstract Builder setAutosharding(Boolean autosharding);

      public abstract Builder setWriteProperties(Map<String, String> writeProperties);

      public abstract Builder setUseSideInputTableCache(Boolean useSideInputTableCache);

      public abstract Builder setTableCacheRefreshIntervalSeconds(
          Integer tableCacheRefreshIntervalSeconds);

      public abstract Builder setMaximumTableCacheSize(Integer maximumTableCacheSize);

      public abstract Builder setTableCachePollingBuckets(Integer pollingBuckets);

      public abstract Builder setMode(String mode);

      public abstract Builder setSequenceNumberColumn(String sequenceNumberColumn);

      public abstract Builder setChangeTypeColumn(String changeTypeColumn);

      public abstract Builder setChangeTypeMap(Map<String, String> changeTypeMap);

      public abstract Builder setUpsert(Boolean upsert);

      public abstract Builder setEqualityColumns(List<String> equalityColumns);

      public abstract Builder setNumShards(Integer numShards);

      public abstract Builder setShardsPerPartition(Integer shardsPerPartition);

      public abstract Builder setAllowedLatenessSeconds(Integer allowedLatenessSeconds);

      public abstract Builder setSinkId(String sinkId);

      public abstract Builder setTokenHeartbeatSeconds(Integer tokenHeartbeatSeconds);

      public abstract Builder setSnapshotProperties(Map<String, String> snapshotProperties);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setSorterMemoryMb(Integer sorterMemoryMb);

      public abstract Configuration build();
    }

    public IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogName(getCatalogName())
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }

    enum Mode {
      APPEND("append"),
      MERGE_ON_READ("merge-on-read");

      /** The value users set {@code mode} to. */
      final String optionValue;

      Mode(String optionValue) {
        this.optionValue = optionValue;
      }
    }

    /** The write mode this configuration selects; unset means append. */
    Mode mode() {
      @Nullable String mode = getMode();
      if (mode == null || mode.equalsIgnoreCase(Mode.APPEND.optionValue)) {
        return Mode.APPEND;
      }
      if (mode.equalsIgnoreCase(Mode.MERGE_ON_READ.optionValue)) {
        return Mode.MERGE_ON_READ;
      }
      throw new IllegalArgumentException(
          String.format(
              "Unknown mode '%s'; expected '%s' or '%s'.",
              mode, Mode.APPEND.optionValue, Mode.MERGE_ON_READ.optionValue));
    }

    /** Rejects every set option that the selected mode does not support. */
    void validateModeOptions() {
      // Resolve the mode first so an unknown value fails here, not only once an option trips it.
      Mode mode = mode();
      List<String> unsupported = new ArrayList<>();
      // Merge-on-read only: the change-stream contract.
      requireMode(
          Mode.MERGE_ON_READ, "sequence_number_column", getSequenceNumberColumn(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "change_type_column", getChangeTypeColumn(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "change_type_map", getChangeTypeMap(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "upsert", getUpsert(), unsupported);
      // Merge-on-read only, until the append write grows these features.
      requireMode(Mode.MERGE_ON_READ, "equality_columns", getEqualityColumns(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "num_shards", getNumShards(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "shards_per_partition", getShardsPerPartition(), unsupported);
      requireMode(
          Mode.MERGE_ON_READ, "allowed_lateness_seconds", getAllowedLatenessSeconds(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "sink_id", getSinkId(), unsupported);
      requireMode(
          Mode.MERGE_ON_READ, "token_heartbeat_seconds", getTokenHeartbeatSeconds(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "snapshot_properties", getSnapshotProperties(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "error_handling", getErrorHandling(), unsupported);
      requireMode(Mode.MERGE_ON_READ, "sorter_memory_mb", getSorterMemoryMb(), unsupported);
      // Append only: merge-on-read has neither a direct-write path nor a side-input table cache.
      requireMode(Mode.APPEND, "direct_write_byte_limit", getDirectWriteByteLimit(), unsupported);
      requireMode(Mode.APPEND, "distribution_mode", getDistributionMode(), unsupported);
      requireMode(Mode.APPEND, "autosharding", getAutosharding(), unsupported);
      requireMode(Mode.APPEND, "write_properties", getWriteProperties(), unsupported);
      requireMode(
          Mode.APPEND, "using_side_input_table_cache", getUseSideInputTableCache(), unsupported);
      requireMode(
          Mode.APPEND,
          "table_refresh_interval_seconds",
          getTableCacheRefreshIntervalSeconds(),
          unsupported);
      requireMode(Mode.APPEND, "maximum_cache_size", getMaximumTableCacheSize(), unsupported);
      requireMode(Mode.APPEND, "polling_buckets", getTableCachePollingBuckets(), unsupported);
      if (!unsupported.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "The following options are not supported in '%s' mode yet: %s",
                mode.optionValue, unsupported));
      }
    }

    /** Records {@code option} as unsupported when it is set under a mode other than its own. */
    private void requireMode(
        Mode supported, String option, @Nullable Object value, List<String> unsupported) {
      if (value != null && mode() != supported) {
        unsupported.add(option);
      }
    }
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new IcebergWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList(SNAPSHOTS_TAG, DEAD_LETTER_TAG, ERRORS_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_WRITE);
  }

  static class IcebergWriteSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    IcebergWriteSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(Configuration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);
      configuration.validateModeOptions();
      return configuration.mode() == Configuration.Mode.MERGE_ON_READ
          ? expandCdc(rows)
          : expandAppend(rows);
    }

    private PCollectionRowTuple expandAppend(PCollection<Row> rows) {
      IcebergIO.WriteRows writeTransform =
          IcebergIO.writeRows(configuration.getIcebergCatalog())
              .to(
                  new PortableIcebergDestinations(
                      configuration.getTable(),
                      FileFormat.PARQUET.toString(),
                      rows.getSchema(),
                      configuration.getPartitionFields(),
                      configuration.getSortFields(),
                      configuration.getTableProperties(),
                      configuration.getDrop(),
                      configuration.getKeep(),
                      configuration.getOnly()));

      Integer trigFreq = configuration.getTriggeringFrequencySeconds();
      if (trigFreq != null) {
        writeTransform = writeTransform.withTriggeringFrequency(Duration.standardSeconds(trigFreq));
      }

      Integer directWriteByteLimit = configuration.getDirectWriteByteLimit();
      if (directWriteByteLimit != null) {
        writeTransform = writeTransform.withDirectWriteByteLimit(directWriteByteLimit);
      }

      @Nullable String mode = configuration.getDistributionMode();
      if (mode != null) {
        writeTransform = writeTransform.withDistributionMode(DistributionMode.fromName(mode));
      }

      @Nullable Boolean autoSharding = configuration.getAutosharding();
      if (autoSharding != null && autoSharding) {
        writeTransform = writeTransform.withAutosharding();
      }

      @Nullable Map<String, String> writeProperties = configuration.getWriteProperties();
      if (writeProperties != null && !writeProperties.isEmpty()) {
        writeTransform = writeTransform.withWriteProperties(writeProperties);
      }

      boolean hasSideInputOptions =
          configuration.getTableCacheRefreshIntervalSeconds() != null
              || configuration.getMaximumTableCacheSize() != null
              || configuration.getTableCachePollingBuckets() != null;

      if (!Boolean.TRUE.equals(configuration.getUseSideInputTableCache()) && hasSideInputOptions) {
        throw new IllegalArgumentException(
            "Cannot specify side-input cache sub-options (table_refresh_interval_seconds, "
                + "maximum_cache_size, polling_buckets) without explicitly setting using_side_input_table_cache to true.");
      }

      boolean enableSideInputCache = Boolean.TRUE.equals(configuration.getUseSideInputTableCache());

      if (enableSideInputCache) {
        writeTransform = writeTransform.withSideInputTableCache();
        @Nullable Integer refreshSec = configuration.getTableCacheRefreshIntervalSeconds();
        if (refreshSec != null) {
          writeTransform =
              writeTransform.withTableCacheRefreshInterval(Duration.standardSeconds(refreshSec));
        }
        @Nullable Integer maxCacheSize = configuration.getMaximumTableCacheSize();
        if (maxCacheSize != null) {
          writeTransform = writeTransform.withMaximumTableCacheSize(maxCacheSize);
        }
        @Nullable Integer pollingBuckets = configuration.getTableCachePollingBuckets();
        if (pollingBuckets != null) {
          writeTransform = writeTransform.withTableCachePollingBuckets(pollingBuckets);
        }
      }

      // TODO: support dynamic destinations
      IcebergWriteResult result = rows.apply(writeTransform);

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);

      return PCollectionRowTuple.of(SNAPSHOTS_TAG, snapshots);
    }

    private PCollectionRowTuple expandCdc(PCollection<Row> rows) {
      Schema inputSchema = rows.getSchema();
      @Nullable List<String> drop = configuration.getDrop();
      @Nullable List<String> keep = configuration.getKeep();
      @Nullable String only = configuration.getOnly();
      @Nullable String changeTypeColumn = configuration.getChangeTypeColumn();
      @Nullable String configuredSeq = configuration.getSequenceNumberColumn();
      String seqColumn =
          configuredSeq != null
              ? configuredSeq
              : IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER;

      // The sink reads the control columns from the raw element, so by default they are dropped
      // from the written row. Listing them in keep writes them too.
      if (keep == null && only == null) {
        Set<String> effectiveDrop =
            new LinkedHashSet<>(controlColumnsPresent(inputSchema, changeTypeColumn, seqColumn));
        if (drop != null) {
          effectiveDrop.addAll(drop);
        }
        drop = effectiveDrop.isEmpty() ? null : new ArrayList<>(effectiveDrop);
      }

      WriteCdcRows write =
          IcebergIO.writeCdcRows(configuration.getIcebergCatalog())
              .to(
                  new PortableIcebergDestinations(
                      configuration.getTable(),
                      FileFormat.PARQUET.toString(),
                      inputSchema,
                      configuration.getPartitionFields(),
                      configuration.getSortFields(),
                      configuration.getTableProperties(),
                      drop,
                      keep,
                      only));
      IcebergWriteResult result = rows.apply(applyOptions(write));

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);
      PCollectionRowTuple output =
          PCollectionRowTuple.of(SNAPSHOTS_TAG, snapshots)
              .and(DEAD_LETTER_TAG, result.getDeadLetterRows());
      @Nullable ErrorHandling errorHandling = configuration.getErrorHandling();
      if (ErrorHandling.hasOutput(errorHandling)) {
        output = output.and(checkStateNotNull(errorHandling).getOutput(), result.getFailedRows());
      }
      return output;
    }

    /** Threads every set option onto {@code write}. */
    private WriteCdcRows applyOptions(WriteCdcRows write) {
      @Nullable List<String> equalityColumns = configuration.getEqualityColumns();
      if (equalityColumns != null) {
        write = write.withEqualityColumns(equalityColumns);
      }
      @Nullable String sequenceNumberColumn = configuration.getSequenceNumberColumn();
      if (sequenceNumberColumn != null) {
        write = write.withSequenceNumberColumn(sequenceNumberColumn);
      }
      @Nullable String changeTypeColumn = configuration.getChangeTypeColumn();
      if (changeTypeColumn != null) {
        write = write.withChangeTypeColumn(changeTypeColumn);
      }
      @Nullable Map<String, String> changeTypeMap = configuration.getChangeTypeMap();
      if (changeTypeMap != null) {
        write = write.withChangeTypeMap(changeTypeMap);
      }
      @Nullable Boolean upsert = configuration.getUpsert();
      if (upsert != null) {
        write = write.withUpsert(upsert);
      }
      @Nullable Integer sorterMemoryMb = configuration.getSorterMemoryMb();
      if (sorterMemoryMb != null) {
        write = write.withSorterMemoryMB(sorterMemoryMb);
      }
      @Nullable Integer numShards = configuration.getNumShards();
      if (numShards != null) {
        write = write.withNumShards(numShards);
      }
      @Nullable Integer shardsPerPartition = configuration.getShardsPerPartition();
      if (shardsPerPartition != null) {
        write = write.withShardsPerPartition(shardsPerPartition);
      }
      @Nullable String sinkId = configuration.getSinkId();
      if (sinkId != null) {
        write = write.withSinkId(sinkId);
      }
      @Nullable Integer triggeringFrequencySeconds = configuration.getTriggeringFrequencySeconds();
      if (triggeringFrequencySeconds != null) {
        write = write.withTriggeringFrequency(Duration.standardSeconds(triggeringFrequencySeconds));
      }
      @Nullable Integer allowedLatenessSeconds = configuration.getAllowedLatenessSeconds();
      if (allowedLatenessSeconds != null) {
        write = write.withAllowedLateness(Duration.standardSeconds(allowedLatenessSeconds));
      }
      if (ErrorHandling.hasOutput(configuration.getErrorHandling())) {
        write = write.withErrorHandling();
      }
      @Nullable Map<String, String> snapshotProperties = configuration.getSnapshotProperties();
      if (snapshotProperties != null) {
        write = write.withSnapshotProperties(snapshotProperties);
      }
      @Nullable Integer tokenHeartbeatSeconds = configuration.getTokenHeartbeatSeconds();
      if (tokenHeartbeatSeconds != null) {
        write = write.withTokenHeartbeat(Duration.standardSeconds(tokenHeartbeatSeconds));
      }
      return write;
    }

    /** The control columns present in the input; a missing one is left to the sink to report. */
    private static List<String> controlColumnsPresent(
        Schema inputSchema, @Nullable String changeTypeColumn, String seqColumn) {
      List<String> controls = new ArrayList<>();
      if (changeTypeColumn != null && inputSchema.hasField(changeTypeColumn)) {
        controls.add(changeTypeColumn);
      }
      if (inputSchema.hasField(seqColumn)) {
        controls.add(seqColumn);
      }
      return controls;
    }

    @VisibleForTesting
    static class SnapshotToRow extends SimpleFunction<KV<String, SnapshotInfo>, Row> {
      @Override
      public Row apply(KV<String, SnapshotInfo> input) {
        SnapshotInfo snapshot = input.getValue();

        return Row.withSchema(OUTPUT_SCHEMA)
            .addValue(input.getKey())
            .addValues(snapshot.toRow().getValues())
            .build();
      }
    }
  }
}
