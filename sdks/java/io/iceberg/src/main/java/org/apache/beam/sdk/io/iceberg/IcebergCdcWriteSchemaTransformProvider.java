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

import static org.apache.beam.sdk.io.iceberg.IcebergCdcWriteSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.io.iceberg.cdc.sink.WriteCdcRows;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * SchemaTransform implementation for {@link IcebergIO#writeCdcRows}. Applies a stream (or batch) of
 * row-level changes ({@code INSERT}/{@code UPDATE_BEFORE}/{@code UPDATE_AFTER}/{@code DELETE}) to
 * one or more Iceberg V2+ tables.
 *
 * <p>Outputs a {@code snapshots} {@code PCollection<Row>} representing the snapshots produced in
 * the process (mirroring {@link IcebergWriteSchemaTransformProvider}'s output), and a {@code
 * dead_letter} {@code PCollection<Row>} of replayable late records (see {@link
 * IcebergWriteResult#getDeadLetterRows()}).
 *
 * <p><b>Change kind for cross-language pipelines.</b> A cross-language pipeline (Python, Go, …)
 * cannot attach a native Beam {@code ValueKind} to each element, so it must carry the change kind
 * in a data column and set {@code change_type_column} (optionally with {@code change_type_map} to
 * map source op codes to {@code ValueKind} names). {@code change_type_column} is therefore
 * effectively required from those SDKs; without it every record defaults to {@code INSERT}.
 *
 * <p><b>Column flow.</b> The user's {@code keep}/{@code drop}/{@code only} projection is applied as
 * a {@code ParDo} <i>upstream</i> of the sink (envelope trimming), and it always preserves the
 * control columns (the {@code change_type_column} and the sequence-number column), which the sink
 * itself consumes and then strips from the written rows: input → projection (user filter, control
 * columns preserved) → sink (consumes + strips control columns). {@link WriteCdcRows} never calls
 * {@link DynamicDestinations#getData}/{@link DynamicDestinations#getDataSchema} (destinations are
 * routing-only), so a destination-side filter would be inert; the upstream projection also means
 * single-table users' filters are honored. For the {@code only} projection the top-level control
 * columns are re-appended after the extracted payload row's fields (the Debezium {@code only=after}
 * + {@code change_type_column=op} pattern).
 *
 * <p>For a templated {@code table} the provider routes via a {@link PortableIcebergDestinations}
 * built over the <i>post-projection</i> schema, so template placeholders must reference columns
 * that survive the projection (control columns included). Only its routing methods are consulted;
 * when a destination table does not exist, the sink auto-creates it from the post-projection input
 * schema minus the control columns, honoring only the destination's partition spec, sort order, and
 * table properties (none of which this provider configures); the create-config schema built by
 * {@code PortableIcebergDestinations#instantiateDestination} is ignored.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergCdcWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {

  static final String INPUT_TAG = "input";
  static final String SNAPSHOTS_TAG = "snapshots";
  static final String DEAD_LETTER_TAG = "dead_letter";

  static final Schema OUTPUT_SCHEMA = IcebergWriteSnapshotOutput.OUTPUT_SCHEMA;

  /** The default sequence-number column when {@code sequence_number_column} is unset. */
  private static final String DEFAULT_SEQUENCE_NUMBER_COLUMN =
      IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER;

  @Override
  public String description() {
    return "Applies a stream of CDC row-level changes (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE) "
        + "to Iceberg V2+ tables via equality deletes; superseded rows are never written.\n"
        + "Returns a 'snapshots' PCollection representing the snapshots produced in the process, "
        + "with the following schema:\n"
        + "{\"table\" (str), \"operation\" (str), \"summary\" (map[str, str]), \"manifestListLocation\" (str)}\n"
        + "and a 'dead_letter' PCollection of replayable late records.";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_IcebergCdcWriteSchemaTransformProvider_Configuration.Builder();
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
        "A list of field names to keep in the input record. All other fields are dropped before "
            + "writing. The change-type and sequence-number control columns are always preserved "
            + "and must not be listed. Is mutually exclusive with 'drop' and 'only'.")
    public abstract @Nullable List<String> getKeep();

    @SchemaFieldDescription(
        "A list of field names to drop from the input record before writing. The change-type and "
            + "sequence-number control columns are stripped automatically and must not be listed. "
            + "Is mutually exclusive with 'keep' and 'only'.")
    public abstract @Nullable List<String> getDrop();

    @SchemaFieldDescription(
        "The name of a single record field that should be written. The change-type and "
            + "sequence-number control columns are carried along automatically and must not be "
            + "named. Is mutually exclusive with 'keep' and 'drop'.")
    public abstract @Nullable String getOnly();

    @SchemaFieldDescription(
        "Columns defining row identity (the Iceberg equality-delete fields). Defaults to the "
            + "destination table's identifier (primary-key) fields.")
    public abstract @Nullable List<String> getEqualityColumns();

    @SchemaFieldDescription(
        "The column holding the per-primary-key monotonic sequence number used to order a single "
            + "key's changes. Required as a non-nullable INT64 in the input schema. Defaults to "
            + "_commit_snapshot_sequence_number. Event timestamps must be non-decreasing with "
            + "this column per key; violations are counted by crossWindowSequenceInversions.")
    public abstract @Nullable String getSequenceNumberColumn();

    @SchemaFieldDescription(
        "If set, read the change kind from this non-nullable string column (stripped from the "
            + "data row and never written to Iceberg) instead of the element's native ValueKind, "
            + "for SDKs without ValueKind support.")
    public abstract @Nullable String getChangeTypeColumn();

    @SchemaFieldDescription(
        "Optional map from change_type_column value to a ValueKind name "
            + "(INSERT|UPDATE_BEFORE|UPDATE_AFTER|DELETE). If omitted, the change_type_column "
            + "value must already be a ValueKind name. Represented as Map<String,String> for "
            + "cross-language compatibility.")
    public abstract @Nullable Map<String, String> getChangeTypeMap();

    @SchemaFieldDescription(
        "The number of deterministic primary-key-hash shards (logical write buckets) per "
            + "destination. Max write parallelism per destination. Defaults to 16; set it to "
            + "about your pipeline's write parallelism. Too low bottlenecks writes (visible as a "
            + "growing commit backlog); too high multiplies the sink's file count, which has no "
            + "symptom until reads and compaction slow down.")
    public abstract @Nullable Integer getNumShards();

    @SchemaFieldDescription(
        "Maximum number of shards a single partition's rows may occupy. Defaults to num_shards"
            + " (no cap). Lower values write proportionally fewer files per commit at the cost of"
            + " per-partition write parallelism; 1 pins each partition to one writer. Ignored for"
            + " unpartitioned tables. Must be between 1 and num_shards.")
    public abstract @Nullable Integer getShardsPerPartition();

    // NOTE: "Mb", not "MB": the config field name is CaseFormat-derived, and "sorterMemoryMB"
    // would snake_case to "sorter_memory_m_b". Do not rename to match
    // WriteCdcRows#withSorterMemoryMB.
    @SchemaFieldDescription(
        "The in-memory buffer size (MB) for the pre-write sort; groups larger than this "
            + "spill to disk. Must be >= 1. Defaults to 100.")
    public abstract @Nullable Integer getSorterMemoryMb();

    @SchemaFieldDescription(
        "If true, only the after-image of each change (INSERT/UPDATE_AFTER) is applied as an "
            + "upsert (equality-delete-then-insert on the primary key); UPDATE_BEFORE records are "
            + "dropped. Requires every partition source column to be an equality column. Defaults "
            + "to false.")
    public abstract @Nullable Boolean getUpsert();

    @SchemaFieldDescription(
        "A stable identifier for this sink, used to namespace the idempotency tokens written to "
            + "each commit's Iceberg snapshot summary. Set this explicitly (and keep it stable "
            + "across relaunches) for cross-relaunch exactly-once commit idempotency. Defaults to "
            + "a per-write UUID. BATCH SEMANTICS: in batch all data commits under one global "
            + "window, so a stable sink_id makes reruns of the SAME load idempotent, but makes "
            + "DIFFERENT loads no-ops (a rerun writes nothing). For periodic batch loads, either "
            + "omit sink_id (per-run UUID) or use a per-load value (e.g. suffix the load date).")
    public abstract @Nullable String getSinkId();

    @SchemaFieldDescription(
        "The size of each event-time commit window, in seconds. Required for streaming "
            + "(unbounded) input; ignored for batch input.")
    public abstract @Nullable Integer getTriggeringFrequencySeconds();

    @SchemaFieldDescription(
        "How long (in seconds) a late record may lag behind the watermark before it is dropped "
            + "entirely, rather than routed to the dead_letter output. Defaults to 21600 (6 hours). "
            + "A larger bound retains more live window state per destination on stateful runners.")
    public abstract @Nullable Integer getAllowedLatenessSeconds();

    @SchemaFieldDescription(
        "This option specifies whether and where to output per-record poison rows (null or "
            + "missing sequence value on any kind, unknown change type, null equality value, "
            + "unresolvable destination) instead of failing the pipeline. Distinct from the "
            + "dead_letter output (late-but-valid rows).")
    public abstract @Nullable ErrorHandling getErrorHandling();

    @SchemaFieldDescription(
        "Extra key/value properties to add to every commit's Iceberg snapshot summary. Keys "
            + "prefixed with 'beam.cdc.' are reserved and rejected.")
    public abstract @Nullable Map<String, String> getSnapshotProperties();

    @SchemaFieldDescription(
        "Streaming only; ignored for batch. If set (> 0), each destination that has committed at "
            + "least once emits a periodic empty token-refresh commit while idle, every this many "
            + "seconds, so its committed-through snapshot stays recent and is less likely to be "
            + "lost to expire_snapshots before the sink resumes. Unset (the default) disables it.")
    public abstract @Nullable Integer getTokenHeartbeatSeconds();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogName(String catalogName);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Builder setConfigProperties(Map<String, String> confProperties);

      public abstract Builder setKeep(List<String> keep);

      public abstract Builder setDrop(List<String> drop);

      public abstract Builder setOnly(String only);

      public abstract Builder setEqualityColumns(List<String> equalityColumns);

      public abstract Builder setSequenceNumberColumn(String sequenceNumberColumn);

      public abstract Builder setChangeTypeColumn(String changeTypeColumn);

      public abstract Builder setChangeTypeMap(Map<String, String> changeTypeMap);

      public abstract Builder setNumShards(Integer numShards);

      public abstract Builder setShardsPerPartition(Integer shardsPerPartition);

      public abstract Builder setSorterMemoryMb(Integer sorterMemoryMb);

      public abstract Builder setUpsert(Boolean upsert);

      public abstract Builder setSinkId(String sinkId);

      public abstract Builder setTriggeringFrequencySeconds(Integer triggeringFrequencySeconds);

      public abstract Builder setAllowedLatenessSeconds(Integer allowedLatenessSeconds);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setSnapshotProperties(Map<String, String> snapshotProperties);

      public abstract Builder setTokenHeartbeatSeconds(Integer tokenHeartbeatSeconds);

      public abstract Configuration build();
    }

    public IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogName(getCatalogName())
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new IcebergCdcWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList(SNAPSHOTS_TAG, DEAD_LETTER_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_CDC_WRITE);
  }

  static class IcebergCdcWriteSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    IcebergCdcWriteSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      return IcebergWriteSnapshotOutput.configurationRow(configuration, Configuration.class);
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);

      String table = configuration.getTable();
      @Nullable List<String> drop = configuration.getDrop();
      @Nullable List<String> keep = configuration.getKeep();
      @Nullable String only = configuration.getOnly();
      @Nullable String changeTypeColumn = configuration.getChangeTypeColumn();
      @Nullable String configuredSeq = configuration.getSequenceNumberColumn();
      String seqColumn = configuredSeq != null ? configuredSeq : DEFAULT_SEQUENCE_NUMBER_COLUMN;
      boolean projectionConfigured = drop != null || keep != null || only != null;

      validateProjectionConfig(keep, drop, only, changeTypeColumn, seqColumn);

      // Apply the user's projection upstream of the sink (see the class Javadoc's column flow).
      if (projectionConfigured) {
        UserProjection projection =
            UserProjection.of(
                rows.getSchema(),
                keep,
                drop,
                only,
                controlColumnsPresent(rows.getSchema(), changeTypeColumn, seqColumn));
        rows =
            rows.apply("ApplyUserProjection", ParDo.of(new ApplyUserProjectionFn(projection)))
                .setRowSchema(projection.outputSchema());
      }

      WriteCdcRows writeTransform = IcebergIO.writeCdcRows(configuration.getIcebergCatalog());

      if (table.contains("{")) {
        // Templated destination: routing/auto-create only (see the class Javadoc), over the
        // post-projection schema the sink interpolates against.
        try {
          writeTransform =
              writeTransform.to(
                  new PortableIcebergDestinations(
                      table,
                      FileFormat.PARQUET.toString(),
                      rows.getSchema(),
                      /* partitionFields= */ null,
                      /* sortFields= */ null,
                      /* tableProperties= */ null,
                      /* fieldsToDrop= */ null,
                      /* fieldsToKeep= */ null,
                      /* onlyField= */ null));
        } catch (IllegalArgumentException e) {
          if (projectionConfigured) {
            throw new IllegalArgumentException(
                "Invalid destination template '"
                    + table
                    + "': "
                    + e.getMessage()
                    + " Note: the template is resolved against the projected input schema, so a "
                    + "placeholder may reference a field removed by the configured keep/drop/only "
                    + "projection; template fields must survive it.",
                e);
          }
          throw e;
        }
      } else {
        writeTransform = writeTransform.to(TableIdentifier.parse(table));
      }

      IcebergWriteResult result = rows.apply(withConfiguredOptions(writeTransform));

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new IcebergWriteSnapshotOutput.SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);

      PCollection<Row> deadLetter = checkStateNotNull(result.getDeadLetterRows());

      PCollectionRowTuple output =
          PCollectionRowTuple.of(SNAPSHOTS_TAG, snapshots).and(DEAD_LETTER_TAG, deadLetter);
      @Nullable ErrorHandling errorHandling = configuration.getErrorHandling();
      if (ErrorHandling.hasOutput(errorHandling)) {
        output =
            output.and(
                checkStateNotNull(errorHandling).getOutput(),
                checkStateNotNull(result.getFailedRows()));
      }
      return output;
    }

    /**
     * Rejects a projection that names either control column (the change-type column and the
     * sequence-number column) in any of {@code keep}/{@code drop}/{@code only}. Dropping one would
     * starve the sink of a column it consumes; naming one in a whitelist is a confusing no-op,
     * since a control column is stripped before writing and can never be a table column.
     */
    private static void validateProjectionConfig(
        @Nullable List<String> keep,
        @Nullable List<String> drop,
        @Nullable String only,
        @Nullable String changeTypeColumn,
        String seqColumn) {
      Preconditions.checkArgument(
          drop == null || !drop.contains(seqColumn),
          "drop must not contain sequence_number_column '%s': it is a control column consumed by "
              + "the sink to order each key's changes, and stripped from the written rows "
              + "automatically.",
          seqColumn);
      Preconditions.checkArgument(
          (keep == null || !keep.contains(seqColumn)) && !seqColumn.equals(only),
          "sequence_number_column '%s' must not be named by 'keep' or 'only': it is a control "
              + "column stripped before writing, and can never be a table column.",
          seqColumn);
      if (changeTypeColumn != null) {
        Preconditions.checkArgument(
            drop == null || !drop.contains(changeTypeColumn),
            "drop must not contain change_type_column '%s': it is a control column consumed by "
                + "the sink to resolve each record's change kind, and stripped from the written "
                + "rows automatically.",
            changeTypeColumn);
        Preconditions.checkArgument(
            (keep == null || !keep.contains(changeTypeColumn)) && !changeTypeColumn.equals(only),
            "change_type_column '%s' must not be named by 'keep' or 'only': it is a control "
                + "column stripped before writing, and can never be a table column.",
            changeTypeColumn);
      }
    }

    /** Threads every set (non-null) configuration option onto {@code write}. */
    private WriteCdcRows withConfiguredOptions(WriteCdcRows write) {
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

      @Nullable Integer numShards = configuration.getNumShards();
      if (numShards != null) {
        write = write.withNumShards(numShards);
      }

      @Nullable Integer shardsPerPartition = configuration.getShardsPerPartition();
      if (shardsPerPartition != null) {
        write = write.withShardsPerPartition(shardsPerPartition);
      }

      @Nullable Integer sorterMemoryMb = configuration.getSorterMemoryMb();
      if (sorterMemoryMb != null) {
        write = write.withSorterMemoryMB(sorterMemoryMb);
      }

      @Nullable Boolean upsert = configuration.getUpsert();
      if (upsert != null) {
        write = write.withUpsert(upsert);
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
  }

  /**
   * The control columns actually present in {@code inputSchema}, in {@code [change-type, sequence]}
   * order (each is normally present; the sink's own validation fails otherwise).
   */
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

  /**
   * The user's {@code keep}/{@code drop}/{@code only} projection, applied upstream of the sink with
   * the control columns preserved:
   *
   * <ul>
   *   <li>{@code drop}: a plain {@link RowFilter} drop; the control columns survive because they
   *       are rejected from the drop list at construction;
   *   <li>{@code keep}: a {@link RowFilter} keep over the user's list plus the control columns;
   *   <li>{@code only}: the named nested payload row's fields, with the <i>top-level</i> control
   *       columns re-appended after them (a plain {@code RowFilter#only} would lose them). A null
   *       payload row (e.g. a Debezium DELETE with {@code after=null}) fails loudly: re-shape such
   *       envelopes upstream.
   * </ul>
   */
  static class UserProjection implements Serializable {
    /** Unused in the {@code only} case, where it serves only to derive the payload schema. */
    private final RowFilter filter;

    private final @Nullable String onlyField;
    private final List<String> appendedControlColumns;
    private final Schema outputSchema;

    /** The input schema this projection was built for; {@link #positions} are indices into it. */
    private final Schema inputSchema;

    /**
     * For each {@link #outputSchema} field, its position in {@link #inputSchema} (the {@code
     * keep}/{@code drop} fast path), or {@code null} when the output is not a plain positional
     * subset of the input (the {@code only} case, or a field whose type the filter rewrote).
     *
     * <p>This exists because {@link RowFilter#filter} is per-record expensive in a way that scales
     * with column count: it re-verifies the row's schema against the filter's with a full
     * structural {@code assignableTo} walk, then rebuilds the row through a {@code HashMap} keyed
     * by field name, resolving a {@code FieldAccessDescriptor} per field. On a wide table that is
     * hundreds of name hashes and allocations per record, on the default path for every
     * Managed/YAML/Python user who configures {@code keep} or {@code drop}. The positional copy is
     * the same shape the sink's own {@code TableSetup.ProjectionPlan} uses immediately downstream
     * on these very rows.
     *
     * <p>One deliberate difference: {@code RowFilter}'s rebuild also <i>normalizes</i> values it
     * copies ({@code ByteBuffer} to {@code byte[]} for {@code BYTES}, any {@code AbstractInstant}
     * to {@code Instant} for {@code DATETIME}), which a positional copy does not. That only ever
     * mattered for rows carrying non-canonical values, which {@code Row.addValues}/coder-decoded
     * rows never do; a row built through the {@code @Internal} {@code attachValues} with such a
     * value already fails on the no-projection path, where nothing normalizes it either.
     */
    private final int @Nullable [] positions;

    private UserProjection(
        RowFilter filter,
        @Nullable String onlyField,
        List<String> appendedControlColumns,
        Schema outputSchema,
        Schema inputSchema) {
      this.filter = filter;
      this.onlyField = onlyField;
      this.appendedControlColumns = appendedControlColumns;
      this.outputSchema = outputSchema;
      this.inputSchema = inputSchema;
      this.positions = onlyField == null ? positionsIn(inputSchema, outputSchema) : null;
    }

    /**
     * For each {@code outputSchema} field, its index in {@code inputSchema}, or {@code null} if any
     * output field is not carried through unchanged (same name, identical {@link Schema.Field}), in
     * which case the caller must keep using {@link RowFilter}, which knows how to rewrite it.
     */
    private static int @Nullable [] positionsIn(Schema inputSchema, Schema outputSchema) {
      int[] positions = new int[outputSchema.getFieldCount()];
      for (int i = 0; i < outputSchema.getFieldCount(); i++) {
        Schema.Field field = outputSchema.getField(i);
        if (!inputSchema.hasField(field.getName())) {
          return null;
        }
        int position = inputSchema.indexOf(field.getName());
        if (!inputSchema.getField(position).equals(field)) {
          return null;
        }
        positions[i] = position;
      }
      return positions;
    }

    static UserProjection of(
        Schema inputSchema,
        @Nullable List<String> keep,
        @Nullable List<String> drop,
        @Nullable String only,
        List<String> controlColumns) {
      // RowFilter also enforces keep/drop/only mutual exclusivity and that every named field
      // exists in the input schema.
      RowFilter filter = new RowFilter(inputSchema);
      if (drop != null) {
        filter = filter.drop(drop);
      }
      if (keep != null) {
        LinkedHashSet<String> effectiveKeep = new LinkedHashSet<>(keep);
        effectiveKeep.addAll(controlColumns);
        filter = filter.keep(new ArrayList<>(effectiveKeep));
      }
      if (only == null) {
        return new UserProjection(
            filter, null, Collections.emptyList(), filter.outputSchema(), inputSchema);
      }
      filter = filter.only(only);
      Schema payloadSchema = filter.outputSchema();
      Schema.Builder outputSchema = Schema.builder().addFields(payloadSchema.getFields());
      for (String control : controlColumns) {
        // The control columns are appended to the extracted payload's fields, so a payload field
        // of the same name would collide; name the collision rather than letting Schema.Builder
        // throw an opaque "Duplicate field" error.
        Preconditions.checkArgument(
            !payloadSchema.hasField(control),
            "The 'only' field '%s' already contains a field named '%s', which collides with the "
                + "control column of that name carried alongside it. Rename the nested field, or "
                + "configure a different control column name.",
            only,
            control);
        outputSchema.addField(inputSchema.getField(control));
      }
      return new UserProjection(filter, only, controlColumns, outputSchema.build(), inputSchema);
    }

    Schema outputSchema() {
      return outputSchema;
    }

    /**
     * Whether {@code schema} is the schema {@link #positions} were resolved against. A row that
     * fails this (a drifting source schema) takes the {@link RowFilter} path, which re-validates it
     * and reports the mismatch itself.
     */
    @SuppressWarnings("ReferenceEquality")
    private boolean matchesInput(Schema schema) {
      return inputSchema == schema || inputSchema.equals(schema);
    }

    Row apply(Row row) {
      if (onlyField == null) {
        int @Nullable [] copyFrom = positions;
        if (copyFrom == null || !matchesInput(row.getSchema())) {
          return filter.filter(row);
        }
        List<@Nullable Object> values = new ArrayList<>(copyFrom.length);
        for (int position : copyFrom) {
          values.add(row.getValue(position));
        }
        return Row.withSchema(outputSchema).attachValues(values);
      }
      @Nullable Row payload = row.getRow(onlyField);
      if (payload == null) {
        throw new IllegalStateException(
            "The 'only' field '"
                + onlyField
                + "' is null for an input row; a null payload cannot be written. Re-shape such "
                + "records upstream (e.g. a Debezium DELETE carries its data in 'before', not "
                + "'after').");
      }
      List<@Nullable Object> values = new ArrayList<>(outputSchema.getFieldCount());
      for (int i = 0; i < payload.getSchema().getFieldCount(); i++) {
        values.add(payload.getValue(i));
      }
      for (String control : appendedControlColumns) {
        values.add(row.getValue(control));
      }
      return Row.withSchema(outputSchema).attachValues(values);
    }
  }

  /** Applies a {@link UserProjection} to each row, preserving the element's {@link ValueKind}. */
  private static class ApplyUserProjectionFn extends DoFn<Row, Row> {
    private final UserProjection projection;

    ApplyUserProjectionFn(UserProjection projection) {
      this.projection = projection;
    }

    @ProcessElement
    public void process(@Element Row row, ValueKind kind, OutputReceiver<Row> out) {
      out.builder(projection.apply(row)).setValueKind(kind).output();
    }
  }
}
