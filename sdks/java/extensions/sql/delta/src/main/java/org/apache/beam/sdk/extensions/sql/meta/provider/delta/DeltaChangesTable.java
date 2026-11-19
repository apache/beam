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
package org.apache.beam.sdk.extensions.sql.meta.provider.delta;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableImpl;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ManagedOptions;
import org.apache.beam.sdk.io.delta.DeltaCdcReadSchemaTransformProvider;
import org.apache.beam.sdk.io.delta.DeltaIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The changes of a Delta Lake table, for a {@code CHANGES(...)} call, read from the table's Change
 * Data Feed through {@link Managed#DELTA_LAKE_CDC}. Rows carry their change kind as their {@code
 * ValueKind}. The metadata columns {@code _change_type}, {@code _commit_version} and {@code
 * _commit_timestamp} are appended when the query names them or the {@code include_metadata_columns}
 * option lists them. {@code _change_type} holds the change types of Delta Lake: {@code insert},
 * {@code update_preimage}, {@code update_postimage} and {@code delete}.
 *
 * <p>A Delta Lake table only records its changes from the version that enabled Change Data Feed on
 * it, so the range needs a start. When the pipeline is built, the range is resolved to the versions
 * it holds, and the read is pinned to them: changes committed later are not read. A version bound
 * must name a version that exists. A timestamp range that holds no versions reads no rows.
 */
class DeltaChangesTable extends SchemaBaseBeamTable {
  private static final Schema READ_CONFIG_SCHEMA =
      new DeltaCdcReadSchemaTransformProvider().configurationSchema();
  private static final String INCLUDE_METADATA_COLUMNS = "include_metadata_columns";
  private static final String START_VERSION = "start_version";
  private static final String END_VERSION = "end_version";

  private static final Map<String, String> OWNED_OPTIONS =
      ImmutableMap.<String, String>builder()
          .putAll(ManagedOptions.owned("the SQL table sets it", "table", "hadoop_config"))
          .putAll(
              ManagedOptions.owned(
                  "CHANGES' arguments set the range",
                  START_VERSION,
                  END_VERSION,
                  "start_timestamp",
                  "end_timestamp"))
          .build();

  private final DeltaTable table;
  private final ChangeRange range;
  private final Map<String, Object> config;

  private DeltaChangesTable(
      Schema schema, DeltaTable table, ChangeRange range, Map<String, Object> config) {
    super(schema);
    this.table = table;
    this.range = range;
    this.config = config;
  }

  static DeltaChangesTable of(
      DeltaTable table, ChangeRange range, Map<String, Object> options, List<String> named) {
    Map<String, Object> checked =
        ManagedOptions.check(options, READ_CONFIG_SCHEMA, OWNED_OPTIONS, "a CHANGES read");
    checkArgument(
        table.version == null && table.timestamp == null,
        "CHANGES cannot read Delta Lake table '%s', which pins the version it reads with the '%s'"
            + " or '%s' table property. Register the table without them to read its changes.",
        table.tableLocation,
        DeltaTable.VERSION_FIELD,
        DeltaTable.TIMESTAMP_FIELD);
    checkArgument(
        range.startVersion() != null || range.startTimestamp() != null,
        "CHANGES on Delta Lake table '%s' needs a start: a version, as in CHANGES(t, 2), or a"
            + " timestamp, as in CHANGES(t, '2026-01-01'). A Delta Lake table only records its"
            + " changes from the version that enabled Change Data Feed on it.",
        table.tableLocation);

    Schema tableSchema = table.getSchema();
    List<String> metadataColumns = metadataColumns(checked, named, tableSchema);
    Schema.Builder schema = Schema.builder().addFields(tableSchema.getFields());
    metadataColumns.forEach(column -> schema.addField(metadataField(column)));

    Map<String, Object> config = new LinkedHashMap<>(table.getBaseConfig());
    config.putAll(checked);
    config.put(INCLUDE_METADATA_COLUMNS, metadataColumns);

    return new DeltaChangesTable(schema.build(), table, range, ImmutableMap.copyOf(config));
  }

  /** The metadata columns that the option lists or the query names, in their order in DeltaIO. */
  @SuppressWarnings("unchecked")
  private static List<String> metadataColumns(
      Map<String, Object> options, List<String> named, Schema tableSchema) {
    List<String> requested =
        (List<String>) options.getOrDefault(INCLUDE_METADATA_COLUMNS, ImmutableList.of());
    for (String column : requested) {
      checkArgument(
          DeltaIO.METADATA_COLUMNS.contains(column),
          "Unknown metadata column '%s'. Supported metadata columns: %s",
          column,
          DeltaIO.METADATA_COLUMNS);
      checkArgument(
          !tableSchema.hasField(column),
          "Metadata column '%s' collides with a column of the table",
          column);
    }
    return DeltaIO.METADATA_COLUMNS.stream()
        .filter(column -> requested.contains(column) || named.contains(column))
        .collect(ImmutableList.toImmutableList());
  }

  private static Schema.Field metadataField(String column) {
    switch (column) {
      case DeltaIO.CHANGE_TYPE_COLUMN:
        return Schema.Field.of(column, FieldType.STRING);
      case DeltaIO.COMMIT_VERSION_COLUMN:
        return Schema.Field.of(column, FieldType.INT64);
      case DeltaIO.COMMIT_TIMESTAMP_COLUMN:
        return Schema.Field.of(column, FieldType.DATETIME);
      default:
        throw new IllegalArgumentException("Unknown metadata column: " + column);
    }
  }

  /** The Managed configuration of the CDC read, without the range. */
  @VisibleForTesting
  Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    @Nullable Map<String, Object> versions = resolveVersions();
    if (versions == null) {
      return begin.apply("Read Delta Lake changes", Create.empty(getSchema()));
    }
    Map<String, Object> readConfig =
        ImmutableMap.<String, Object>builder().putAll(config).putAll(versions).build();
    return begin
        .apply(
            "Read Delta Lake changes", Managed.read(Managed.DELTA_LAKE_CDC).withConfig(readConfig))
        .getSinglePCollection();
  }

  /**
   * The {@code start_version} and {@code end_version} of the versions the range holds, or null if
   * it holds none.
   */
  private @Nullable Map<String, Object> resolveVersions() {
    Configuration conf = new Configuration();
    if (table.hadoopConfig != null) {
      table.hadoopConfig.forEach(conf::set);
    }
    Engine engine = DefaultEngine.create(conf);
    TableImpl deltaTable = (TableImpl) Table.forPath(engine, table.tableLocation);
    Snapshot latest = deltaTable.getLatestSnapshot(engine);
    long latestVersion = latest.getVersion();

    long start;
    long end;
    @Nullable Long startVersion = range.startVersion();
    if (startVersion != null) {
      @Nullable Long endVersion = range.endVersion();
      start = startVersion;
      end = endVersion != null ? endVersion : latestVersion;
      long last = Math.max(start, end);
      checkArgument(
          last <= latestVersion,
          "Delta Lake table '%s' has no version %s. Its latest version is %s.",
          table.tableLocation,
          last,
          latestVersion);
      checkArgument(
          start <= end, "CHANGES start version %s is after its end version %s.", start, end);
    } else {
      Instant startTimestamp = checkStateNotNull(range.startTimestamp());
      @Nullable Instant endTimestamp = range.endTimestamp();
      checkArgument(
          endTimestamp == null || !startTimestamp.isAfter(endTimestamp),
          "CHANGES start timestamp %s is after its end timestamp %s.",
          startTimestamp,
          endTimestamp);
      // The kernel fails to resolve a timestamp past either end of the table's history, so check
      // for ranges that hold no versions first, rather than treating every failure as one.
      if (startTimestamp.getMillis() > latest.getTimestamp(engine)) {
        // Nothing was committed at or after the start.
        return null;
      }
      start = deltaTable.getVersionAtOrAfterTimestamp(engine, startTimestamp.getMillis());
      if (endTimestamp == null) {
        end = latestVersion;
      } else {
        if (deltaTable.getSnapshotAsOfVersion(engine, start).getTimestamp(engine)
            > endTimestamp.getMillis()) {
          // The first version committed at or after the start came after the end, which includes
          // an end earlier than every version of the table.
          return null;
        }
        end = deltaTable.getVersionBeforeOrAtTimestamp(engine, endTimestamp.getMillis());
      }
      if (end < start) {
        return null;
      }
    }
    return ImmutableMap.<String, Object>of(START_VERSION, start, END_VERSION, end);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException(
        String.format("CHANGES of Delta Lake table '%s' cannot be written.", table.tableLocation));
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }
}
