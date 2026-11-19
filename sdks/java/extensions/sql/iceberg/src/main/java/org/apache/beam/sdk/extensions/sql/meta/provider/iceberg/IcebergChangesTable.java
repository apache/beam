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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ManagedOptions;
import org.apache.beam.sdk.io.iceberg.IcebergCdcReadSchemaTransformProvider;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The changes of an Iceberg table, for a {@code CHANGES(...)} call, read through {@link
 * Managed#ICEBERG_CDC}. Rows carry their change kind as their {@code ValueKind}. The metadata
 * columns that the query names, and those that the {@code include_metadata_columns} option lists,
 * are appended to the table's columns.
 *
 * <p>The range becomes the CDC read's {@code from_snapshot} and {@code to_snapshot}, or its {@code
 * from_timestamp} and {@code to_timestamp}. The read checks them against the table when the
 * pipeline is built.
 */
class IcebergChangesTable extends SchemaBaseBeamTable {
  private static final Schema READ_CONFIG_SCHEMA =
      new IcebergCdcReadSchemaTransformProvider().configurationSchema();
  private static final String INCLUDE_METADATA_COLUMNS = "include_metadata_columns";
  private static final String STARTING_STRATEGY = "starting_strategy";
  private static final Map<String, String> OWNED_OPTIONS =
      ImmutableMap.<String, String>builder()
          .putAll(IcebergTable.TABLE_OPTIONS)
          .putAll(ManagedOptions.owned("the query selects the columns", "keep", "drop"))
          .putAll(ManagedOptions.owned("WHERE filters the rows", "filter"))
          .putAll(
              ManagedOptions.owned(
                  "CHANGES' arguments set the range",
                  "from_snapshot",
                  "to_snapshot",
                  "from_timestamp",
                  "to_timestamp"))
          .build();

  private final IcebergTable table;
  private final List<String> metadataColumns;
  private final Map<String, Object> config;

  private final boolean streaming;

  private IcebergChangesTable(
      Schema schema,
      IcebergTable table,
      List<String> metadataColumns,
      Map<String, Object> config,
      boolean streaming) {
    super(schema);
    this.table = table;
    this.metadataColumns = metadataColumns;
    this.config = config;
    this.streaming = streaming;
  }

  static IcebergChangesTable of(
      IcebergTable table, ChangeRange range, Map<String, Object> options, List<String> named) {
    Map<String, Object> checked =
        ManagedOptions.check(options, READ_CONFIG_SCHEMA, ownedOptions(range), "a CHANGES read");

    @Nullable List<String> primaryKey = table.primaryKey;
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "CHANGES needs a primary key, and Iceberg table '%s' has none. Declare one with"
                  + " PRIMARY KEY (...) NOT ENFORCED when creating the table, or set the table's"
                  + " identifier fields in the Iceberg catalog.",
              table.tableIdentifier));
    }

    Schema tableSchema = table.getSchema();
    List<String> metadataColumns = metadataColumns(checked, named, tableSchema);
    Schema.Builder schema = Schema.builder().addFields(tableSchema.getFields());
    metadataColumns.forEach(column -> schema.addField(IcebergCdcMetadataColumns.beamField(column)));

    Map<String, Object> config = new LinkedHashMap<>(table.getBaseConfig());
    putIfNotNull(config, "from_snapshot", range.startVersion());
    putIfNotNull(config, "to_snapshot", range.endVersion());
    putIfNotNull(config, "from_timestamp", millis(range.startTimestamp()));
    putIfNotNull(config, "to_timestamp", millis(range.endTimestamp()));
    config.putAll(checked);
    config.put(INCLUDE_METADATA_COLUMNS, metadataColumns);

    return new IcebergChangesTable(
        schema.build(),
        table,
        metadataColumns,
        ImmutableMap.copyOf(config),
        Boolean.TRUE.equals(checked.get("streaming")));
  }

  private static Map<String, String> ownedOptions(ChangeRange range) {
    if (range.startVersion() == null && range.startTimestamp() == null) {
      return OWNED_OPTIONS;
    }
    return ImmutableMap.<String, String>builder()
        .putAll(OWNED_OPTIONS)
        .put(STARTING_STRATEGY, "CHANGES' start bound sets the start")
        .build();
  }

  /** The metadata columns that the option lists or the query names, in the CDC source's order. */
  @SuppressWarnings("unchecked")
  private static List<String> metadataColumns(
      Map<String, Object> options, List<String> named, Schema tableSchema) {
    List<String> requested =
        (List<String>) options.getOrDefault(INCLUDE_METADATA_COLUMNS, ImmutableList.of());
    for (String column : requested) {
      checkArgument(
          IcebergCdcMetadataColumns.isSupportedColumn(column),
          "Unknown metadata column '%s'. Supported metadata columns: %s",
          column,
          IcebergCdcMetadataColumns.SUPPORTED_COLUMNS);
      checkArgument(
          !tableSchema.hasField(column),
          "Metadata column '%s' collides with a column of the table",
          column);
    }
    return IcebergCdcMetadataColumns.SUPPORTED_COLUMNS.stream()
        .filter(column -> requested.contains(column) || named.contains(column))
        .collect(ImmutableList.toImmutableList());
  }

  private static void putIfNotNull(Map<String, Object> config, String key, @Nullable Long value) {
    if (value != null) {
      config.put(key, value);
    }
  }

  private static @Nullable Long millis(@Nullable Instant timestamp) {
    return timestamp == null ? null : timestamp.getMillis();
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return read(begin, config);
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filter, List<String> fieldNames) {
    List<String> used = fieldNames.isEmpty() ? getSchema().getFieldNames() : fieldNames;
    List<String> columns =
        used.stream().filter(table.getSchema()::hasField).collect(Collectors.toList());
    Map<String, Object> readConfig = new LinkedHashMap<>(config);
    // an empty 'keep' would read every column
    readConfig.put("keep", columns.isEmpty() ? checkStateNotNull(table.primaryKey) : columns);
    readConfig.put(
        INCLUDE_METADATA_COLUMNS,
        metadataColumns.stream().filter(used::contains).collect(Collectors.toList()));
    IcebergTable.putFilter(readConfig, getSchema(), filter);
    PCollection<Row> rows = read(begin, readConfig);
    // A read of metadata columns alone keeps the primary key, which the query did not select.
    return rows.getSchema().getFieldNames().equals(used)
        ? rows
        : rows.apply("Select queried columns", Select.fieldNames(used.toArray(new String[0])));
  }

  private static PCollection<Row> read(PBegin begin, Map<String, Object> config) {
    return begin
        .apply("Read Iceberg changes", Managed.read(Managed.ICEBERG_CDC).withConfig(config))
        .getSinglePCollection();
  }

  @Override
  public ProjectSupport supportsProjects() {
    return ProjectSupport.WITHOUT_FIELD_REORDERING;
  }

  /** Filters on metadata columns, which are not columns of the Iceberg table, are not pushed. */
  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return new IcebergFilter(filter, ImmutableBitSet.range(table.getSchema().getFieldCount()));
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return streaming ? PCollection.IsBounded.UNBOUNDED : PCollection.IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return streaming ? BeamTableStatistics.UNBOUNDED_UNKNOWN : BeamTableStatistics.BOUNDED_UNKNOWN;
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException(
        String.format(
            "CHANGES of Iceberg table '%s' cannot be written. Write to a regular "
                + "Iceberg table with mode='merge-on-read'",
            table.tableIdentifier));
  }
}
