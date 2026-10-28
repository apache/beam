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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamSqlUnparseContext;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class IcebergTable extends SchemaBaseBeamTable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTable.class);
  @VisibleForTesting static final String CATALOG_PROPERTIES_FIELD = "catalog_properties";
  @VisibleForTesting static final String HADOOP_CONFIG_PROPERTIES_FIELD = "config_properties";
  @VisibleForTesting static final String CATALOG_NAME_FIELD = "catalog_name";
  static final String BEAM_WRITE_PROPERTY = "beam.write.";
  static final String BEAM_READ_PROPERTY = "beam.read.";

  @VisibleForTesting
  static final String TRIGGERING_FREQUENCY_FIELD = "triggering_frequency_seconds";

  private static final Schema WRITE_CONFIG_SCHEMA =
      new IcebergWriteSchemaTransformProvider().configurationSchema();
  private static final Map<String, String> WRITE_OWNED_OPTIONS =
      ImmutableMap.<String, String>builder()
          .putAll(
              ManagedOptions.owned(
                  "the SQL table sets it",
                  "table",
                  "catalog_name",
                  "catalog_properties",
                  "config_properties"))
          .putAll(
              ManagedOptions.owned(
                  "the INSERT writes all of the table's columns", "keep", "drop", "only"))
          .putAll(
              ManagedOptions.owned(
                  "it applies only when the write creates the table",
                  "partition_fields",
                  "sort_fields",
                  "table_properties"))
          .putAll(ManagedOptions.owned("an INSERT has no error output", "error_handling"))
          .build();

  @VisibleForTesting final String tableIdentifier;
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;
  @VisibleForTesting @Nullable Integer triggeringFrequency;
  @VisibleForTesting final @Nullable List<String> partitionFields;

  /** The options of an INSERT's {@code WITH} clause, or empty for the table itself. */
  private final Map<String, Object> writeOptions;

  IcebergTable(String tableIdentifier, Table table, IcebergCatalogConfig catalogConfig) {
    super(table.getSchema());
    this.schema = table.getSchema();
    this.tableIdentifier = tableIdentifier;
    this.catalogConfig = catalogConfig;
    ObjectNode properties = table.getProperties();
    for (Map.Entry<String, JsonNode> property : properties.properties()) {
      String name = property.getKey().toLowerCase();
      if (name.startsWith(BEAM_WRITE_PROPERTY)) {
        String prop = name.substring(BEAM_WRITE_PROPERTY.length());
        if (prop.equalsIgnoreCase(TRIGGERING_FREQUENCY_FIELD)) {
          this.triggeringFrequency = property.getValue().asInt();
        } else {
          throw new IllegalArgumentException("Unknown Beam write property: " + name);
        }
      } else if (name.startsWith(BEAM_READ_PROPERTY)) {
        // none supported yet
        throw new IllegalArgumentException("Unknown Beam read property: " + name);
      }
    }

    this.partitionFields = table.getPartitionFields();
    this.writeOptions = ImmutableMap.of();
  }

  private IcebergTable(IcebergTable table, Schema schema, Map<String, Object> writeOptions) {
    super(schema);
    this.tableIdentifier = table.tableIdentifier;
    this.catalogConfig = table.catalogConfig;
    this.triggeringFrequency = table.triggeringFrequency;
    this.partitionFields = table.partitionFields;
    this.writeOptions = writeOptions;
  }

  /**
   * Returns this table as the target of an INSERT with {@code WITH} options, which are options of
   * the Managed Iceberg write. With {@code 'mode' = 'merge-on-read'}, the INSERT also supplies the
   * change type column, if {@code change_type_column} names one, and the sequence number column.
   */
  @Override
  public BeamSqlTable forWrite(Map<String, Object> options) {
    Map<String, Object> checked =
        ManagedOptions.check(options, WRITE_CONFIG_SCHEMA, WRITE_OWNED_OPTIONS, "an Iceberg write");
    Schema schema = getSchema();
    if ("merge-on-read".equalsIgnoreCase((String) checked.get("mode"))) {
      @Nullable String changeTypeColumn = (String) checked.get("change_type_column");
      if (changeTypeColumn != null) {
        schema = withControlColumn(schema, changeTypeColumn, Schema.FieldType.STRING);
      }
      Object sequenceNumberColumn =
          checked.getOrDefault(
              "sequence_number_column", IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER);
      schema = withControlColumn(schema, (String) sequenceNumberColumn, Schema.FieldType.INT64);
    }
    return new IcebergTable(this, schema, checked);
  }

  private static Schema withControlColumn(Schema schema, String name, Schema.FieldType type) {
    checkArgument(
        !schema.hasField(name),
        "Merge-on-read control column '%s' collides with another column",
        name);
    return Schema.builder().addFields(schema.getFields()).addField(name, type).build();
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input.apply(Managed.write(Managed.ICEBERG).withConfig(writeConfig()));
  }

  /**
   * The Managed write configuration: the table's, its stored triggering frequency, the INSERT's
   * {@code WITH} options, which override it, and its partition fields.
   */
  @VisibleForTesting
  Map<String, Object> writeConfig() {
    ImmutableMap.Builder<String, Object> configBuilder = ImmutableMap.builder();
    configBuilder.putAll(getBaseConfig());
    if (triggeringFrequency != null) {
      configBuilder.put(TRIGGERING_FREQUENCY_FIELD, triggeringFrequency);
    }
    configBuilder.putAll(writeOptions);
    if (partitionFields != null) {
      configBuilder.put("partition_fields", partitionFields);
    }
    return configBuilder.buildKeepingLast();
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(Managed.read(Managed.ICEBERG).withConfig(getBaseConfig()))
        .getSinglePCollection();
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filter, List<String> fieldNames) {

    Map<String, Object> readConfig = new HashMap<>(getBaseConfig());

    if (!(filter instanceof DefaultTableFilter)) {
      IcebergFilter icebergFilter = (IcebergFilter) filter;
      if (!icebergFilter.getSupported().isEmpty()) {
        String expression = generateFilterExpression(getSchema(), icebergFilter.getSupported());
        if (!expression.isEmpty()) {
          LOG.info("Pushing down the following filter: {}", expression);
          readConfig.put("filter", expression);
        }
      }
    }

    if (!fieldNames.isEmpty()) {
      readConfig.put("keep", fieldNames);
    }

    return begin
        .apply("Read Iceberg with push-down", Managed.read(Managed.ICEBERG).withConfig(readConfig))
        .getSinglePCollection();
  }

  @Override
  public ProjectSupport supportsProjects() {
    return ProjectSupport.WITHOUT_FIELD_REORDERING;
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return new IcebergFilter(filter);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  private Map<String, Object> getBaseConfig() {
    ImmutableMap.Builder<String, Object> managedConfigBuilder = ImmutableMap.builder();
    managedConfigBuilder.put("table", tableIdentifier);
    @Nullable String name = catalogConfig.getCatalogName();
    @Nullable Map<String, String> catalogProps = catalogConfig.getCatalogProperties();
    @Nullable Map<String, String> hadoopConfProps = catalogConfig.getConfigProperties();
    if (name != null) {
      managedConfigBuilder.put(CATALOG_NAME_FIELD, name);
    }
    if (catalogProps != null) {
      managedConfigBuilder.put(CATALOG_PROPERTIES_FIELD, catalogProps);
    }
    if (hadoopConfProps != null) {
      managedConfigBuilder.put(HADOOP_CONFIG_PROPERTIES_FIELD, hadoopConfProps);
    }
    return managedConfigBuilder.build();
  }

  private String generateFilterExpression(Schema schema, List<RexNode> supported) {
    final IntFunction<SqlNode> field =
        i -> new SqlIdentifier(schema.getField(i).getName(), SqlParserPos.ZERO);

    SqlImplementor.Context context = new BeamSqlUnparseContext(field);

    // Create a single SqlNode from a list of RexNodes
    SqlNode andSqlNode = null;
    for (RexNode node : supported) {
      SqlNode sqlNode = context.toSql(null, node);
      if (andSqlNode == null) {
        andSqlNode = sqlNode;
        continue;
      }
      // AND operator must have exactly 2 operands.
      andSqlNode =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO, ImmutableList.of(andSqlNode, sqlNode));
    }
    return checkStateNotNull(andSqlNode).toSqlString(BigQuerySqlDialect.DEFAULT).getSql();
  }
}
