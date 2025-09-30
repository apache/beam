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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamSqlUnparseContext;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
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

class IcebergTable extends SchemaBaseBeamTable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTable.class);
  @VisibleForTesting static final String CATALOG_PROPERTIES_FIELD = "catalog_properties";
  @VisibleForTesting static final String HADOOP_CONFIG_PROPERTIES_FIELD = "config_properties";
  @VisibleForTesting static final String CATALOG_NAME_FIELD = "catalog_name";

  @VisibleForTesting
  static final String TRIGGERING_FREQUENCY_FIELD = "triggering_frequency_seconds";

  @VisibleForTesting final String tableIdentifier;
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;
  @VisibleForTesting @Nullable Integer triggeringFrequency;
  @VisibleForTesting final @Nullable List<String> partitionFields;

  IcebergTable(Table table, IcebergCatalogConfig catalogConfig) {
    super(table.getSchema());
    this.schema = table.getSchema();
    this.tableIdentifier = checkArgumentNotNull(table.getLocation());
    this.catalogConfig = catalogConfig;
    ObjectNode properties = table.getProperties();
    if (properties.has(TRIGGERING_FREQUENCY_FIELD)) {
      this.triggeringFrequency = properties.get(TRIGGERING_FREQUENCY_FIELD).asInt();
    }
    this.partitionFields = table.getPartitionFields();
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    ImmutableMap.Builder<String, Object> configBuilder = ImmutableMap.builder();
    configBuilder.putAll(getBaseConfig());
    if (triggeringFrequency != null) {
      configBuilder.put(TRIGGERING_FREQUENCY_FIELD, triggeringFrequency);
    }
    if (partitionFields != null) {
      configBuilder.put("partition_fields", partitionFields);
    }
    return input.apply(Managed.write(Managed.ICEBERG).withConfig(configBuilder.build()));
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
