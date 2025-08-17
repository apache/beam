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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaVersion;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schemas;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Adapter from {@link TableProvider} to {@link Schema}. */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
public class BeamCalciteSchema implements Schema {
  private JdbcConnection connection;
  private @Nullable TableProvider tableProvider;
  private @Nullable CatalogManager catalogManager;
  private Map<String, BeamCalciteSchema> subSchemas;

  BeamCalciteSchema(JdbcConnection jdbcConnection, TableProvider tableProvider) {
    this.connection = jdbcConnection;
    this.tableProvider = tableProvider;
    this.subSchemas = new HashMap<>();
  }

  /**
   * Creates a {@link BeamCalciteSchema} representing a {@link CatalogManager}. This will typically
   * be the root node of a pipeline.
   */
  BeamCalciteSchema(JdbcConnection jdbcConnection, CatalogManager catalogManager) {
    this.connection = jdbcConnection;
    this.catalogManager = catalogManager;
    this.subSchemas = new HashMap<>();
  }

  public TableProvider getTableProvider() {
    return resolveMetastore();
  }

  public @Nullable CatalogManager getCatalogManager() {
    return catalogManager;
  }

  public Map<String, String> getPipelineOptions() {
    return connection.getPipelineOptionsMap();
  }

  public void setPipelineOption(String key, String value) {
    Map<String, String> options = new HashMap<>(connection.getPipelineOptionsMap());
    options.put(key, value);
    connection.setPipelineOptionsMap(options);
  }

  public void removePipelineOption(String key) {
    Map<String, String> options = new HashMap<>(connection.getPipelineOptionsMap());
    options.remove(key);
    connection.setPipelineOptionsMap(options);
  }

  public void removeAllPipelineOptions() {
    connection.setPipelineOptionsMap(Collections.emptyMap());
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  @Override
  public Set<String> getTableNames() {
    return resolveMetastore().getTables().keySet();
  }

  @Override
  public @Nullable RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table getTable(
      String name) {
    Table table = resolveMetastore().getTable(name);
    if (table == null) {
      return null;
    }
    return new BeamCalciteTable(
        resolveMetastore().buildBeamSqlTable(table),
        getPipelineOptions(),
        connection.getPipelineOptions());
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return resolveMetastore().getSubProviders();
  }

  /**
   * If this is the root schema (in other words, a {@link CatalogManager}), the sub schema will be a
   * {@link Catalog}'s metastore.
   *
   * <p>Otherwise, the sub-schema is derived from the {@link TableProvider} implementation.
   */
  @Override
  public Schema getSubSchema(String name) {
    if (!subSchemas.containsKey(name)) {
      BeamCalciteSchema subSchema;
      if (tableProvider != null) {
        @Nullable TableProvider subProvider = tableProvider.getSubProvider(name);
        subSchema = subProvider != null ? new BeamCalciteSchema(connection, subProvider) : null;
      } else {
        @Nullable Catalog catalog = checkStateNotNull(catalogManager).getCatalog(name);
        subSchema = catalog != null ? new BeamCalciteSchema(connection, catalog.metaStore()) : null;
      }
      subSchemas.put(name, subSchema);
    }

    return subSchemas.get(name);
  }

  public TableProvider resolveMetastore() {
    if (tableProvider != null) {
      return tableProvider;
    }
    return checkStateNotNull(catalogManager).currentCatalog().metaStore();
  }
}
