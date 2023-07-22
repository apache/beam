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
package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.hcatalog.HCatalogBeamSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table provider that represents a schema stored in Hive Metastore.
 *
 * <p>Supports only 'db.table' format at the moment, does not support 'catalog.db.table'.
 *
 * <p>Delegates to sub-providers to get the actual table.
 *
 * <p>If only the table name is provided, then 'db' is assumed to be 'default'.
 */
class HCatalogTableProvider implements TableProvider, Serializable {

  private HashMap<String, String> configuration;
  private transient DatabaseProvider defaultDBProvider;
  private transient HCatalogBeamSchema metastoreSchema;

  private HCatalogTableProvider(
      HashMap<String, String> configuration,
      HCatalogBeamSchema metastoreSchema,
      DatabaseProvider defaultDBProvider) {
    this.configuration = configuration;
    this.defaultDBProvider = defaultDBProvider;
    this.metastoreSchema = metastoreSchema;
  }

  public static HCatalogTableProvider create(Map<String, String> configuration) {
    HCatalogBeamSchema metastoreSchema = HCatalogBeamSchema.create(configuration);
    return new HCatalogTableProvider(
        new HashMap<>(configuration),
        metastoreSchema,
        new DatabaseProvider("default", metastoreSchema, configuration));
  }

  @Override
  public String getTableType() {
    return "hcatalog";
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException("Creating tables in HCatalog is not supported");
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException("Deleting tables in HCatalog is not supported");
  }

  @Override
  public Map<String, Table> getTables() {
    throw new UnsupportedOperationException("Extracting all tables from HCatalog is not supported");
  }

  @Nullable
  @Override
  public Table getTable(String name) {
    // Tables should have been looked up from sub-providers.
    // If we reached this then getSubProvider(name) returned null
    // meaning there's no such DB. Try to look up the table in the default DB instead.
    return defaultDBProvider.getTable(name);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    // This is the same `default` DB use case similar to how `getTable()` behaves.
    // This path should only be hit if none of the sub-providers for the DBs
    // was able to find the table.
    return defaultDBProvider.buildBeamSqlTable(table);
  }

  @Override
  public Set<String> getSubProviders() {
    throw new UnsupportedOperationException("Listing DBs is not supported in metastore");
  }

  @Override
  public TableProvider getSubProvider(String name) {
    return metastoreSchema.hasDatabase(name)
        ? new DatabaseProvider(name, metastoreSchema, configuration)
        : null;
  }
}
