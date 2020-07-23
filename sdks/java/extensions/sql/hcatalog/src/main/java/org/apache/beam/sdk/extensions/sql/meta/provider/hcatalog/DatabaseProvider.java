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

import com.alibaba.fastjson.JSONObject;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.hcatalog.HCatalogBeamSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Metastore has a structure of 'db.table'.
 *
 * <p>This provider represents the 'db' and contains the table look up logic.
 */
class DatabaseProvider implements TableProvider {

  private String db;
  private HCatalogBeamSchema metastoreSchema;
  private Map<String, String> config;

  DatabaseProvider(String db, HCatalogBeamSchema metastoreSchema, Map<String, String> config) {
    this.db = db;
    this.metastoreSchema = metastoreSchema;
    this.config = config;
  }

  String getDb() {
    return db;
  }

  @Override
  public String getTableType() {
    return "hcatalog";
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException("Creating tables is not supported in HCatalog");
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException("Deleting tables is not supported in HCatalog");
  }

  @Override
  public Map<String, Table> getTables() {
    throw new UnsupportedOperationException("Listing tables is not supported in HCatalog");
  }

  /** Table metadata to pass the schema to Calcite. */
  @Nullable
  @Override
  public Table getTable(String table) {
    Optional<Schema> tableSchema = metastoreSchema.getTableSchema(db, table);
    if (!tableSchema.isPresent()) {
      return null;
    }

    return Table.builder()
        .schema(tableSchema.get())
        .name(table)
        .location("")
        .properties(new JSONObject())
        .comment("")
        .type("hcatalog")
        .build();
  }

  /** Actual wrapper for the IO, reads/writes the data. */
  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return HCatalogTable.builder()
        .setConfig(config)
        .setDatabase(db)
        .setTable(table.getName())
        .setSchema(table.getSchema())
        .build();
  }
}
