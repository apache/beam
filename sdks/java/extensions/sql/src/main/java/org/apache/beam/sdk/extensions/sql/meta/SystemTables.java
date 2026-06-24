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
package org.apache.beam.sdk.extensions.sql.meta;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides {@link BeamSqlTable}s that track metadata around catalogs, databases, and tables. For
 * now, it tracks the following:
 *
 * <ul>
 *   <li>Catalogs: Name and Type
 *   <li>Databases: Name
 *   <li>Tables: Name and Type
 * </ul>
 */
public class SystemTables {
  public static CatalogsMetaTable catalogs(CatalogManager catalogManager, boolean currentOnly) {
    return new CatalogsMetaTable(catalogManager, currentOnly);
  }

  public static DatabasesMetaTable databases(Catalog catalog, boolean currentOnly) {
    return new DatabasesMetaTable(catalog, currentOnly);
  }

  public static TablesMetaTable tables(Catalog catalog, String dbName) {
    return new TablesMetaTable(catalog, dbName);
  }

  public static class CatalogsMetaTable extends BaseBeamTable {
    private final CatalogManager catalogManager;
    private final boolean currentOnly;

    private static final Schema SCHEMA =
        Schema.builder().addStringField("NAME").addStringField("TYPE").build();

    public CatalogsMetaTable(CatalogManager catalogManager, boolean currentOnly) {
      this.catalogManager = catalogManager;
      this.currentOnly = currentOnly;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      Collection<Catalog> catalogs =
          currentOnly
              ? ImmutableList.of(catalogManager.currentCatalog())
              : catalogManager.catalogs();
      List<Row> rows =
          catalogs.stream()
              .map(cat -> Row.withSchema(SCHEMA).addValues(cat.name(), cat.type()).build())
              .collect(Collectors.toList());

      return begin.apply(Create.of(rows).withRowSchema(SCHEMA));
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException("Cannot write to SHOW CATALOGS");
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    @Override
    public Schema getSchema() {
      return SCHEMA;
    }
  }

  public static class DatabasesMetaTable extends BaseBeamTable {
    private final Catalog catalog;
    private final boolean currentOnly;
    private static final Schema SCHEMA = Schema.builder().addStringField("NAME").build();

    DatabasesMetaTable(Catalog catalog, boolean currentOnly) {
      this.catalog = catalog;
      this.currentOnly = currentOnly;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      Collection<String> databases;
      if (currentOnly) {
        @Nullable String currentDb = catalog.currentDatabase();
        databases = currentDb != null ? Collections.singleton(currentDb) : Collections.emptyList();
      } else {
        databases = catalog.databases();
      }
      List<Row> rows =
          databases.stream()
              .map(db -> Row.withSchema(SCHEMA).addValues(db).build())
              .collect(Collectors.toList());

      return begin.apply(Create.of(rows).withRowSchema(SCHEMA));
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException("Cannot write to SHOW DATABASES");
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    @Override
    public Schema getSchema() {
      return SCHEMA;
    }
  }

  public static class TablesMetaTable extends BaseBeamTable {
    private final Catalog catalog;
    private final String dbName;
    private static final Schema SCHEMA =
        Schema.builder().addStringField("NAME").addStringField("TYPE").build();

    public TablesMetaTable(Catalog catalog, String dbName) {
      this.catalog = catalog;
      this.dbName = dbName;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      // Note: This captures the state *at the moment of planning*
      List<Row> rows =
          catalog.metaStore(dbName).getTables().values().stream()
              .map(
                  table ->
                      Row.withSchema(SCHEMA).addValues(table.getName(), table.getType()).build())
              .collect(Collectors.toList());

      return begin.apply(Create.of(rows).withRowSchema(SCHEMA));
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException("Cannot write to SHOW TABLES");
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    @Override
    public Schema getSchema() {
      return SCHEMA;
    }
  }
}
