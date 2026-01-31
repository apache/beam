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
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.SystemTables;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaVersion;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schemas;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Calcite {@link Schema} specialized for displaying the session's metadata. Top node that manages
 * requests to {@code SHOW} {@code CATALOGS}, {@code DATABASES}, and {@code TABLES}. Used by {@link
 * CatalogManagerSchema}.
 *
 * <p>{@code SHOW} requests are treated as aliases, listed below:
 *
 * <ul>
 *   <li>{@code SHOW CURRENT CATALOG} --> {@code SELECT * FROM `beamsystem`.`__current_catalog__`}
 *   <li>{@code SHOW CATALOGS} --> {@code SELECT * FROM `beamsystem`.`catalogs`}
 *   <li>{@code SHOW CATALOGS LIKE '{pattern}'} --> {@code SELECT * FROM `beamsystem`.`catalogs`
 *       WHERE NAME LIKE '{pattern}'}
 *   <li>{@code SHOW CURRENT DATABASE} --> {@code SELECT * FROM `beamsystem`.`__current_database__`}
 *   <li>{@code SHOW DATABASES} --> {@code SELECT * FROM
 *       `beamsystem`.`databases`.`__current_catalog__`}
 *   <li>{@code SHOW DATABASES FROM my_catalog} --> {@code SELECT * FROM
 *       `beamsystem`.`databases`.`my_catalog`}
 *   <li>{@code SHOW DATABASES FROM my_catalog LIKE '{pattern}'} --> {@code SELECT * FROM
 *       `beamsystem`.`databases`.`my_catalog` WHERE NAME LIKE '{pattern}'}
 *   <li>{@code SHOW TABLES} --> {@code SELECT * FROM
 *       `beamsystem`.`tables`.`__current_catalog__`.`__current_database__`}
 *   <li>{@code SHOW TABLES FROM my_db} --> {@code SELECT * FROM
 *       `beamsystem`.`tables`.`__current_catalog__`.`my_db`}
 *   <li>{@code SHOW TABLES FROM my_catalog.my_db} --> {@code SELECT * FROM
 *       `beamsystem`.`tables`.`my_catalog`.`my_db`}
 *   <li>{@code SHOW TABLES FROM my_catalog.my_db LIKE '{pattern}'} --> {@code SELECT * FROM
 *       `beamsystem`.`tables`.`my_catalog`.`my_db` WHERE NAME LIKE '{pattern}'}
 * </ul>
 */
public class BeamSystemSchema implements Schema {
  private final CatalogManager catalogManager;
  private final BeamSystemDbMetadataSchema dbSchema;
  private final BeamSystemTableMetadataSchema tableSchema;
  public static final String BEAMSYSTEM = "beamsystem";
  private static final String CATALOGS = "catalogs";
  private static final String DATABASES = "databases";
  private static final String TABLES = "tables";

  BeamSystemSchema(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
    this.dbSchema = new BeamSystemDbMetadataSchema(catalogManager);
    this.tableSchema = new BeamSystemTableMetadataSchema(catalogManager, null);
  }

  @Override
  public @Nullable Table getTable(String table) {
    switch (table) {
      case CATALOGS:
        return BeamCalciteTable.of(SystemTables.catalogs(catalogManager, false));
      case "__current_catalog__":
        return BeamCalciteTable.of(SystemTables.catalogs(catalogManager, true));
      case "__current_database__":
        return BeamCalciteTable.of(SystemTables.databases(catalogManager.currentCatalog(), true));
      default:
        return null;
    }
  }

  @Override
  public Set<String> getTableNames() {
    return ImmutableSet.of(CATALOGS);
  }

  @Override
  public @Nullable Schema getSubSchema(@Nullable String name) {
    if (name == null) {
      return null;
    }
    switch (name) {
      case DATABASES:
        return dbSchema;
      case TABLES:
        return tableSchema;
      default:
        return null;
    }
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return ImmutableSet.of(DATABASES, TABLES);
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public @Nullable RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(@Nullable SchemaPlus schemaPlus, String s) {
    return Schemas.subSchemaExpression(checkStateNotNull(schemaPlus), s, getClass());
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return this;
  }
}
