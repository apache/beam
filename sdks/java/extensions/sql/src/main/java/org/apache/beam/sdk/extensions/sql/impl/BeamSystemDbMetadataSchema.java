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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.SystemTables;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaVersion;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schemas;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A Calcite {@link Schema} responsible for {@code SHOW DATABASES} requests. */
public class BeamSystemDbMetadataSchema implements Schema {
  private final CatalogManager catalogManager;

  BeamSystemDbMetadataSchema(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  @Override
  public @Nullable Table getTable(String catalogName) {
    Catalog catalog;
    if (catalogName.equals("__current_catalog__")) {
      catalog = catalogManager.currentCatalog();
    } else {
      catalog =
          checkArgumentNotNull(
              catalogManager.getCatalog(catalogName), "Catalog '%s' does not exist.", catalogName);
    }

    return BeamCalciteTable.of(SystemTables.databases(catalog, false));
  }

  @Override
  public Set<String> getTableNames() {
    return catalogManager.catalogs().stream().map(Catalog::name).collect(Collectors.toSet());
  }

  @Override
  public @Nullable Schema getSubSchema(@Nullable String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
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
