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
import static org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Static.RESOURCE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlDdlNodes;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaVersion;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Schemas;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Calcite {@link Schema} that corresponds to a {@link CatalogManager}. This is typically the root
 * node of a pipeline. Child schemas are of type {@link CatalogSchema}.
 */
public class CatalogManagerSchema implements Schema {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogManagerSchema.class);
  private final JdbcConnection connection;
  private final CatalogManager catalogManager;
  private final Map<String, CatalogSchema> catalogSubSchemas = new HashMap<>();

  CatalogManagerSchema(JdbcConnection jdbcConnection, CatalogManager catalogManager) {
    this.connection = jdbcConnection;
    this.catalogManager = catalogManager;
  }

  @VisibleForTesting
  public JdbcConnection connection() {
    return connection;
  }

  public void createCatalog(
      SqlIdentifier catalogIdentifier,
      String type,
      Map<String, String> properties,
      boolean replace,
      boolean ifNotExists) {
    String name = SqlDdlNodes.name(catalogIdentifier);
    if (catalogManager.getCatalog(name) != null) {
      if (replace) {
        LOG.info("Replacing existing catalog '{}'", name);
        catalogManager.dropCatalog(name);
      } else if (!ifNotExists) {
        throw SqlUtil.newContextException(
            catalogIdentifier.getParserPosition(),
            RESOURCE.internal(String.format("Catalog '%s' already exists.", name)));
      } else {
        LOG.info("Catalog '{}' already exists", name);
        return;
      }
    }

    // create the catalog
    catalogManager.createCatalog(name, type, properties);
    CatalogSchema catalogSchema =
        new CatalogSchema(connection, checkStateNotNull(catalogManager.getCatalog(name)));
    catalogSubSchemas.put(name, catalogSchema);
  }

  public void useCatalog(SqlIdentifier catalogIdentifier) {
    String name = catalogIdentifier.toString();
    if (catalogManager.getCatalog(catalogIdentifier.toString()) == null) {
      throw SqlUtil.newContextException(
          catalogIdentifier.getParserPosition(),
          RESOURCE.internal(String.format("Cannot use catalog: '%s' not found.", name)));
    }

    if (catalogManager.currentCatalog().name().equals(name)) {
      LOG.info("Catalog '{}' is already in use.", name);
      return;
    }

    catalogManager.useCatalog(name);
    LOG.info("Switched to catalog '{}' (type: {})", name, catalogManager.currentCatalog().type());
  }

  public void dropCatalog(SqlIdentifier identifier, boolean ifExists) {
    String name = SqlDdlNodes.name(identifier);
    if (catalogManager.getCatalog(name) == null) {
      if (!ifExists) {
        throw SqlUtil.newContextException(
            identifier.getParserPosition(),
            RESOURCE.internal(String.format("Cannot drop catalog: '%s' not found.", name)));
      }
      LOG.info("Ignoring 'DROP CATALOG` call for non-existent catalog: {}", name);
      return;
    }

    if (catalogManager.currentCatalog().name().equals(name)) {
      throw SqlUtil.newContextException(
          identifier.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Unable to drop active catalog '%s'. Please switch to another catalog first.",
                  name)));
    }

    catalogManager.dropCatalog(name);
    LOG.info("Successfully dropped catalog '{}'", name);
    catalogSubSchemas.remove(name);
  }

  @Override
  public @Nullable Table getTable(String table) {
    @Nullable
    CatalogSchema catalogSchema = catalogSubSchemas.get(catalogManager.currentCatalog().name());
    return catalogSchema != null ? catalogSchema.getTable(table) : null;
  }

  @Override
  public Set<String> getTableNames() {
    ImmutableSet.Builder<String> names = ImmutableSet.builder();
    // TODO: this might be a heavy operation
    for (Catalog catalog : catalogManager.catalogs()) {
      for (String db : catalog.listDatabases()) {
        names.addAll(catalog.metaStore(db).getTables().keySet());
      }
    }
    return names.build();
  }

  public CatalogSchema getCatalogSchema(TableName tablePath) {
    @Nullable Schema catalogSchema = getSubSchema(tablePath.catalog());
    if (catalogSchema == null) {
      catalogSchema = getCurrentCatalogSchema();
    }
    Preconditions.checkState(
        catalogSchema instanceof CatalogSchema,
        "Unexpected Schema type for Catalog '%s': %s",
        tablePath.catalog(),
        catalogSchema.getClass());
    return (CatalogSchema) catalogSchema;
  }

  public CatalogSchema getCurrentCatalogSchema() {
    return (CatalogSchema)
        checkStateNotNull(
            getSubSchema(catalogManager.currentCatalog().name()),
            "Could not find Calcite Schema for active catalog '%s'.",
            catalogManager.currentCatalog().name());
  }

  @Override
  public @Nullable Schema getSubSchema(@Nullable String name) {
    if (name == null) {
      return null;
    }
    @Nullable CatalogSchema catalogSchema = catalogSubSchemas.get(name);
    if (catalogSchema == null) {
      @Nullable Catalog catalog = catalogManager.getCatalog(name);
      if (catalog != null) {
        catalogSchema = new CatalogSchema(connection, catalog);
        catalogSubSchemas.put(name, catalogSchema);
      }
    }
    if (catalogSchema != null) {
      return catalogSchema;
    }

    // ** Backwards compatibility **
    // Name could be referring to a BeamCalciteSchema.
    // Attempt to fetch from current catalog
    return getCurrentCatalogSchema().getSubSchema(name);
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return catalogManager.catalogs().stream().map(Catalog::name).collect(Collectors.toSet());
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
