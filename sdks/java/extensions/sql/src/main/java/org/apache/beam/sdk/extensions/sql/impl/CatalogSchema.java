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

import static java.lang.String.format;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlDdlNodes;
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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Calcite {@link Schema} that corresponds to a {@link Catalog}. Child schemas are of type {@link
 * BeamCalciteSchema}.
 */
public class CatalogSchema implements Schema {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogSchema.class);
  private final JdbcConnection connection;
  private final Catalog catalog;
  private final Map<String, BeamCalciteSchema> subSchemas = new HashMap<>();
  /**
   * Creates a Calcite {@link Schema} representing a {@link CatalogManager}. This will typically be
   * the root node of a pipeline.
   */
  CatalogSchema(JdbcConnection jdbcConnection, Catalog catalog) {
    this.connection = jdbcConnection;
    this.catalog = catalog;
    // try to eagerly populate Calcite sub-schemas with existing databases
    try {
      catalog
          .listDatabases()
          .forEach(
              database ->
                  subSchemas.put(
                      database,
                      new BeamCalciteSchema(database, connection, catalog.metaStore(database))));
    } catch (Exception ignored) {
    }
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public @Nullable BeamCalciteSchema getCurrentDatabaseSchema() {
    return getSubSchema(catalog.currentDatabase());
  }

  public BeamCalciteSchema getDatabaseSchema(TableName tablePath) {
    @Nullable BeamCalciteSchema beamCalciteSchema = getSubSchema(tablePath.database());
    if (beamCalciteSchema == null) {
      beamCalciteSchema = getCurrentDatabaseSchema();
    }
    return checkStateNotNull(
        beamCalciteSchema, "Could not find BeamCalciteSchema for table: '%s'", tablePath);
  }

  public void createDatabase(SqlIdentifier databaseIdentifier, boolean ifNotExists) {
    String name = SqlDdlNodes.name(databaseIdentifier);
    boolean alreadyExists = subSchemas.containsKey(name);

    if (!alreadyExists) {
      try {
        LOG.info("Creating database '{}'", name);
        if (catalog.createDatabase(name)) {
          LOG.info("Successfully created database '{}'", name);
        } else {
          alreadyExists = true;
        }
      } catch (Exception e) {
        throw SqlUtil.newContextException(
            databaseIdentifier.getParserPosition(),
            RESOURCE.internal(
                format("Encountered an error when creating database '%s': %s", name, e)));
      }
    }

    if (alreadyExists) {
      String message = format("Database '%s' already exists.", name);
      if (ifNotExists) {
        LOG.info(message);
      } else {
        throw SqlUtil.newContextException(
            databaseIdentifier.getParserPosition(), RESOURCE.internal(message));
      }
    }

    subSchemas.put(name, new BeamCalciteSchema(name, connection, catalog.metaStore(name)));
  }

  public void useDatabase(SqlIdentifier identifier) {
    String name = SqlDdlNodes.name(identifier);
    if (!subSchemas.containsKey(name)) {
      if (!catalog.listDatabases().contains(name)) {
        throw SqlUtil.newContextException(
            identifier.getParserPosition(),
            RESOURCE.internal(String.format("Cannot use database: '%s' not found.", name)));
      }
      subSchemas.put(name, new BeamCalciteSchema(name, connection, catalog.metaStore(name)));
    }

    if (name.equals(catalog.currentDatabase())) {
      LOG.info("Database '{}' is already in use.", name);
      return;
    }

    catalog.useDatabase(name);
    LOG.info("Switched to database '{}'.", name);
  }

  public void dropDatabase(SqlIdentifier identifier, boolean cascade, boolean ifExists) {
    String name = SqlDdlNodes.name(identifier);
    try {
      LOG.info("Dropping database '{}'", name);
      boolean dropped = catalog.dropDatabase(name, cascade);

      if (dropped) {
        LOG.info("Successfully dropped database '{}'", name);
      } else if (ifExists) {
        LOG.info("Database '{}' does not exist.", name);
      } else {
        throw SqlUtil.newContextException(
            identifier.getParserPosition(),
            RESOURCE.internal(String.format("Database '%s' does not exist.", name)));
      }
    } catch (Exception e) {
      throw SqlUtil.newContextException(
          identifier.getParserPosition(),
          RESOURCE.internal(
              format("Encountered an error when dropping database '%s': %s", name, e)));
    }

    subSchemas.remove(name);
  }

  @Override
  public @Nullable Table getTable(String s) {
    @Nullable BeamCalciteSchema beamCalciteSchema = currentDatabase();
    return beamCalciteSchema != null ? beamCalciteSchema.getTable(s) : null;
  }

  @Override
  public Set<String> getTableNames() {
    @Nullable BeamCalciteSchema beamCalciteSchema = currentDatabase();
    return beamCalciteSchema != null ? beamCalciteSchema.getTableNames() : Collections.emptySet();
  }

  @Override
  public @Nullable BeamCalciteSchema getSubSchema(@Nullable String name) {
    if (name == null) {
      return null;
    }
    @Nullable BeamCalciteSchema beamCalciteSchema = subSchemas.get(name);
    if (beamCalciteSchema == null) {
      Set<String> databases;
      try {
        databases = catalog.listDatabases();
      } catch (Exception ignored) {
        return null;
      }
      if (databases.contains(name)) {
        beamCalciteSchema = new BeamCalciteSchema(name, connection, catalog.metaStore(name));
        subSchemas.put(name, beamCalciteSchema);
      }
    }
    return beamCalciteSchema;
  }

  private @Nullable BeamCalciteSchema currentDatabase() {
    @Nullable String currentDatabase = catalog.currentDatabase();
    if (currentDatabase != null) {
      return subSchemas.get(currentDatabase);
    }
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return catalog.listDatabases();
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
