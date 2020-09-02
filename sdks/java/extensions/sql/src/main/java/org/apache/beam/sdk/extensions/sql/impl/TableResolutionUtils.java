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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.TableNameExtractionUtils;
import org.apache.beam.sdk.extensions.sql.meta.CustomTableResolver;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils to wire up the custom table resolution into Calcite's planner. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class TableResolutionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableResolutionUtils.class);

  /**
   * Extract table names from the FROM clauses, register them with root TableProviders that support
   * custom table schema resolution, e.g. DataCatalog.
   *
   * <p>Go over top-level schemas in the JdbcConnection, and for all top-level table providers that
   * support custom table resolution, register all the parsed table names with them.
   *
   * <p>This way when a table provider has custom name-resolution strategy it can analyze whether it
   * supports the name without using Calcite's logic. E.g. for DataCatalog we need to assemble the
   * table name back into a single string and then query the back-end, whereas Calcite would require
   * us to call the back-end for each part of the table name.
   *
   * <p>The logic is:
   *
   * <pre>
   *   - if it's a compound identifier (table name contains multiple parts):
   *       - get the first part of the identifier, assume it represents a top-level schema;
   *       - find a top-level table provider with the same name;
   *       - register the table identifier with it, if supported;
   *       - if not supported, then ignore the table identifier, everything will be resolved using
   *         existing Calcite's logic;
   *
   *   - if it's a simple identifier (contains only a table name without a schema part),
   *     or if there was no matching top-level schema:
   *       - register with the default schema, if it supports custom table resolution;
   *       - if it does not, existing Calcite logic will still work as is;
   * </pre>
   */
  static void setupCustomTableResolution(JdbcConnection connection, SqlNode parsed) {
    List<TableName> tableNames = TableNameExtractionUtils.extractTableNamesFromNode(parsed);
    String currentSchemaName = getCurrentSchemaName(connection);

    SchemaWithName defaultSchema = SchemaWithName.create(connection, currentSchemaName);

    if (defaultSchema.supportsCustomResolution()) {
      registerWithDefaultSchema(connection, tableNames, defaultSchema);
    }

    registerWithTopLevelSchemas(connection, tableNames);
  }

  /** Current (default) schema name in the JdbcConnection. */
  private static String getCurrentSchemaName(JdbcConnection connection) {
    try {
      return connection.getSchema();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Unable to get current schema name from JdbcConnection. "
              + "Assuming table names in the query are fully-qualified from the root.",
          e);
    }
  }

  /**
   * Simple identifiers have to be resolved by the default schema, as well as compoung identifiers
   * that don't have a matching top-level schema (meaning that a user didn't specify a top-level
   * schema and expected it to be inferred).
   */
  private static void registerWithDefaultSchema(
      JdbcConnection connection, List<TableName> tableNames, SchemaWithName defaultSchema) {
    Set<String> topLevelSchemas = connection.getRootSchema().getSubSchemaNames();

    List<TableName> simpleIdentifiers =
        tableNames.stream().filter(TableName::isSimple).collect(toList());

    List<TableName> withoutMatchingSchemas =
        tableNames.stream()
            .filter(name -> name.isCompound() && !topLevelSchemas.contains(name.getPrefix()))
            .collect(toList());

    List<TableName> explicitlyInDefaulSchema =
        tableNames.stream()
            .filter(name -> name.isCompound() && name.getPrefix().equals(defaultSchema.name))
            .map(TableName::removePrefix)
            .collect(toList());

    List<TableName> shouldGoIntoDefaultSchema =
        ImmutableList.<TableName>builder()
            .addAll(simpleIdentifiers)
            .addAll(withoutMatchingSchemas)
            .addAll(explicitlyInDefaulSchema)
            .build();

    defaultSchema.getCustomTableResolver().registerKnownTableNames(shouldGoIntoDefaultSchema);
  }

  /**
   * Register compound table identifiers with the matching custom resolvers that correspond to the
   * top-level schemas.
   */
  private static void registerWithTopLevelSchemas(
      JdbcConnection connection, List<TableName> tableNames) {

    Map<String, CustomTableResolver> topLevelResolvers = getCustomTopLevelResolvers(connection);

    topLevelResolvers.forEach(
        (topLevelSchemaName, resolver) ->
            resolver.registerKnownTableNames(tablesForSchema(tableNames, topLevelSchemaName)));
  }

  /** Get the custom schema resolvers for all top-level schemas that support custom resolution. */
  private static Map<String, CustomTableResolver> getCustomTopLevelResolvers(
      JdbcConnection connection) {
    return connection.getRootSchema().getSubSchemaNames().stream()
        .map(topLevelSchemaName -> SchemaWithName.create(connection, topLevelSchemaName))
        .filter(schema -> !schema.getName().equals(getCurrentSchemaName(connection)))
        .filter(SchemaWithName::supportsCustomResolution)
        .collect(toMap(SchemaWithName::getName, SchemaWithName::getCustomTableResolver));
  }

  /**
   * Get the compound identifiers that have the first component matching the given top-level schema
   * name and remove the first component.
   */
  private static List<TableName> tablesForSchema(
      List<TableName> tableNames, String topLevelSchema) {
    return tableNames.stream()
        .filter(TableName::isCompound)
        .filter(t -> t.getPrefix().equals(topLevelSchema))
        .map(TableName::removePrefix)
        .collect(toList());
  }

  /**
   * A utility class that keeps track of schema name and other properties.
   *
   * <p>Sole purpose is to reduce inline boilerplate and encapsulate stuff.
   */
  private static class SchemaWithName {
    String name;
    org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Schema schema;

    static SchemaWithName create(JdbcConnection connection, String name) {
      SchemaWithName schemaWithName = new SchemaWithName();
      schemaWithName.name = name;
      schemaWithName.schema =
          CalciteSchema.from(connection.getRootSchema().getSubSchema(name)).schema;
      return schemaWithName;
    }

    /** Whether this schema/table provider supports custom table resolution. */
    boolean supportsCustomResolution() {
      return isBeamSchema() && tableProviderSupportsCustomResolution();
    }

    /** Whether this Calcite schema is actually an instance of BeamCalciteSchema. */
    boolean isBeamSchema() {
      return schema instanceof BeamCalciteSchema;
    }

    /** Whether the table provider is an instance of CustomTableResolver. */
    boolean tableProviderSupportsCustomResolution() {
      return getTableProvider() instanceof CustomTableResolver;
    }

    /** Gets the table provider that backs the BeamCalciteSchema. */
    TableProvider getTableProvider() {
      checkState(isBeamSchema());
      return ((BeamCalciteSchema) schema).getTableProvider();
    }

    /** Schema name. */
    String getName() {
      return name;
    }

    /** Custom table resolver in the provider. */
    CustomTableResolver getCustomTableResolver() {
      checkState(supportsCustomResolution());
      return (CustomTableResolver) getTableProvider();
    }
  }
}
