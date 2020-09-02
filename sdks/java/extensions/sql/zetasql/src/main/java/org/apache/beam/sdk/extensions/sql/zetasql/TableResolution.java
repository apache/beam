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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.SimpleTable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.CustomTableResolver;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Utility methods to resolve a table, given a top-level Calcite schema and a table path. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TableResolution {

  /**
   * Resolves {@code tablePath} according to the given {@code schemaPlus}.
   *
   * <p>{@code tablePath} represents a structured table name where the last component is the name of
   * the table and all the preceding components are sub-schemas / namespaces within {@code
   * schemaPlus}.
   */
  public static Table resolveCalciteTable(SchemaPlus schemaPlus, List<String> tablePath) {
    Schema subSchema = schemaPlus;

    // subSchema.getSubschema() for all except last
    for (int i = 0; i < tablePath.size() - 1; i++) {
      subSchema = subSchema.getSubSchema(tablePath.get(i));
      if (subSchema == null) {
        throw new IllegalStateException(
            String.format(
                "While resolving table path %s, no sub-schema found for component %s (\"%s\")",
                tablePath, i, tablePath.get(i)));
      }
    }

    // for the final one call getTable()
    return subSchema.getTable(Iterables.getLast(tablePath));
  }

  /**
   * Registers tables that will be resolved during query analysis, so table providers can eagerly
   * pre-load metadata.
   */
  // TODO(https://issues.apache.org/jira/browse/BEAM-8817): share this logic between dialects
  public static void registerTables(SchemaPlus schemaPlus, List<List<String>> tables) {
    Schema defaultSchema = CalciteSchema.from(schemaPlus).schema;
    if (defaultSchema instanceof BeamCalciteSchema
        && ((BeamCalciteSchema) defaultSchema).getTableProvider() instanceof CustomTableResolver) {
      ((CustomTableResolver) ((BeamCalciteSchema) defaultSchema).getTableProvider())
          .registerKnownTableNames(
              tables.stream().map(TableName::create).collect(Collectors.toList()));
    }

    for (String subSchemaName : schemaPlus.getSubSchemaNames()) {
      Schema subSchema = CalciteSchema.from(schemaPlus.getSubSchema(subSchemaName)).schema;

      if (subSchema instanceof BeamCalciteSchema
          && ((BeamCalciteSchema) subSchema).getTableProvider() instanceof CustomTableResolver) {
        ((CustomTableResolver) ((BeamCalciteSchema) subSchema).getTableProvider())
            .registerKnownTableNames(
                tables.stream().map(TableName::create).collect(Collectors.toList()));
      }
    }
  }

  /**
   * Data class to store simple table, its full path (excluding top-level schema), and top-level
   * schema.
   */
  static class SimpleTableWithPath {

    SimpleTable table;
    List<String> path;

    static SimpleTableWithPath of(List<String> path) {
      SimpleTableWithPath tableWithPath = new SimpleTableWithPath();
      tableWithPath.table = new SimpleTable(Iterables.getLast(path));
      tableWithPath.path = path;
      return tableWithPath;
    }

    SimpleTable getTable() {
      return table;
    }

    List<String> getPath() {
      return path;
    }
  }
}
