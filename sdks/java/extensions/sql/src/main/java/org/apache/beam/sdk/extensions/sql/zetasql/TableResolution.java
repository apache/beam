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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

/** Utility methods to resolve a table, given a top-level Calcite schema and a table path. */
public class TableResolution {

  /**
   * Returns Calcite Table by consulting the schema.
   *
   * <p>The way the schema is queried is defined by the name resolution strategey implemented by a
   * TableResolver and stored as a TableResolutionContext in the context.
   *
   * <p>If no custom table resolution logic is provided, default one is used, which is: drill down
   * the getSubschema() path until the second-to-last path element. We expect the path to be a table
   * path, so the last element should be a valid table id, we don't expect anything else there.
   *
   * <p>This resembles a default Calcite planner strategy. One difference is that Calcite doesn't
   * assume the last element is a table and will continue to call getSubschema(), making it
   * impossible for a table provider to understand the context.
   */
  public static Table resolveCalciteTable(
      Context context, SchemaPlus schemaPlus, List<String> tablePath) {
    TableResolutionContext tableResolutionContext = context.unwrap(TableResolutionContext.class);
    TableResolver tableResolver = getTableResolver(tableResolutionContext, schemaPlus.getName());
    return tableResolver.resolveCalciteTable(schemaPlus, tablePath);
  }

  static TableResolver getTableResolver(
      TableResolutionContext tableResolutionContext, String schemaName) {
    if (tableResolutionContext == null
        || !tableResolutionContext.hasCustomResolutionFor(schemaName)) {
      return TableResolver.DEFAULT_ASSUME_LEAF_IS_TABLE;
    }

    return tableResolutionContext.getTableResolver(schemaName);
  }

  /**
   * Data class to store simple table, its full path (excluding top-level schema), and top-level
   * schema.
   */
  static class SimpleTableWithPath {

    SimpleTable table;
    List<String> path;
    String topLevelSchema;

    static SimpleTableWithPath of(String topLevelSchema, List<String> path) {
      SimpleTableWithPath tableWithPath = new SimpleTableWithPath();
      tableWithPath.table = new SimpleTable(Iterables.getLast(path));
      tableWithPath.path = path;
      tableWithPath.topLevelSchema = topLevelSchema;
      return tableWithPath;
    }

    SimpleTable getTable() {
      return table;
    }

    List<String> getPath() {
      return path;
    }

    String getTopLevelSchema() {
      return topLevelSchema;
    }
  }
}
