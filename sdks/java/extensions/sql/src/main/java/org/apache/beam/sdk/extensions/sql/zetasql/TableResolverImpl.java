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

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

/** A couple of implementations of TableResolver. */
class TableResolverImpl {

  /**
   * Uses the logic similar to Calcite's EmptyScope.resolve_(...) except assumes the last element in
   * the table path is a table name (which is assumed by ZetaSQL API getTableNames()).
   *
   * <p>This is the default.
   *
   * <p>I.e. drills down into schema.getSubschema() until the second last element of the table path,
   * then calls schema.getTable().
   */
  static Table assumeLeafIsTable(Schema schema, List<String> tablePath) {
    Schema subSchema = schema;

    // subSchema.getSubschema() for all except last
    for (int i = 0; i < tablePath.size() - 1; i++) {
      subSchema = subSchema.getSubSchema(tablePath.get(i));
    }

    // for the final one call getTable()
    return subSchema.getTable(Iterables.getLast(tablePath));
  }

  /**
   * Joins the table name parts into a single ZetaSQL-compatible compound identifier, then calls
   * schema.getTable().
   *
   * <p>This is the input expected, for example, by Data Catalog.
   *
   * <p>Escapes slashes, backticks, quotes, for details see {@link
   * ZetaSqlIdUtils#escapeAndJoin(List)}.
   */
  static Table joinIntoCompoundId(Schema schema, List<String> tablePath) {
    return schema.getTable(ZetaSqlIdUtils.escapeAndJoin(tablePath));
  }
}
