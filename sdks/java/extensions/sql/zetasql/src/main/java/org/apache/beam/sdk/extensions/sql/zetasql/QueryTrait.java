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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.zetasql.Table;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOutputColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.zetasql.TableResolution.SimpleTableWithPath;

/** QueryTrait. */
public class QueryTrait {
  public Map<String, ResolvedWithEntry> withEntries = new HashMap<>();

  public Map<ResolvedColumn, String> outputColumnMap = new HashMap<>();

  public Map<Long, SimpleTableWithPath> resolvedTables = new HashMap<>();

  // TODO: move query parameter map to QueryTrait.

  public void addOutputColumnList(List<ResolvedOutputColumn> outputColumnList) {
    outputColumnList.forEach(
        column -> {
          outputColumnMap.put(column.getColumn(), column.getName());
        });
  }

  /** Store a table together with its full path for repeated resolutions. */
  public void addResolvedTable(SimpleTableWithPath tableWithPath) {
    // table ids are autoincremted in SimpleTable
    resolvedTables.put(tableWithPath.getTable().getId(), tableWithPath);
  }

  /** True if the table was resolved using the Calcite schema. */
  public boolean isTableResolved(Table table) {
    return resolvedTables.containsKey(table.getId());
  }

  /** Returns a full table path (exlucding top-level schema) for a given ZetaSQL Table. */
  public List<String> getTablePath(Table table) {
    checkArgument(
        isTableResolved(table),
        "Attempting to get a path of an unresolved table. Resolve and add the table first: %s",
        table.getFullName());
    return resolvedTables.get(table.getId()).getPath();
  }

  public List<String> retrieveFieldNames(List<ResolvedColumn> resolvedColumnList) {
    return resolvedColumnList.stream().map(this::resolveAlias).collect(Collectors.toList());
  }

  public String resolveAlias(ResolvedColumn resolvedColumn) {
    return this.outputColumnMap.getOrDefault(resolvedColumn, resolvedColumn.getName());
  }
}
