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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.CustomTableResolver;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for table providers that look up table metadata using full table names, instead of
 * querying it by parts of the name separately.
 */
@Experimental
public abstract class FullNameTableProvider implements TableProvider, CustomTableResolver {

  private List<TableName> knownTables;

  protected FullNameTableProvider() {
    knownTables = new ArrayList<>();
  }

  public abstract Table getTableByFullName(TableName fullTableName);

  @Override
  public void registerKnownTableNames(List<TableName> tableNames) {
    knownTables.addAll(tableNames);
  }

  @Override
  public TableProvider getSubProvider(String name) {
    // TODO: implement with trie

    // If 'name' matches a sub-schema/sub-provider we start tracking
    // the subsequent calls to getSubProvider().
    //
    // Simple table ids and final table lookup
    //
    // If there is no matching sub-schema then returning null from here indicates
    // that 'name' is either not part of this schema or it's a table, not a sub-schema,
    // this will be checked right after this in a getTable() call.
    //
    // Because this is a getSubProvider() call it means Calcite expects
    // the sub-schema/sub-provider to be returned, not a table,
    // so we only need to check against known compound table identifiers.
    // If 'name' actually represents a simple identifier then it will be checked
    // in a 'getTable()' call later. Unless there's the same sub-provider name,
    // in which case it's a conflict and we will use the sub-schema and not assume it's a table.
    // Calcite does the same.
    //
    // Here we find if there are any parsed tables that start from 'name' that belong to this
    // table provider.
    // We then create a fake tracking provider that in a trie-manner collects
    // getSubProvider()/getTable() calls by checking whether there are known parsed table names
    // matching what Calcite asks us for.
    List<TableName> tablesToLookFor =
        knownTables.stream()
            .filter(TableName::isCompound)
            .filter(tableName -> tableName.getPrefix().equals(name))
            .collect(toList());

    return tablesToLookFor.size() > 0 ? new TableNameTrackingProvider(1, tablesToLookFor) : null;
  }

  /**
   * Calcite calls getSubProvider()/getTable() on this class when resolving a table name. This class
   * keeps track of these calls and checks against known table names (extracted from a query), so
   * that when a full table name is parsed out it calls the actual table provider to get a table
   * based on the full name, instead of calling it component by component.
   *
   * <p>This class nables table providers to query their metadata source using full table names.
   */
  class TableNameTrackingProvider extends InMemoryMetaTableProvider {
    int schemaLevel;
    List<TableName> tableNames;

    TableNameTrackingProvider(int schemaLevel, List<TableName> tableNames) {
      this.schemaLevel = schemaLevel;
      this.tableNames = tableNames;
    }

    @Override
    public TableProvider getSubProvider(String name) {
      // Find if any of the parsed table names have 'name' as part
      // of their path at current index.
      //
      // If there are, return a new tracking provider for such tables and incremented index.
      //
      // If there are none, it means something weird has happened and returning null
      // will make Calcite try other schemas. Maybe things will work out.
      //
      // However since we originally register all parsed table names for the given schema
      // in this provider we should only receive a getSubProvider() call for something unknown
      // when it's a leaf path element, i.e. actual table name, which will be handled in
      // getTable() call.
      List<TableName> matchingTables =
          tableNames.stream()
              .filter(TableName::isCompound)
              .filter(tableName -> tableName.getPath().size() > schemaLevel)
              .filter(tableName -> tableName.getPath().get(schemaLevel).equals(name))
              .collect(toList());

      return matchingTables.size() > 0
          ? new TableNameTrackingProvider(schemaLevel + 1, matchingTables)
          : null;
    }

    @Override
    public String getTableType() {
      return "google.cloud.datacatalog.subprovider";
    }

    @Nullable
    @Override
    public Table getTable(String name) {

      // This is called only after getSubProvider() returned null,
      // and since we are tracking the actual parsed table names, this should
      // be it, there should exist a parsed table that matches the 'name'.

      Optional<TableName> matchingTable =
          tableNames.stream()
              .filter(tableName -> tableName.getTableName().equals(name))
              .findFirst();

      TableName fullTableName =
          matchingTable.orElseThrow(
              () ->
                  new IllegalStateException(
                      "Unexpected table '"
                          + name
                          + "' requested. Current schema level is "
                          + schemaLevel
                          + ". Current known table names: "
                          + tableNames.toString()));
      return FullNameTableProvider.this.getTableByFullName(fullTableName);
    }

    @Override
    public synchronized BeamSqlTable buildBeamSqlTable(Table table) {
      return FullNameTableProvider.this.buildBeamSqlTable(table);
    }
  }
}
