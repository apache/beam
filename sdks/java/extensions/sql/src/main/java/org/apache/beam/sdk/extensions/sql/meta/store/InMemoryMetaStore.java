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

package org.apache.beam.sdk.extensions.sql.meta.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * A {@link MetaStore} which stores the meta info in memory.
 *
 * <p>NOTE, because this implementation is memory based, the metadata is NOT persistent.
 * for tables which created, you need to create again every time you launch the
 * {@link org.apache.beam.sdk.extensions.sql.BeamSqlCli}.
 */
public class InMemoryMetaStore implements MetaStore {
  private Map<String, Table> tables = new HashMap<>();
  private Map<String, TableProvider> providers = new HashMap<>();

  @Override public String getTableType() {
    return "store";
  }

  @Override public void createTable(Table table) {
    validateTableType(table);

    // first assert the table name is unique
    if (tables.containsKey(table.getName())) {
      throw new IllegalArgumentException("Duplicate table name: " + table.getName());
    }

    // invoke the provider's create
    providers.get(table.getType()).createTable(table);

    // store to the global metastore
    tables.put(table.getName(), table);
  }

  @Override public void dropTable(String tableName) {
    if (!tables.containsKey(tableName)) {
      throw new IllegalArgumentException("No such table: " + tableName);
    }

    Table table = tables.get(tableName);
    providers.get(table.getType()).dropTable(tableName);
    tables.remove(tableName);
  }

  @Override public Table getTable(String tableName) {
    if (tableName == null) {
      return null;
    }
    return tables.get(tableName.toLowerCase());
  }

  @Override public List<Table> listTables() {
    return new ArrayList<>(tables.values());
  }

  @Override public BeamSqlTable buildBeamSqlTable(Table table) {
    TableProvider provider = providers.get(table.getType());

    return provider.buildBeamSqlTable(table);
  }

  private void validateTableType(Table table) {
    if (!providers.containsKey(table.getType())) {
      throw new IllegalArgumentException(
          "Table type: " + table.getType() + " not supported!");
    }
  }

  @Override public void registerProvider(TableProvider provider) {
    if (providers.containsKey(provider.getTableType())) {
      throw new IllegalArgumentException("Provider is already registered for table type: "
          + provider.getTableType());
    }

    this.providers.put(provider.getTableType(), provider);
    initTablesFromProvider(provider);
  }

  private void initTablesFromProvider(TableProvider provider) {
    List<Table> tables = provider.listTables();
    for (Table table : tables) {
      if (this.tables.containsKey(table.getName())) {
        throw new IllegalStateException(
            "Duplicate table: " + table.getName() + " from provider: " + provider);
      }

      this.tables.put(table.getName(), table);
    }
  }

  Map<String, TableProvider> getProviders() {
    return providers;
  }
}
