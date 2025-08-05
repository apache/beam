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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig.IcebergTableInfo;
import org.apache.beam.sdk.io.iceberg.TableAlreadyExistsException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMetastore implements MetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergMetastore.class);
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;
  private final Map<String, Table> cachedTables = new HashMap<>();
  private final String database;

  public IcebergMetastore(String db, IcebergCatalogConfig catalogConfig) {
    this.database = db;
    this.catalogConfig = catalogConfig;
  }

  @Override
  public String getTableType() {
    return "iceberg";
  }

  @Override
  public void createTable(Table table) {
    String identifier = getIdentifier(table);
    try {
      catalogConfig.createTable(identifier, table.getSchema(), table.getPartitionFields());
    } catch (TableAlreadyExistsException e) {
      LOG.info("Iceberg table '{}' already exists at location '{}'.", table.getName(), identifier);
    }
    cachedTables.put(table.getName(), table);
  }

  @Override
  public void dropTable(String tableName) {
    String identifier = getIdentifier(tableName);
    if (catalogConfig.dropTable(identifier)) {
      LOG.info("Dropped table '{}' (path: '{}').", tableName, identifier);
    } else {
      LOG.info(
          "Ignoring DROP TABLE call for '{}' (path: '{}') because it does not exist.",
          tableName,
          identifier);
    }
    cachedTables.remove(tableName);
  }

  @Override
  public Map<String, Table> getTables() {
    for (String id : catalogConfig.listTables(database)) {
      String name = Iterables.getLast(Splitter.on(".").split(id));
      if (!cachedTables.containsKey(name)) {
        Table table = checkStateNotNull(loadTable(id));
        cachedTables.put(name, table);
      }
    }
    return cachedTables;
  }

  @Override
  public @Nullable Table getTable(String name) {
    if (cachedTables.containsKey(name)) {
      return cachedTables.get(name);
    }
    @Nullable Table table = loadTable(getIdentifier(name));
    if (table != null) {
      cachedTables.put(name, table);
    }
    return table;
  }

  private String getIdentifier(String name) {
    return database + "." + name;
  }

  private String getIdentifier(Table table) {
    if (table.getLocation() != null) {
      return table.getLocation();
    }
    return getIdentifier(table.getName());
  }

  private @Nullable Table loadTable(String identifier) {
    @Nullable IcebergTableInfo tableInfo = catalogConfig.loadTable(identifier);
    if (tableInfo == null) {
      return null;
    }
    String name = Iterables.getLast(Splitter.on(".").split(tableInfo.getIdentifier()));
    return Table.builder()
        .type(getTableType())
        .name(name)
        .schema(tableInfo.getSchema())
        .location(tableInfo.getIdentifier())
        .properties(TableUtils.parseProperties(tableInfo.getProperties()))
        .build();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new IcebergTable(getIdentifier(table), table, catalogConfig);
  }

  @Override
  public boolean supportsPartitioning(Table table) {
    return true;
  }

  @Override
  public void registerProvider(TableProvider provider) {
    // no-op
  }

  @Override
  public void clearProviders() {
    // no-op
  }
}
