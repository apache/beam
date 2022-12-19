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
package org.apache.beam.sdk.extensions.spd;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code PCollectionTableProvider} provides in-memory set of {@code BeamSqlTable BeamSqlTables}
 * based on PCollections.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PCollectionTableProvider implements TableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(PCollectionTableProvider.class);

  private String tableType;
  private Map<String, PCollection<?>> collections;
  private Map<String, PCollectionTableProvider> subproviders;

  public PCollectionTableProvider(String tableType) {
    this.tableType = tableType;
    this.collections = new HashMap<>();
    this.subproviders = new HashMap<>();
  }

  @Override
  public String getTableType() {
    return tableType;
  }

  public Table associatePCollection(String tableName, PCollection<?> collection) throws Exception {
    if (!collection.hasSchema()) {
      throw new Exception("PCollection must have a schema to be used as a table");
    }
    LOG.info("Registering table " + tableName + " with schema " + collection.getSchema());
    collections.put(tableName, collection);
    return getTable(tableName);
  }

  @Override
  public void createTable(Table table) {
    // NO-OP. You associate the PCollection which is then used to create the table in the metastore
    // so we
    // don't need to store that here
  }

  @Override
  public void dropTable(String tableName) {
    collections.remove(tableName);
  }

  @Override
  public Map<String, Table> getTables() {
    HashMap<String, Table> tables = new HashMap<>();
    for (Map.Entry<String, PCollection<?>> e : collections.entrySet()) {
      Table t = getTable(e.getKey());
      if (t != null) {
        tables.put(e.getKey(), t);
      }
    }
    return tables;
  }

  @Override
  public @Nullable Table getTable(String tableName) {
    LOG.info("Looking up " + tableName);
    PCollection<?> collection = collections.get(tableName);
    if (collection == null) {
      return null;
    }
    LOG.info("Building table " + tableName);
    return Table.builder().name(tableName).type(tableType).schema(collection.getSchema()).build();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    String name = table.getName();
    if (name == null) {

      return null;
    }
    PCollection<?> collection = collections.get(name);
    if (collection == null) {
      return null;
    }
    return new BeamPCollectionTable<>(collection);
  }

  public PCollection<?> getPCollection(Table table) {
    return getPCollection(table.getName());
  }

  @Nullable
  public PCollection<?> getPCollection(String tableName) {
    return collections.get(tableName);
  }

  @Override
  public TableProvider getSubProvider(String name) {
    return subproviders.computeIfAbsent(name, (key) -> new PCollectionTableProvider(name));
  }
}
