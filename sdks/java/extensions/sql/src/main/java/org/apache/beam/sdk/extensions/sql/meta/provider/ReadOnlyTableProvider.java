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

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;

/**
 * A {@code ReadOnlyTableProvider} provides in-memory read only set of {@code BeamSqlTable
 * BeamSqlTables}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ReadOnlyTableProvider implements TableProvider {
  private final String typeName;
  private final Map<String, BeamSqlTable> tables;

  public ReadOnlyTableProvider(String typeName, Map<String, BeamSqlTable> tables) {
    this.typeName = typeName;
    this.tables = tables;
  }

  @Override
  public String getTableType() {
    return typeName;
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException(
        "ReadOnlyTableProvider does not support table creation");
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException(
        "ReadOnlyTableProvider does not support table deletion");
  }

  @Override
  public Map<String, Table> getTables() {
    ImmutableMap.Builder<String, Table> map = ImmutableMap.builder();
    for (Map.Entry<String, BeamSqlTable> table : tables.entrySet()) {
      map.put(
          table.getKey(),
          Table.builder()
              .type(getTableType())
              .name(table.getKey())
              .schema(Schema.builder().build())
              .build());
    }
    return map.build();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return tables.get(table.getName());
  }
}
