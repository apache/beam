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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/** Test in-memory table provider for use in tests. */
public class TestTableProvider implements TableProvider {
  private Map<String, Table> tables = new HashMap<>();

  @Override
  public String getTableType() {
    return "test";
  }

  @Override
  public void createTable(Table table) {
    tables.put(table.getName(), table);
  }

  @Override
  public void dropTable(String tableName) {
    tables.remove(tableName);
  }

  @Override
  public Map<String, Table> getTables() {
    return tables;
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    throw new UnsupportedOperationException("Test table provider cannot build tables");
  }
}
