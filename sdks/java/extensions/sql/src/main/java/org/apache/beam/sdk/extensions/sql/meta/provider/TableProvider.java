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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * A {@code TableProvider} handles the metadata CRUD of a specified kind of tables.
 *
 * <p>So there will be a provider to handle textfile(CSV) based tables, there is a provider to
 * handle MySQL based tables, a provider to handle Casandra based tables etc.
 */
public interface TableProvider {
  /**
   * Init the provider.
   */
  void init();

  /**
   * Gets the table type this provider handles.
   */
  String getTableType();

  /**
   * Creates a table.
   */
  void createTable(Table table);

  /**
   * Drops a table.
   *
   * @param tableName
   */
  void dropTable(String tableName);

  /**
   * List all tables from this provider.
   */
  List<Table> listTables();

  /**
   * Build a {@link BeamSqlTable} using the given table meta info.
   */
  BeamSqlTable buildBeamSqlTable(Table table);

  /**
   * Close the provider.
   */
  void close();
}
