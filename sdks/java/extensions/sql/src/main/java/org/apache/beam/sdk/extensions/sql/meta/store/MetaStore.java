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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * The interface to handle CRUD of {@code BeamSql} table metadata.
 */
public interface MetaStore {
  /**
   * create a table.
   */
  void createTable(Table table);

  /**
   * drop a table.
   */
  void dropTable(String tableName);

  /**
   * Get table with the specified name.
   */
  Table getTable(String tableName);

  /**
   * List all the tables.
   */
  List<Table> listTables();

  /**
   * Build the {@code BeamSqlTable} for the specified table.
   */
  BeamSqlTable buildBeamSqlTable(String tableName);

  /**
   * Register a table provider.
   * @param provider
   */
  void registerProvider(TableProvider provider);
}
