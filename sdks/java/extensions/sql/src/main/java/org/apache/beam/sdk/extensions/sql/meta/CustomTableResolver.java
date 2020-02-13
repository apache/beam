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
package org.apache.beam.sdk.extensions.sql.meta;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * Interface that table providers can implement if they require custom table name resolution.
 *
 * <p>{@link #registerKnownTableNames(List)} is called by the parser/planner and takes the list of
 * all tables mentioned in the query. Then when normal Calcite lifecycle is executed the table
 * provider can now check against this list and perform custom resolution. This is a workaround for
 * lack of context in Calcite's logic, e.g. it's impossible to receive the whole table name at once,
 * or understand that it has done querying sub-schemas and expects a table.
 */
public interface CustomTableResolver extends TableProvider {

  /**
   * Register the table names as extracted from the FROM clause.
   *
   * <p>Calcite doesn't provide these full names to table providers and queries them with individual
   * parts of the identifiers without giving any extra context. So if a table provider needs to
   * implement some custom table name resolution strategy it doesn't have information to do so. E.g.
   * if you want to take the compound SQL identifiers that were originally split by dots, join them
   * into a single string, and then query a back-end service, this interface makes this possible.
   */
  void registerKnownTableNames(List<TableName> tableNames);
}
