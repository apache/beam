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
package org.apache.beam.sdk.io.iceberg;

/**
 * Kinds of table schema change {@link AddFiles} may make so that a file's columns are covered by
 * the table schema. Evolution is all or nothing per file schema: a file schema needing a change
 * that is not allowed is incompatible; see {@link SchemaEvolutionConfig.IncompatibleSchemaHandling}
 * for what happens to its files. Pinned columns ({@link
 * SchemaEvolutionConfig#getRequiredColumns()}) are never relaxed whatever the options.
 *
 * <p>The options constrain changes to columns the table already has when a window's schema commit
 * starts (the whole input, in batch). A column that is new in that window takes the union of the
 * window's file schemas: its type is the widest among them and it is optional unless pinned, so two
 * files that disagree about a new column never conflict with each other, only with the table.
 */
public enum SchemaEvolutionOption {
  /**
   * Add columns present in files but absent from the table, as optional columns at every level (a
   * pinned column is created required). Also lets this transform create a missing table from the
   * union of the file schemas.
   */
  ALLOW_FIELD_ADDITION,
  /**
   * Make a required table column optional when a file has nulls in it, carries no null-count
   * statistics for it, or lacks it entirely (every row would read null). For a column outside lists
   * and maps, a file whose footer proves zero nulls never triggers this, however its writer
   * declared the column; under a list or map the declaration is taken as is. Relaxation is
   * table-wide and permanent; pin the columns that must stay required.
   */
  ALLOW_FIELD_RELAXATION,
  /**
   * Widen a column's type (int to long, float to double, decimal precision) when a file needs it.
   */
  ALLOW_TYPE_PROMOTION
}
