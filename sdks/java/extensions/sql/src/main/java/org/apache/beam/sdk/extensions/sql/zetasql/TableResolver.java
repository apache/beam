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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.util.List;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

/** An interface to implement a custom resolution strategy. */
interface TableResolver {

  TableResolver DEFAULT_ASSUME_LEAF_IS_TABLE = TableResolverImpl::assumeLeafIsTable;
  TableResolver JOIN_INTO_COMPOUND_ID = TableResolverImpl::joinIntoCompoundId;

  /**
   * Returns a resolved table given a table path.
   *
   * <p>Returns null if table is not found.
   */
  Table resolveCalciteTable(Schema calciteSchema, List<String> tablePath);
}
