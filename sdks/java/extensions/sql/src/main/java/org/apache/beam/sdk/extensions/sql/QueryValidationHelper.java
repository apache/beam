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

package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.QueryTransform.PCOLLECTION_NAME;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * QueryValidationHelper.
 */
class QueryValidationHelper {

  static void validateQuery(BeamSqlEnv sqlEnv, String queryString) {
    SqlNode sqlNode;

    try {
      sqlNode = sqlEnv.getPlanner().parseQuery(queryString);
      sqlEnv.getPlanner().getPlanner().close();
    } catch (SqlParseException e) {
      throw new IllegalStateException(e);
    }

    if (!(sqlNode instanceof SqlSelect)) {
      throw new UnsupportedOperationException(
          "Sql operation " + sqlNode.toString() + " is not supported");
    }

    if (!PCOLLECTION_NAME.equalsIgnoreCase(((SqlSelect) sqlNode).getFrom().toString())) {
      throw new IllegalStateException("Use " + PCOLLECTION_NAME + " as table name"
                                          + " when selecting from single PCollection."
                                          + " Use PCollectionTuple to explicitly "
                                          + "name the input PCollections");
    }
  }
}
