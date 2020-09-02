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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import java.util.Map;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** SqlOperatorMappingTable. */
class SqlOperatorMappingTable {

  // todo: Some of operators defined here are later overridden in ZetaSQLPlannerImpl.
  // We should remove them from this table and add generic way to provide custom
  // implementation. (Ex.: timestamp_add)
  static final Map<String, SqlOperator> ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR =
      ImmutableMap.<String, SqlOperator>builder()
          // grouped window function
          .put("TUMBLE", SqlStdOperatorTable.TUMBLE)
          .put("HOP", SqlStdOperatorTable.HOP)
          .put("SESSION", SqlStdOperatorTable.SESSION)

          // ZetaSQL functions
          .put("$and", SqlStdOperatorTable.AND)
          .put("$or", SqlStdOperatorTable.OR)
          .put("$not", SqlStdOperatorTable.NOT)
          .put("$equal", SqlStdOperatorTable.EQUALS)
          .put("$not_equal", SqlStdOperatorTable.NOT_EQUALS)
          .put("$greater", SqlStdOperatorTable.GREATER_THAN)
          .put("$greater_or_equal", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
          .put("$less", SqlStdOperatorTable.LESS_THAN)
          .put("$less_or_equal", SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
          .put("$like", SqlOperators.LIKE)
          .put("$in", SqlStdOperatorTable.IN)
          .put("$is_null", SqlStdOperatorTable.IS_NULL)
          .put("$is_true", SqlStdOperatorTable.IS_TRUE)
          .put("$is_false", SqlStdOperatorTable.IS_FALSE)
          .put("$add", SqlStdOperatorTable.PLUS)
          .put("$subtract", SqlStdOperatorTable.MINUS)
          .put("$multiply", SqlStdOperatorTable.MULTIPLY)
          .put("$unary_minus", SqlStdOperatorTable.UNARY_MINUS)
          .put("$divide", SqlStdOperatorTable.DIVIDE)
          .put("concat", SqlOperators.CONCAT)
          .put("substr", SqlOperators.SUBSTR)
          .put("trim", SqlOperators.TRIM)
          .put("replace", SqlOperators.REPLACE)
          .put("char_length", SqlOperators.CHAR_LENGTH)
          .put("starts_with", SqlOperators.START_WITHS)
          .put("ends_with", SqlOperators.ENDS_WITH)
          .put("ltrim", SqlOperators.LTRIM)
          .put("rtrim", SqlOperators.RTRIM)
          .put("reverse", SqlOperators.REVERSE)
          .put("$count_star", SqlStdOperatorTable.COUNT)
          .put("max", SqlStdOperatorTable.MAX)
          .put("min", SqlStdOperatorTable.MIN)
          .put("avg", SqlStdOperatorTable.AVG)
          .put("sum", SqlStdOperatorTable.SUM)
          .put("any_value", SqlStdOperatorTable.ANY_VALUE)
          .put("count", SqlStdOperatorTable.COUNT)
          // .put("bit_and", SqlStdOperatorTable.BIT_AND) //JIRA link:
          // https://issues.apache.org/jira/browse/BEAM-10379
          .put("string_agg", SqlOperators.STRING_AGG_STRING_FN) // NULL values not supported
          .put("bit_or", SqlStdOperatorTable.BIT_OR)
          .put("ceil", SqlStdOperatorTable.CEIL)
          .put("floor", SqlStdOperatorTable.FLOOR)
          .put("mod", SqlStdOperatorTable.MOD)
          .put("timestamp", SqlOperators.TIMESTAMP_OP)
          .put("$case_no_value", SqlStdOperatorTable.CASE)

          // if operator - IF(cond, pos, neg) can actually be mapped directly to `CASE WHEN cond
          // THEN pos ELSE neg`
          .put("if", SqlStdOperatorTable.CASE)

          // $case_no_value specializations
          // all of these operators can have their operands adjusted to achieve the same thing with
          // a call to $case_with_value
          .put("$case_with_value", SqlStdOperatorTable.CASE)
          .put("coalesce", SqlStdOperatorTable.CASE)
          .put("ifnull", SqlStdOperatorTable.CASE)
          .put("nullif", SqlStdOperatorTable.CASE)
          .build();

  static final Map<String, SqlOperatorRewriter> ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR_REWRITER =
      ImmutableMap.<String, SqlOperatorRewriter>builder()
          .put("$case_with_value", new SqlCaseWithValueOperatorRewriter())
          .put("coalesce", new SqlCoalesceOperatorRewriter())
          .put("ifnull", new SqlIfNullOperatorRewriter())
          .put("nullif", new SqlNullIfOperatorRewriter())
          .build();
}
