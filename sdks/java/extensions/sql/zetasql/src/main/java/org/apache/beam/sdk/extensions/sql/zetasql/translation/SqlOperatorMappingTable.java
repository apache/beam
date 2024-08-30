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

import com.google.zetasql.resolvedast.ResolvedNodes;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** SqlOperatorMappingTable. */
class SqlOperatorMappingTable {

  // todo: Some of operators defined here are later overridden in ZetaSQLPlannerImpl.
  // We should remove them from this table and add generic way to provide custom
  // implementation. (Ex.: timestamp_add)
  static final Map<String, Function<ResolvedNodes.ResolvedFunctionCallBase, SqlOperator>>
      ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR =
          ImmutableMap
              .<String, Function<ResolvedNodes.ResolvedFunctionCallBase, SqlOperator>>builder()
              // grouped window function
              .put("TUMBLE", resolvedFunction -> SqlStdOperatorTable.TUMBLE_OLD)
              .put("HOP", resolvedFunction -> SqlStdOperatorTable.HOP_OLD)
              .put("SESSION", resolvedFunction -> SqlStdOperatorTable.SESSION_OLD)

              // ZetaSQL functions
              .put("$and", resolvedFunction -> SqlStdOperatorTable.AND)
              .put("$or", resolvedFunction -> SqlStdOperatorTable.OR)
              .put("$not", resolvedFunction -> SqlStdOperatorTable.NOT)
              .put("$equal", resolvedFunction -> SqlStdOperatorTable.EQUALS)
              .put("$not_equal", resolvedFunction -> SqlStdOperatorTable.NOT_EQUALS)
              .put("$greater", resolvedFunction -> SqlStdOperatorTable.GREATER_THAN)
              .put(
                  "$greater_or_equal",
                  resolvedFunction -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
              .put("$less", resolvedFunction -> SqlStdOperatorTable.LESS_THAN)
              .put("$less_or_equal", resolvedFunction -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
              .put("$like", resolvedFunction -> SqlOperators.LIKE)
              .put("$is_null", resolvedFunction -> SqlStdOperatorTable.IS_NULL)
              .put("$is_true", resolvedFunction -> SqlStdOperatorTable.IS_TRUE)
              .put("$is_false", resolvedFunction -> SqlStdOperatorTable.IS_FALSE)
              .put("$add", resolvedFunction -> SqlStdOperatorTable.PLUS)
              .put("$subtract", resolvedFunction -> SqlStdOperatorTable.MINUS)
              .put("$multiply", resolvedFunction -> SqlStdOperatorTable.MULTIPLY)
              .put("$unary_minus", resolvedFunction -> SqlStdOperatorTable.UNARY_MINUS)
              .put("$divide", resolvedFunction -> SqlStdOperatorTable.DIVIDE)
              .put("concat", resolvedFunction -> SqlOperators.CONCAT)
              .put("substr", resolvedFunction -> SqlOperators.SUBSTR)
              .put("substring", resolvedFunction -> SqlOperators.SUBSTR)
              .put("trim", resolvedFunction -> SqlOperators.TRIM)
              .put("replace", resolvedFunction -> SqlOperators.REPLACE)
              .put("char_length", resolvedFunction -> SqlOperators.CHAR_LENGTH)
              .put("starts_with", resolvedFunction -> SqlOperators.START_WITHS)
              .put("ends_with", resolvedFunction -> SqlOperators.ENDS_WITH)
              .put("ltrim", resolvedFunction -> SqlOperators.LTRIM)
              .put("rtrim", resolvedFunction -> SqlOperators.RTRIM)
              .put("reverse", resolvedFunction -> SqlOperators.REVERSE)
              .put("$count_star", resolvedFunction -> SqlStdOperatorTable.COUNT)
              .put("max", resolvedFunction -> SqlStdOperatorTable.MAX)
              .put("min", resolvedFunction -> SqlStdOperatorTable.MIN)
              .put("avg", resolvedFunction -> SqlStdOperatorTable.AVG)
              .put("sum", resolvedFunction -> SqlStdOperatorTable.SUM)
              .put("any_value", resolvedFunction -> SqlStdOperatorTable.ANY_VALUE)
              .put("count", resolvedFunction -> SqlStdOperatorTable.COUNT)
              .put("bit_and", resolvedFunction -> SqlStdOperatorTable.BIT_AND)
              .put("string_agg", SqlOperators::createStringAggOperator) // NULL values not supported
              .put("array_agg", resolvedFunction -> SqlOperators.ARRAY_AGG_FN)
              .put("bit_or", resolvedFunction -> SqlStdOperatorTable.BIT_OR)
              .put("bit_xor", resolvedFunction -> SqlOperators.BIT_XOR)
              .put("ceil", resolvedFunction -> SqlStdOperatorTable.CEIL)
              .put("floor", resolvedFunction -> SqlStdOperatorTable.FLOOR)
              .put("mod", resolvedFunction -> SqlStdOperatorTable.MOD)
              .put("timestamp", resolvedFunction -> SqlOperators.TIMESTAMP_OP)
              .put("$case_no_value", resolvedFunction -> SqlStdOperatorTable.CASE)

              // if operator - IF(cond, pos, neg) can actually be mapped directly to `CASE WHEN cond
              // THEN pos ELSE neg`
              .put("if", resolvedFunction -> SqlStdOperatorTable.CASE)

              // $case_no_value specializations
              // all of these operators can have their operands adjusted to achieve the same thing
              // with
              // a call to $case_with_value
              .put("$case_with_value", resolvedFunction -> SqlStdOperatorTable.CASE)
              .put("coalesce", resolvedFunction -> SqlStdOperatorTable.CASE)
              .put("ifnull", resolvedFunction -> SqlStdOperatorTable.CASE)
              .put("nullif", resolvedFunction -> SqlStdOperatorTable.CASE)
              .put("countif", resolvedFunction -> SqlOperators.COUNTIF)
              .build();

  static final Map<String, SqlOperatorRewriter> ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR_REWRITER =
      ImmutableMap.<String, SqlOperatorRewriter>builder()
          .put("$case_with_value", new SqlCaseWithValueOperatorRewriter())
          .put("coalesce", new SqlCoalesceOperatorRewriter())
          .put("ifnull", new SqlIfNullOperatorRewriter())
          .put("nullif", new SqlNullIfOperatorRewriter())
          .put("$in", new SqlInOperatorRewriter())
          .build();

  static @Nullable SqlOperator create(
      ResolvedNodes.ResolvedFunctionCallBase aggregateFunctionCall) {

    Function<ResolvedNodes.ResolvedFunctionCallBase, SqlOperator> sqlOperatorFactory =
        ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(aggregateFunctionCall.getFunction().getName());

    if (sqlOperatorFactory != null) {
      return sqlOperatorFactory.apply(aggregateFunctionCall);
    }
    return null;
  }
}
