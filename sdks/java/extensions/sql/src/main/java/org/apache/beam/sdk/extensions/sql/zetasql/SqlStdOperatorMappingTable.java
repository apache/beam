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

import com.google.zetasql.ZetaSQLFunction.FunctionSignatureId;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** SqlStdOperatorMappingTable. */
@Internal
public class SqlStdOperatorMappingTable {
  static final List<FunctionSignatureId> ZETASQL_BUILTIN_FUNCTION_WHITELIST =
      Arrays.asList(
          FunctionSignatureId.FN_AND,
          FunctionSignatureId.FN_OR,
          FunctionSignatureId.FN_NOT,
          FunctionSignatureId.FN_MULTIPLY_DOUBLE,
          FunctionSignatureId.FN_MULTIPLY_INT64,
          FunctionSignatureId.FN_MULTIPLY_NUMERIC,
          FunctionSignatureId.FN_DIVIDE_DOUBLE,
          FunctionSignatureId.FN_DIVIDE_NUMERIC,
          FunctionSignatureId.FN_ADD_DOUBLE,
          FunctionSignatureId.FN_ADD_INT64,
          FunctionSignatureId.FN_ADD_NUMERIC,
          FunctionSignatureId.FN_SUBTRACT_DOUBLE,
          FunctionSignatureId.FN_SUBTRACT_INT64,
          FunctionSignatureId.FN_SUBTRACT_NUMERIC,
          FunctionSignatureId.FN_UNARY_MINUS_INT64,
          FunctionSignatureId.FN_UNARY_MINUS_DOUBLE,
          FunctionSignatureId.FN_UNARY_MINUS_NUMERIC,
          FunctionSignatureId.FN_GREATER,
          FunctionSignatureId.FN_GREATER_OR_EQUAL,
          FunctionSignatureId.FN_LESS,
          FunctionSignatureId.FN_LESS_OR_EQUAL,
          FunctionSignatureId.FN_EQUAL,
          FunctionSignatureId.FN_NOT_EQUAL,
          FunctionSignatureId.FN_IS_NULL,
          FunctionSignatureId.FN_IS_TRUE,
          FunctionSignatureId.FN_IS_FALSE,
          FunctionSignatureId.FN_STARTS_WITH_STRING,
          FunctionSignatureId.FN_SUBSTR_STRING,
          FunctionSignatureId.FN_TRIM_STRING,
          FunctionSignatureId.FN_LTRIM_STRING,
          FunctionSignatureId.FN_RTRIM_STRING,
          FunctionSignatureId.FN_REPLACE_STRING,
          FunctionSignatureId.FN_CONCAT_STRING,
          FunctionSignatureId.FN_COUNT_STAR,
          FunctionSignatureId.FN_COUNT,
          FunctionSignatureId.FN_MAX,
          FunctionSignatureId.FN_MIN,
          FunctionSignatureId.FN_AVG_DOUBLE,
          FunctionSignatureId.FN_AVG_INT64,
          FunctionSignatureId.FN_AVG_NUMERIC,
          FunctionSignatureId.FN_SUM_DOUBLE,
          FunctionSignatureId.FN_SUM_INT64,
          FunctionSignatureId.FN_SUM_NUMERIC,
          FunctionSignatureId.FN_MOD_INT64,
          FunctionSignatureId.FN_MOD_NUMERIC,
          FunctionSignatureId.FN_CASE_NO_VALUE,
          FunctionSignatureId.FN_CASE_WITH_VALUE,
          FunctionSignatureId.FN_TIMESTAMP_ADD,
          // TODO: FunctionSignatureId.FN_TIMESTAMP_SUB,
          FunctionSignatureId.FN_FLOOR_DOUBLE,
          FunctionSignatureId.FN_FLOOR_NUMERIC,
          FunctionSignatureId.FN_CEIL_DOUBLE,
          FunctionSignatureId.FN_CEIL_NUMERIC,
          FunctionSignatureId.FN_REVERSE_STRING,
          FunctionSignatureId.FN_CHAR_LENGTH_STRING,
          FunctionSignatureId.FN_ENDS_WITH_STRING,
          FunctionSignatureId.FN_STRING_LIKE,
          FunctionSignatureId.FN_COALESCE,
          FunctionSignatureId.FN_IF,
          FunctionSignatureId.FN_IFNULL,
          FunctionSignatureId.FN_NULLIF,
          FunctionSignatureId.FN_EXTRACT_FROM_DATE,
          FunctionSignatureId.FN_EXTRACT_FROM_DATETIME,
          FunctionSignatureId.FN_EXTRACT_FROM_TIME,
          FunctionSignatureId.FN_EXTRACT_FROM_TIMESTAMP,
          FunctionSignatureId.FN_TIMESTAMP_FROM_STRING,
          FunctionSignatureId.FN_TIMESTAMP_FROM_DATE,
          // TODO: FunctionSignatureId.FN_TIMESTAMP_FROM_DATETIME
          FunctionSignatureId.FN_DATE_FROM_YEAR_MONTH_DAY
          // TODO: FunctionSignatureId.FN_DATE_FROM_TIMESTAMP
          );

  // todo: Some of operators defined here are later overridden in ZetaSQLPlannerImpl.
  // We should remove them from this table and add generic way to provide custom
  // implementation. (Ex.: timestamp_add)
  public static final ImmutableMap<String, SqlOperator> ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR =
      ImmutableMap.<String, SqlOperator>builder()
          // grouped window function
          .put("TUMBLE", SqlStdOperatorTable.TUMBLE)
          .put("HOP", SqlStdOperatorTable.HOP)
          .put("SESSION", SqlStdOperatorTable.SESSION)

          // built-in logical operator
          .put("$and", SqlStdOperatorTable.AND)
          .put("$or", SqlStdOperatorTable.OR)
          .put("$not", SqlStdOperatorTable.NOT)

          // built-in comparison operator
          .put("$equal", SqlStdOperatorTable.EQUALS)
          .put("$not_equal", SqlStdOperatorTable.NOT_EQUALS)
          .put("$greater", SqlStdOperatorTable.GREATER_THAN)
          .put("$greater_or_equal", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
          .put("$less", SqlStdOperatorTable.LESS_THAN)
          .put("$less_or_equal", SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
          .put("$like", SqlOperators.LIKE)
          // .put("$in", SqlStdOperatorTable.IN)
          // .put("$between", SqlStdOperatorTable.BETWEEN)
          .put("$is_null", SqlStdOperatorTable.IS_NULL)
          .put("$is_true", SqlStdOperatorTable.IS_TRUE)
          .put("$is_false", SqlStdOperatorTable.IS_FALSE)

          // +, -, *, /
          .put("$add", SqlStdOperatorTable.PLUS)
          .put("$subtract", SqlStdOperatorTable.MINUS)
          .put("$multiply", SqlStdOperatorTable.MULTIPLY)
          .put("$unary_minus", SqlStdOperatorTable.UNARY_MINUS)
          .put("$divide", SqlStdOperatorTable.DIVIDE)

          // built-in string function
          .put("concat", SqlOperators.CONCAT)
          // .put("lower", SqlStdOperatorTable.LOWER)
          // .put("upper", SqlStdOperatorTable.UPPER)
          .put("substr", SqlOperators.SUBSTR)
          .put("trim", SqlOperators.TRIM)
          .put("replace", SqlOperators.REPLACE)
          .put("char_length", SqlOperators.CHAR_LENGTH)

          // string function UDFs
          // .put("strpos", )
          // .put("length", )
          // tells Calcite codegen that starts_with function is a udf.
          .put("starts_with", SqlOperators.START_WITHS)
          .put("ends_with", SqlOperators.ENDS_WITH)
          .put("ltrim", SqlOperators.LTRIM)
          .put("rtrim", SqlOperators.RTRIM)
          // .put("regexp_match",)
          // .put("regexp_extract",)
          // .put("regexp_replace",)
          // .put("regexp_extract_all",)
          // .put("byte_length",)
          // .put("format",)
          // .put("split",)
          // .put("regexp_contains", )
          // .put("normalize",)
          // .put("to_base32",)
          // .put("to_base64",)
          // .put("to_hex",)
          // .put("from_base64",)
          // .put("from_base32",)
          // .put("from_hex",)
          // .put("to_code_points")
          // .put("code_points_to_string")
          // .put("lpad", )
          // .put("rpad", )
          // .put("repeat", )
          .put("reverse", SqlOperators.REVERSE)

          // built-in aggregate function
          .put("$count_star", SqlStdOperatorTable.COUNT)
          // TODO: add support to all aggregate functions.
          .put("max", SqlStdOperatorTable.MAX)
          .put("min", SqlStdOperatorTable.MIN)
          .put("avg", SqlStdOperatorTable.AVG)
          .put("sum", SqlStdOperatorTable.SUM)
          // .put("any_value", SqlStdOperatorTable.ANY_VALUE)
          .put("count", SqlStdOperatorTable.COUNT)

          // aggregate UDF
          // .put("array_agg", )
          // .put("array_concat_agg")
          // .put("string_agg")
          // .put("bit_and")
          // .put("bit_or")
          // .put("bit_xor")
          // .put("logical_and")
          // .put("logical_or")

          // built-in statistical aggregate function
          // .put("covar_pop", SqlStdOperatorTable.COVAR_POP)
          // .put("covar_samp", SqlStdOperatorTable.COVAR_SAMP)
          // .put("stddev_pop", SqlStdOperatorTable.STDDEV_POP)
          // .put("stddev_samp", SqlStdOperatorTable.STDDEV_SAMP)
          // .put("var_pop", SqlStdOperatorTable.VAR_POP)
          // .put("var_samp", SqlStdOperatorTable.VAR_SAMP)

          // statistical aggregate UDF
          // .put("corr", )

          // built-in approximate aggregate function
          // .put("approx_count_distinct", SqlStdOperatorTable.APPROX_COUNT_DISTINCT)

          // approximate aggregate UDF
          // .put("approx_quantiles", )
          // .put("approx_top_sum")

          // HLL++ UDF
          // hll_count.merge
          // hll_count.extract
          // hll_count.init
          // hll_count.merge_partial

          // CAST
          // CAST operator does not go through lookup table.
          // .put("cast", SqlStdOperatorTable.CAST)

          // built-in math functions
          // .put("math", SqlStdOperatorTable.ABS)
          // .put("sign", SqlStdOperatorTable.SIGN)
          // .put("round", SqlStdOperatorTable.ROUND)
          .put("ceil", SqlStdOperatorTable.CEIL)
          .put("floor", SqlStdOperatorTable.FLOOR)
          .put("mod", SqlStdOperatorTable.MOD)
          // .put("sqrt", SqlStdOperatorTable.SQRT)
          // .put("exp", SqlStdOperatorTable.EXP)
          // .put("ln and log", SqlStdOperatorTable.LN)
          // .put("log10", SqlStdOperatorTable.LOG10)
          // .put("cos", SqlStdOperatorTable.COS)
          // .put("acos", SqlStdOperatorTable.ACOS)
          // .put("sin", SqlStdOperatorTable.SIN)
          // .put("asin", SqlStdOperatorTable.ASIN)
          // .put("tan", SqlStdOperatorTable.TAN)
          // .put("atan", SqlStdOperatorTable.ATAN)
          // .put("atan2", SqlStdOperatorTable.ATAN2)
          // .put("abs", SqlStdOperatorTable.ABS)
          // .put("pow", SqlStdOperatorTable.POWER)
          // .put("div", SqlStdOperatorTable.DIVIDE)
          // .put("trunc", SqlStdOperatorTable.TRUNCATE)

          // math UDF
          // .put("is_inf",)
          // .put("is_nan",)
          // .put("ieee_divide")
          // .put("safe_add")
          // .put("safe_divide")
          // .put("safe_subtract")
          // .put("safe_multiply")
          // .put("safe_negate")
          // .put("greatest")
          // .put("least")
          // .put("log")
          // .put("cosh")
          // .put("acosh")
          // .put("sinh")
          // .put("asinh")
          // .put("tanh")
          // .put("atanh")

          // Analytic functions
          // .put("dense_rank", SqlStdOperatorTable.DENSE_RANK)
          // .put("rank", SqlStdOperatorTable.RANK)
          // .put("row_number", SqlStdOperatorTable.ROW_NUMBER)
          // .put("percent_rank", SqlStdOperatorTable.PERCENT_RANK)
          // .put("cume_dist", SqlStdOperatorTable.CUME_DIST)
          // .put("ntile", SqlStdOperatorTable.NTILE)
          // .put("lead", SqlStdOperatorTable.LEAD)
          // .put("lag", SqlStdOperatorTable.LAG)
          // .put("first_value", SqlStdOperatorTable.FIRST_VALUE)
          // .put("last_value", SqlStdOperatorTable.LAST_VALUE)
          // .put("nth_value", SqlStdOperatorTable.NTH_VALUE)

          // .put("percentile_cont", )
          // .put("percentile_disc",)

          // misc functions
          // .put("fingerprint")
          // .put("fingerprint2011")

          // hash functions
          // .put("md5")
          // .put("sha1")
          // .put("sha256")
          // .put("sha512")

          // date functions
          // .put("date_add", SqlStdOperatorTable.DATETIME_PLUS)
          // .put("date_sub", SqlStdOperatorTable.MINUS_DATE)
          .put("date", SqlOperators.DATE_OP)

          // time functions
          // .put("time_add", SqlStdOperatorTable.DATETIME_PLUS)
          // .put("time_sub", SqlStdOperatorTable.MINUS_DATE)

          // timestamp functions
          .put(
              "timestamp_add",
              SqlStdOperatorTable.DATETIME_PLUS) // overridden in ZetaSQLPlannerImpl
          // .put("timestamp_sub", SqlStdOperatorTable.MINUS_DATE)
          .put("$extract", SqlStdOperatorTable.EXTRACT)
          .put("timestamp", SqlOperators.TIMESTAMP_OP)

          // other functions
          // .put("session_user", SqlStdOperatorTable.SESSION_USER)
          // .put("bit_cast_to_int32")
          // .put("bit_cast_to_int64")
          // .put("bit_cast_to_uint32")
          // .put("bit_cast_to_uint64")
          // .put("countif", )

          // case operator
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

  // argument one and two should compose of a interval.
  public static final ImmutableSet<String> FUNCTION_FAMILY_DATE_ADD =
      ImmutableSet.of(
          "date_add",
          "date_sub",
          "datetime_add",
          "datetime_sub",
          "time_add",
          "time_sub",
          "timestamp_add",
          "timestamp_sub");

  public static final ImmutableMap<String, SqlOperatorRewriter>
      ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR_REWRITER =
          ImmutableMap.<String, SqlOperatorRewriter>builder()
              .put("$case_with_value", new SqlCaseWithValueOperatorRewriter())
              .put("coalesce", new SqlCoalesceOperatorRewriter())
              .put("ifnull", new SqlIfNullOperatorRewriter())
              .put("nullif", new SqlNullIfOperatorRewriter())
              .put("$extract", new SqlExtractTimestampOperatorRewriter())
              .build();
}
