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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.config.NullCollation;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

// TODO(CALCITE-3381): some methods below can be deleted after updating vendor Calcite version.
// Calcite v1_20_0 does not have type translation implemented, but later (unreleased) versions do.
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamBigQuerySqlDialect extends BigQuerySqlDialect {

  public static final SqlDialect.Context DEFAULT_CONTEXT =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
          .withIdentifierQuoteString("`")
          .withNullCollation(NullCollation.LOW)
          .withUnquotedCasing(Casing.UNCHANGED)
          .withQuotedCasing(Casing.UNCHANGED)
          .withCaseSensitive(false);

  public static final SqlDialect DEFAULT = new BeamBigQuerySqlDialect(DEFAULT_CONTEXT);

  // ZetaSQL defined functions that need special unparsing
  private static final List<String> FUNCTIONS_USING_INTERVAL =
      ImmutableList.of(
          "date_add",
          "date_sub",
          "datetime_add",
          "datetime_sub",
          "time_add",
          "time_sub",
          "timestamp_add",
          "timestamp_sub");
  private static final Map<String, String> EXTRACT_FUNCTIONS =
      ImmutableMap.<String, String>builder()
          .put("$extract", "")
          .put("$extract_date", "DATE")
          .put("$extract_time", "TIME")
          .put("$extract_datetime", "DATETIME")
          .build();
  public static final String DOUBLE_POSITIVE_INF_WRAPPER = "double_positive_inf";
  public static final String DOUBLE_NEGATIVE_INF_WRAPPER = "double_negative_inf";
  public static final String DOUBLE_NAN_WRAPPER = "double_nan";
  // ZetaSQL has no literal representation of NaN and infinity, so we need to CAST from strings
  private static final Map<String, String> DOUBLE_LITERAL_WRAPPERS =
      ImmutableMap.<String, String>builder()
          .put(DOUBLE_POSITIVE_INF_WRAPPER, "CAST('+inf' AS FLOAT64)")
          .put(DOUBLE_NEGATIVE_INF_WRAPPER, "CAST('-inf' AS FLOAT64)")
          .put(DOUBLE_NAN_WRAPPER, "CAST('NaN' AS FLOAT64)")
          .build();
  public static final String NUMERIC_LITERAL_WRAPPER = "numeric_literal";
  public static final String IN_ARRAY_OPERATOR = "$in_array";

  public BeamBigQuerySqlDialect(Context context) {
    super(context);
  }

  @Override
  public String quoteIdentifier(String val) {
    return quoteIdentifier(new StringBuilder(), val).toString();
  }

  @Override
  public void unparseCall(
      final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
      case ROW:
        final SqlWriter.Frame structFrame = writer.startFunCall("STRUCT");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endFunCall(structFrame);
        break;
      case OTHER_FUNCTION:
        String funName = call.getOperator().getName();
        if (DOUBLE_LITERAL_WRAPPERS.containsKey(funName)) {
          // self-designed function dealing with the unparsing of ZetaSQL DOUBLE positive
          // infinity, negative infinity and NaN
          unparseDoubleLiteralWrapperFunction(writer, funName);
          break;
        } else if (NUMERIC_LITERAL_WRAPPER.equals(funName)) {
          // self-designed function dealing with the unparsing of ZetaSQL NUMERIC literal
          unparseNumericLiteralWrapperFunction(writer, call, leftPrec, rightPrec);
          break;
        } else if (FUNCTIONS_USING_INTERVAL.contains(funName)) {
          unparseFunctionsUsingInterval(writer, call, leftPrec, rightPrec);
          break;
        } else if (EXTRACT_FUNCTIONS.containsKey(funName)) {
          unparseExtractFunctions(writer, call, leftPrec, rightPrec);
          break;
        } else if (IN_ARRAY_OPERATOR.equals(funName)) {
          unparseInArrayOperator(writer, call, leftPrec, rightPrec);
          break;
        } // fall through
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /** BigQuery interval syntax: INTERVAL int64 time_unit. */
  @Override
  public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    writer.keyword("INTERVAL");
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    Long intervalValueInLong;
    try {
      intervalValueInLong = Long.parseLong(literal.getValue().toString());
    } catch (NumberFormatException e) {
      throw new UnsupportedOperationException(
          "Only INT64 is supported as the interval value for BigQuery.");
    }
    writer.literal(intervalValueInLong.toString());
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(), RelDataTypeSystem.DEFAULT);
  }

  private void unparseDoubleLiteralWrapperFunction(SqlWriter writer, String funName) {
    writer.literal(DOUBLE_LITERAL_WRAPPERS.get(funName));
  }

  private void unparseNumericLiteralWrapperFunction(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    writer.literal("NUMERIC '");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.literal("'");
  }

  /**
   * For usage of INTERVAL, see <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#timestamp_add">
   * BQ TIMESTAMP_ADD function</a> for example.
   */
  private void unparseFunctionsUsingInterval(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    // e.g. TIMESTAMP_ADD syntax:
    // TIMESTAMP_ADD(timestamp_expression, INTERVAL int64_expression date_part)
    int operandCount = call.operandCount();
    if (operandCount == 2) {
      // operand0: timestamp_expression
      // operand1: SqlIntervalLiteral (INTERVAL int64_expression date_part)
      super.unparseCall(writer, call, leftPrec, rightPrec);
    } else if (operandCount == 3) {
      // operand0: timestamp_expression
      // operand1: int64_expression
      // operand2: date_part
      final SqlWriter.Frame frame = writer.startFunCall(call.getOperator().getName());
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.literal(",");
      writer.literal("INTERVAL");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      call.operand(2).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unable to unparse %s with %d operands.",
              call.getOperator().getName(), operandCount));
    }
  }

  private void unparseExtractFunctions(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String funName = call.getOperator().getName();
    int operandCount = call.operandCount();
    SqlNode tz = null;

    final SqlWriter.Frame frame = writer.startFunCall("EXTRACT");
    if (!funName.equals("$extract") && (operandCount == 1 || operandCount == 2)) {
      // EXTRACT(DATE/TIME/DATETIME FROM timestamp_expression [AT TIME ZONE tz])
      // operand0: timestamp_expression
      // operand1: tz (optional)
      writer.literal(EXTRACT_FUNCTIONS.get(funName));
      if (operandCount == 2) {
        tz = call.operand(1);
      }
    } else if (funName.equals("$extract") && (operandCount == 2 || operandCount == 3)) {
      // EXTRACT(date_part FROM timestamp_expression [AT TIME ZONE tz])
      // operand0: timestamp_expression
      // operand1: date_part
      // operand2: tz (optional)
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      if (operandCount == 3) {
        tz = call.operand(2);
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Unable to unparse %s with %d operands.", funName, operandCount));
    }
    writer.literal("FROM");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    if (tz != null) {
      writer.literal("AT TIME ZONE");
      tz.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  private void unparseInArrayOperator(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.literal("IN UNNEST(");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.literal(")");
  }

  @Override
  public void unparseDateTimeLiteral(
      SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    if (literal instanceof SqlTimestampLiteral) {
      // 'Z' stands for +00 timezone offset, which is guaranteed in TIMESTAMP literal
      writer.literal("TIMESTAMP '" + literal.toFormattedString() + "Z'");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }
}
