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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.TimeUnit;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.NullCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlSetOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.BasicSqlType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

// TODO(CALCITE-3381): some methods below can be deleted after updating vendor Calcite version.
// Calcite v1_20_0 does not have type translation implemented, but later (unreleased) versions do.
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

  // List of BigQuery Specific Operators needed to form Syntactically Correct SQL
  private static final SqlOperator UNION_DISTINCT =
      new SqlSetOperator("UNION DISTINCT", SqlKind.UNION, 14, false);
  private static final SqlSetOperator EXCEPT_DISTINCT =
      new SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false);
  private static final SqlSetOperator INTERSECT_DISTINCT =
      new SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false);

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
  public static final String DOUBLE_POSITIVE_INF_FUNCTION = "double_positive_inf";
  public static final String DOUBLE_NEGATIVE_INF_FUNCTION = "double_negative_inf";
  public static final String DOUBLE_NAN_FUNCTION = "double_nan";
  private static final Map<String, String> DOUBLE_FUNCTIONS =
      ImmutableMap.<String, String>builder()
          .put(DOUBLE_POSITIVE_INF_FUNCTION, "CAST('+inf' AS FLOAT64)")
          .put(DOUBLE_NEGATIVE_INF_FUNCTION, "CAST('-inf' AS FLOAT64)")
          .put(DOUBLE_NAN_FUNCTION, "CAST('NaN' AS FLOAT64)")
          .build();
  public static final String NUMERIC_LITERAL_FUNCTION = "numeric_literal";
  public static final String DATETIME_LITERAL_FUNCTION = "datetime_literal";

  public BeamBigQuerySqlDialect(Context context) {
    super(context);
  }

  @Override
  public String quoteIdentifier(String val) {
    return quoteIdentifier(new StringBuilder(), val).toString();
  }

  @Override
  public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override
  public boolean supportsNestedAggregations() {
    return false;
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override
  public void unparseCall(
      final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
      case POSITION:
        final SqlWriter.Frame frame = writer.startFunCall("STRPOS");
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        if (3 == call.operandCount()) {
          throw new UnsupportedOperationException(
              "3rd operand Not Supported for Function STRPOS in Big Query");
        }
        writer.endFunCall(frame);
        break;
      case ROW:
        final SqlWriter.Frame structFrame = writer.startFunCall("STRUCT");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endFunCall(structFrame);
        break;
      case UNION:
        if (!((SqlSetOperator) call.getOperator()).isAll()) {
          SqlSyntax.BINARY.unparse(writer, UNION_DISTINCT, call, leftPrec, rightPrec);
        }
        break;
      case EXCEPT:
        if (!((SqlSetOperator) call.getOperator()).isAll()) {
          SqlSyntax.BINARY.unparse(writer, EXCEPT_DISTINCT, call, leftPrec, rightPrec);
        }
        break;
      case INTERSECT:
        if (!((SqlSetOperator) call.getOperator()).isAll()) {
          SqlSyntax.BINARY.unparse(writer, INTERSECT_DISTINCT, call, leftPrec, rightPrec);
        }
        break;
      case TRIM:
        unparseTrim(writer, call, leftPrec, rightPrec);
        break;
      case OTHER_FUNCTION:
        String funName = call.getOperator().getName();
        if (DATETIME_LITERAL_FUNCTION.equals(funName)) {
          // self-designed function dealing with the unparsing of ZetaSQL DATETIME literal, to
          // differentiate it from ZetaSQL TIMESTAMP literal
          unparseDateTimeLiteralWrapperFunction(writer, call, leftPrec, rightPrec);
          break;
        } else if (DOUBLE_FUNCTIONS.containsKey(funName)) {
          // self-designed function dealing with the unparsing of ZetaSQL DOUBLE positive
          // infinity, negative infinity and NaN
          unparseDoubleWrapperFunction(writer, funName);
          break;
        } else if (NUMERIC_LITERAL_FUNCTION.equals(funName)) {
          // self-designed function dealing with the unparsing of ZetaSQL NUMERIC literal
          unparseNumericLiteralWrapperFunction(writer, call, leftPrec, rightPrec);
          break;
        } else if (FUNCTIONS_USING_INTERVAL.contains(funName)) {
          unparseFunctionsUsingInterval(writer, call, leftPrec, rightPrec);
          break;
        } else if (EXTRACT_FUNCTIONS.containsKey(funName)) {
          unparseExtractFunctions(writer, call, leftPrec, rightPrec);
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

  @Override
  public void unparseSqlIntervalQualifier(
      SqlWriter writer, SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.endUnit == null) {
      writer.keyword(start);
    } else {
      throw new UnsupportedOperationException("Range time unit is not supported for BigQuery.");
    }
  }

  /**
   * For usage of TRIM, LTRIM and RTRIM in BQ see <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#trim">
   * BQ Trim Function</a>.
   */
  private void unparseTrim(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    SqlLiteral valueToTrim = call.operand(1);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
      case LEADING:
        operatorName = "LTRIM";
        break;
      case TRAILING:
        operatorName = "RTRIM";
        break;
      default:
        operatorName = call.getOperator().getName();
        break;
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);

    /**
     * If the trimmed character is non space character then add it to the target sql. eg: TRIM(BOTH
     * 'A' from 'ABCD' Output Query: TRIM('ABC', 'A')
     */
    if (!valueToTrim.toValue().matches("\\s+")) {
      writer.literal(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(trimFrame);
  }

  private void unparseDateTimeLiteralWrapperFunction(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    writer.literal(call.operand(0).toString().replace("TIMESTAMP", "DATETIME"));
  }

  /**
   * As there is no direct ZetaSQL literal representation of NaN or infinity, we cast String "+inf",
   * "-inf" and "NaN" to FLOAT64 representing positive infinity, negative infinity and NaN.
   */
  private void unparseDoubleWrapperFunction(SqlWriter writer, String funName) {
    writer.literal(DOUBLE_FUNCTIONS.get(funName));
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

  private TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
      case MICROSECOND:
      case MILLISECOND:
      case SECOND:
      case MINUTE:
      case HOUR:
      case DAY:
      case WEEK:
      case MONTH:
      case QUARTER:
      case YEAR:
      case ISOYEAR:
        return timeUnit;
      default:
        throw new UnsupportedOperationException(
            "Time unit " + timeUnit + " is not supported for BigQuery.");
    }
  }

  /**
   * BigQuery data type reference: <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">Bigquery
   * Standard SQL Data Types</a>.
   */
  @Override
  public SqlNode getCastSpec(final RelDataType type) {
    if (type instanceof BasicSqlType) {
      switch (type.getSqlTypeName()) {
          // BigQuery only supports INT64 for integer types.
        case BIGINT:
        case INTEGER:
        case TINYINT:
        case SMALLINT:
          return typeFromName(type, "INT64");
          // BigQuery only supports FLOAT64(aka. Double) for floating point types.
        case FLOAT:
        case DOUBLE:
          return typeFromName(type, "FLOAT64");
        case DECIMAL:
          return typeFromName(type, "NUMERIC");
        case BOOLEAN:
          return typeFromName(type, "BOOL");
        case CHAR:
        case VARCHAR:
          return typeFromName(type, "STRING");
        case VARBINARY:
        case BINARY:
          return typeFromName(type, "BYTES");
        case DATE:
          return typeFromName(type, "DATE");
        case TIME:
          return typeFromName(type, "TIME");
        case TIMESTAMP:
          return typeFromName(type, "TIMESTAMP");
        default:
          break;
      }
    }
    return super.getCastSpec(type);
  }

  private static SqlNode typeFromName(RelDataType type, String name) {
    return new SqlDataTypeSpec(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        type.getPrecision(),
        -1,
        null,
        null,
        SqlParserPos.ZERO);
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
