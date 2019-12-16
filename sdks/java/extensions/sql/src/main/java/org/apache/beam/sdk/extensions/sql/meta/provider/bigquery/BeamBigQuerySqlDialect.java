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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.NullCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlSetOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.BasicSqlType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

// TODO(CALCITE-3381): BeamBigQuerySqlDialect can be deleted after updating vendor Calcite version.
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

  /**
   * An unquoted BigQuery identifier must start with a letter and be followed by zero or more
   * letters, digits or _.
   */
  private static final Pattern IDENTIFIER_REGEX = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  /** List of BigQuery Specific Operators needed to form Syntactically Correct SQL. */
  private static final SqlOperator UNION_DISTINCT =
      new SqlSetOperator("UNION DISTINCT", SqlKind.UNION, 14, false);

  private static final SqlSetOperator EXCEPT_DISTINCT =
      new SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false);
  private static final SqlSetOperator INTERSECT_DISTINCT =
      new SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false);

  private static final List<String> RESERVED_KEYWORDS =
      ImmutableList.copyOf(
          Arrays.asList(
              "ALL",
              "AND",
              "ANY",
              "ARRAY",
              "AS",
              "ASC",
              "ASSERT_ROWS_MODIFIED",
              "AT",
              "BETWEEN",
              "BY",
              "CASE",
              "CAST",
              "COLLATE",
              "CONTAINS",
              "CREATE",
              "CROSS",
              "CUBE",
              "CURRENT",
              "DEFAULT",
              "DEFINE",
              "DESC",
              "DISTINCT",
              "ELSE",
              "END",
              "ENUM",
              "ESCAPE",
              "EXCEPT",
              "EXCLUDE",
              "EXISTS",
              "EXTRACT",
              "FALSE",
              "FETCH",
              "FOLLOWING",
              "FOR",
              "FROM",
              "FULL",
              "GROUP",
              "GROUPING",
              "GROUPS",
              "HASH",
              "HAVING",
              "IF",
              "IGNORE",
              "IN",
              "INNER",
              "INTERSECT",
              "INTERVAL",
              "INTO",
              "IS",
              "JOIN",
              "LATERAL",
              "LEFT",
              "LIKE",
              "LIMIT",
              "LOOKUP",
              "MERGE",
              "NATURAL",
              "NEW",
              "NO",
              "NOT",
              "NULL",
              "NULLS",
              "OF",
              "ON",
              "OR",
              "ORDER",
              "OUTER",
              "OVER",
              "PARTITION",
              "PRECEDING",
              "PROTO",
              "RANGE",
              "RECURSIVE",
              "RESPECT",
              "RIGHT",
              "ROLLUP",
              "ROWS",
              "SELECT",
              "SET",
              "SOME",
              "STRUCT",
              "TABLESAMPLE",
              "THEN",
              "TO",
              "TREAT",
              "TRUE",
              "UNBOUNDED",
              "UNION",
              "UNNEST",
              "USING",
              "WHEN",
              "WHERE",
              "WINDOW",
              "WITH",
              "WITHIN"));

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
          throw new RuntimeException("3rd operand Not Supported for Function STRPOS in Big Query");
        }
        writer.endFunCall(frame);
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
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
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
}
