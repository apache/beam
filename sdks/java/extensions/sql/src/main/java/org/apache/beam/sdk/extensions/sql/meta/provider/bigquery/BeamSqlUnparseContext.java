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

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.text.translate.JavaUnicodeEscaper;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexDynamicParam;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlDynamicParam;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.BitString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamSqlUnparseContext extends SqlImplementor.SimpleContext {

  // More about escape sequences here:
  // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
  // No need to escape: \`, \?, \v, \a, \ooo, \xhh (since this in not a thing in Java)
  // TODO: Move away from deprecated classes.
  // TODO: Escaping single quotes, SqlCharStringLiteral (produced by SqlLiteral.createCharString)
  // introduces extra.
  private static final CharSequenceTranslator ESCAPE_FOR_ZETA_SQL =
      // ZetaSQL specific:
      new LookupTranslator(
              new String[][] {
                {"\"", "\\\""},
                {"\\", "\\\\"},
              })
          // \b, \n, \t, \f, \r
          .with(new LookupTranslator(EntityArrays.JAVA_CTRL_CHARS_ESCAPE()))
          // TODO(BEAM-9180): Add support for \Uhhhhhhhh
          // Unicode (only 4 hex digits)
          .with(JavaUnicodeEscaper.outsideOf(32, 0x7f));

  private Map<String, RelDataType> nullParams = new HashMap<>();

  public BeamSqlUnparseContext(IntFunction<SqlNode> field) {
    super(BeamBigQuerySqlDialect.DEFAULT, field);
  }

  public Map<String, RelDataType> getNullParams() {
    return nullParams;
  }

  @Override
  public SqlNode toSql(RexProgram program, RexNode rex) {
    if (rex.getKind().equals(SqlKind.LITERAL)) {
      final RexLiteral literal = (RexLiteral) rex;
      SqlTypeName name = literal.getTypeName();
      SqlTypeFamily family = name.getFamily();
      if (SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.equals(name)) {
        TimestampString timestampString = literal.getValueAs(TimestampString.class);
        return new SqlDateTimeLiteral(timestampString, POS);
      } else if (SqlTypeFamily.BINARY.equals(family)) {
        ByteString byteString = literal.getValueAs(ByteString.class);
        BitString bitString = BitString.createFromHexString(byteString.toString(16));
        return new SqlByteStringLiteral(bitString, POS);
      } else if (SqlTypeFamily.CHARACTER.equals(family)) {
        String escaped = ESCAPE_FOR_ZETA_SQL.translate(literal.getValueAs(String.class));
        return SqlLiteral.createCharString(escaped, POS);
      } else if (SqlTypeName.SYMBOL.equals(literal.getTypeName())) {
        Enum symbol = literal.getValueAs(Enum.class);
        if (TimeUnitRange.DOW.equals(symbol)) {
          return new ReplaceLiteral(literal, POS, "DAYOFWEEK");
        } else if (TimeUnitRange.DOY.equals(symbol)) {
          return new ReplaceLiteral(literal, POS, "DAYOFYEAR");
        } else if (TimeUnitRange.WEEK.equals(symbol)) {
          return new ReplaceLiteral(literal, POS, "ISOWEEK");
        }
      }
    } else if (rex.getKind().equals(SqlKind.DYNAMIC_PARAM)) {
      final RexDynamicParam param = (RexDynamicParam) rex;
      final int index = param.getIndex();
      final String name = "null_param_" + index;
      nullParams.put(name, param.getType());
      return new NamedDynamicParam(index, POS, name);
    }

    return super.toSql(program, rex);
  }

  private static class SqlDateTimeLiteral extends SqlLiteral {

    private final TimestampString timestampString;

    SqlDateTimeLiteral(TimestampString timestampString, SqlParserPos pos) {
      super(timestampString, SqlTypeName.TIMESTAMP, pos);
      this.timestampString = timestampString;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.literal("DATETIME '" + timestampString.toString() + "'");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      SqlDateTimeLiteral that = (SqlDateTimeLiteral) o;
      return Objects.equals(timestampString, that.timestampString);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), timestampString);
    }
  }

  private static class SqlByteStringLiteral extends SqlLiteral {

    SqlByteStringLiteral(BitString bytes, SqlParserPos pos) {
      super(bytes, SqlTypeName.BINARY, pos);
    }

    @Override
    public SqlByteStringLiteral clone(SqlParserPos pos) {
      return new SqlByteStringLiteral((BitString) this.value, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      assert this.value instanceof BitString;

      StringBuilder builder = new StringBuilder("B'");
      for (byte b : ((BitString) this.value).getAsByteArray()) {
        builder.append(String.format("\\x%02X", b));
      }
      builder.append("'");

      writer.literal(builder.toString());
    }
  }

  private static class ReplaceLiteral extends SqlLiteral {

    private final String newValue;

    ReplaceLiteral(RexLiteral literal, SqlParserPos pos, String newValue) {
      super(literal.getValue(), literal.getTypeName(), pos);
      this.newValue = newValue;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.literal(newValue);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof ReplaceLiteral)) {
        return false;
      }
      if (!newValue.equals(((ReplaceLiteral) obj).newValue)) {
        return false;
      }
      return super.equals(obj);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  private static class NamedDynamicParam extends SqlDynamicParam {
    private final String newName;

    NamedDynamicParam(int index, SqlParserPos pos, String newName) {
      super(index, pos);
      this.newName = newName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.literal("@" + newName);
    }
  }
}
