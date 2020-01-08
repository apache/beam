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

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

import java.util.function.IntFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.BitString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.commons.lang.StringEscapeUtils;

public class BeamSqlUnparseContext extends SqlImplementor.SimpleContext {

  public BeamSqlUnparseContext(IntFunction<SqlNode> field) {
    super(BeamBigQuerySqlDialect.DEFAULT, field);
  }

  @Override
  public SqlNode toSql(RexProgram program, RexNode rex) {
    if (rex.getKind().equals(SqlKind.LITERAL)) {
      final RexLiteral literal = (RexLiteral) rex;
      SqlTypeFamily family = literal.getTypeName().getFamily();
      if (SqlTypeFamily.BINARY.equals(family)) {
        ByteString byteString = literal.getValueAs(ByteString.class);
        BitString bitString = BitString.createFromHexString(byteString.toString(16));
        return new SqlByteStringLiteral(bitString, POS);
      } else if (SqlTypeFamily.CHARACTER.equals(family)) {
        String escaped = StringEscapeUtils.escapeJava(literal.getValueAs(String.class));
        return SqlLiteral.createCharString(escaped, POS);
      }
    }

    return super.toSql(program, rex);
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
}
