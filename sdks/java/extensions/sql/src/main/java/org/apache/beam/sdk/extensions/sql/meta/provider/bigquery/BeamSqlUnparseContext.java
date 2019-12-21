package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

import java.util.function.IntFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.BitString;

public class BeamSqlUnparseContext extends SqlImplementor.SimpleContext {

  public BeamSqlUnparseContext(SqlDialect dialect, IntFunction<SqlNode> field) {
    super(dialect, field);
  }

  @Override
  public SqlNode toSql(RexProgram program, RexNode rex) {
    if (rex.getKind().equals(SqlKind.LITERAL)) {
      final RexLiteral literal = (RexLiteral) rex;
      if (literal.getTypeName().getFamily().equals(SqlTypeFamily.BINARY)) {

        return new SqlByteStringLiteral(BitString.createFromBytes(literal.getValueAs(byte[].class)), POS);
      }
    }

    return super.toSql(program, rex);
  }

  private class SqlByteStringLiteral extends SqlLiteral {

    SqlByteStringLiteral(BitString bytes, SqlParserPos pos) {
      super(bytes, SqlTypeName.BINARY, pos);
    }

    @Override
    public SqlByteStringLiteral clone(SqlParserPos pos) {
      return new SqlByteStringLiteral((BitString)this.value, pos);
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
