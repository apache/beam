package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathBinaryExpression} for 'TRUNCATE' function.
 */
public class BeamSqlTruncateExpression extends BeamSqlMathBinaryExpression {

  public BeamSqlTruncateExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive<? extends Number> calculate(BeamSqlPrimitive leftOp,
      BeamSqlPrimitive rightOp) {
    BeamSqlPrimitive result = null;
    int rightIntOperand = SqlFunctions.toInt(rightOp.getValue());
    switch (leftOp.getOutputType()) {
      case SMALLINT:
      case TINYINT:
      case INTEGER:
        result = BeamSqlPrimitive.of(SqlTypeName.INTEGER,
            SqlFunctions.struncate(SqlFunctions.toInt(leftOp.getValue()), rightIntOperand));
        break;
      case BIGINT:
        result = BeamSqlPrimitive
            .of(SqlTypeName.BIGINT, SqlFunctions.struncate(leftOp.getLong(), rightIntOperand));
        break;
      case FLOAT:
      case DOUBLE:
        result = BeamSqlPrimitive.of(SqlTypeName.DOUBLE,
            SqlFunctions.struncate(SqlFunctions.toDouble(leftOp.getValue()), rightIntOperand));
        break;
      case DECIMAL:
        result = BeamSqlPrimitive.of(SqlTypeName.DECIMAL,
            SqlFunctions.struncate(leftOp.getBigDecimal(), rightIntOperand));
        break;
    }
    return result;
  }
}
