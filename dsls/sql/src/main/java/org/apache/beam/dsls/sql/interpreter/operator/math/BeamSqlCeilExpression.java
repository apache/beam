package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathUnaryExpression} for 'CEIL' function.
 */
public class BeamSqlCeilExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlCeilExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    switch (getOutputType()) {
      case DECIMAL:
        return BeamSqlPrimitive.of(SqlTypeName.DECIMAL, SqlFunctions.ceil(op.getBigDecimal()));
      default:
        return BeamSqlPrimitive
            .of(SqlTypeName.DOUBLE, SqlFunctions.ceil(SqlFunctions.toDouble(op.getValue())));
    }
  }
}
