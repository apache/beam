package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathUnaryExpression} for 'FLOOR' function.
 */
public class BeamSqlFloorExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlFloorExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    switch (getOutputType()) {
      case DECIMAL:
        return BeamSqlPrimitive.of(SqlTypeName.DECIMAL, SqlFunctions.floor(op.getBigDecimal()));
      default:
        return BeamSqlPrimitive
            .of(SqlTypeName.DOUBLE, SqlFunctions.floor(SqlFunctions.toDouble(op.getValue())));
    }
  }
}
