package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * {@code BeamSqlMathUnaryExpression} for 'SQRT' function.
 */
public class BeamSqlSqrtExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlSqrtExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, Math.sqrt(op.getDouble()));
  }
}
