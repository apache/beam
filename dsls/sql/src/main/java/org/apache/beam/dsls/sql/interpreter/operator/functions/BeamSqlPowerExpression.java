package org.apache.beam.dsls.sql.interpreter.operator.functions;

import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.calcite.runtime.SqlFunctions;


/**
 * {@code BeamSqlFunctionBinaryExpression} for 'POWER' function.
 */
public class BeamSqlPowerExpression extends BeamSqlFunctionBinaryExpression {

  public BeamSqlPowerExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public Long calculate(Long leftOp, Long rightOp) {
    return (long) SqlFunctions.power(leftOp, rightOp);
  }

  @Override public Double calculate(Number leftOp, Number rightOp) {
    return SqlFunctions.power(leftOp.doubleValue(), rightOp.doubleValue());
  }
}
