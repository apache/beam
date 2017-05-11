package org.apache.beam.dsls.sql.interpreter.operator.functions;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Base class for all unary functions.
 */
public abstract class BeamSqlFunctionUnaryExpression extends BeamSqlExpression {

  public BeamSqlFunctionUnaryExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.ANY);
  }

  @Override public boolean accept() {
    boolean acceptance = false;
    if (operands.size() != 1) {
      return acceptance;
    }

    if (SqlTypeName.NUMERIC_TYPES.contains(operands.get(0).getOutputType())) {
      acceptance = true;
    }
    return acceptance;
  }

  /**
   * For the operands of type {@link SqlTypeName#INT_TYPES}
   * */
  public abstract Long calculate(Long leftOp, Long rightOp);

  /**
   * For the operands of other type {@link SqlTypeName#NUMERIC_TYPES}
   * */
  public abstract Double calculate(Double leftOp, Double rightOp);
}
