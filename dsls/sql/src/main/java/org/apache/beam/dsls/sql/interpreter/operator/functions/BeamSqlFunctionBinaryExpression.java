package org.apache.beam.dsls.sql.interpreter.operator.functions;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Base class for all binary functions.
 */
public abstract class BeamSqlFunctionBinaryExpression extends BeamSqlExpression {

  private BeamSqlFunctionBinaryExpression(List<BeamSqlExpression> operands,
      SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlFunctionBinaryExpression(List<BeamSqlExpression> operands) {
    this(operands, SqlTypeName.ANY);
  }

  @Override public boolean accept() {
    boolean acceptance = false;
    if (operands.size() != 2) {
      return false;
    }

    if (SqlTypeName.NUMERIC_TYPES.contains(operands.get(0).getOutputType())
        && SqlTypeName.NUMERIC_TYPES.contains(operands.get(1).getOutputType())) {
      acceptance = true;
    }
    return acceptance;
  }

  @Override public BeamSqlPrimitive<? extends Number> evaluate(BeamSQLRow inputRecord) {
    BeamSqlExpression leftOp = operands.get(0);
    BeamSqlExpression rightOp = operands.get(1);
    if (SqlTypeName.INT_TYPES.contains(leftOp.getOutputType()) && SqlTypeName.INT_TYPES
        .contains(rightOp.getOutputType())) {
      Long result = calculate(Long.valueOf(leftOp.evaluate(inputRecord).getValue().toString()),
          Long.valueOf(rightOp.evaluate(inputRecord).getValue().toString()));
      return BeamSqlPrimitive.of(SqlTypeName.BIGINT, result);
    } else {
      Double result = calculate(Double.valueOf(leftOp.evaluate(inputRecord).getValue().toString()),
          Double.valueOf(rightOp.evaluate(inputRecord).getValue().toString()));
      return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, result);
    }
  }

  /**
   * For the operands of type {@link SqlTypeName#INT_TYPES}
   * */
  public abstract Long calculate(Long leftOp, Long rightOp);

  /**
   * For the operands of other type {@link SqlTypeName#NUMERIC_TYPES}
   * */
  public abstract Double calculate(Number leftOp, Number rightOp);
}
