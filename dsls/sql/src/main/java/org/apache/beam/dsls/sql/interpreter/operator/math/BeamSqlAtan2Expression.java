package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@link BeamSqlMathBinaryExpression} for 'ATAN2' function.
 */
public class BeamSqlAtan2Expression extends BeamSqlMathBinaryExpression {

  public BeamSqlAtan2Expression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive<? extends Number> calculate(BeamSqlPrimitive leftOp,
      BeamSqlPrimitive rightOp) {
    return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, SqlFunctions
        .atan2(SqlFunctions.toDouble(leftOp.getValue()),
            SqlFunctions.toDouble(rightOp.getValue())));
  }
}
