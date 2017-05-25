package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathUnaryExpression} for 'ABS' function.
 */
public class BeamSqlAbsExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlAbsExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override
  public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    switch (op.getOutputType()) {
      case INTEGER:
        return BeamSqlPrimitive.of(SqlTypeName.INTEGER, SqlFunctions.abs(op.getInteger()));
      case BIGINT:
        return BeamSqlPrimitive.of(SqlTypeName.BIGINT, SqlFunctions.abs(op.getLong()));
      case TINYINT:
        return BeamSqlPrimitive.of(SqlTypeName.TINYINT, SqlFunctions.abs(op.getByte()));
      case SMALLINT:
        return BeamSqlPrimitive.of(SqlTypeName.SMALLINT, SqlFunctions.abs(op.getShort()));
      case FLOAT:
        return BeamSqlPrimitive.of(SqlTypeName.FLOAT, SqlFunctions.abs(op.getFloat()));
      default:
        return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, SqlFunctions.abs(op.getDouble()));
    }
  }
}
