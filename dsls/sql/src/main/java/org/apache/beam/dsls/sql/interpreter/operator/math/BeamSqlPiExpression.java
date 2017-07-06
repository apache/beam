package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * Base class for the PI function.
 */
public class BeamSqlPiExpression extends BeamSqlExpression {

  public BeamSqlPiExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.ANY);
  }

  @Override public boolean accept() {
    return numberOfOperands() == 0;
  }

  @Override public BeamSqlPrimitive evaluate(BeamSqlRow inputRecord) {
    return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, Math.PI);
  }
}
