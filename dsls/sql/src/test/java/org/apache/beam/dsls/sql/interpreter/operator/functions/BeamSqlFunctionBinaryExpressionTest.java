package org.apache.beam.dsls.sql.interpreter.operator.functions;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.interpreter.BeamSQLFnExecutorTestBase;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.interpreter.operator.math.BeamSqlPowerExpression;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for {@link BeamSqlFunctionBinaryExpression}.
 */
public class BeamSqlFunctionBinaryExpressionTest extends BeamSQLFnExecutorTestBase {

  @Test public void testForGreaterThanTwoOperands() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // operands more than 2 not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5));
    Assert.assertFalse(new BeamSqlPowerExpression(operands).accept());
  }

  @Test public void testForOperandsType() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // varchar operand not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    Assert.assertFalse(new BeamSqlPowerExpression(operands).accept());
  }

  @Test public void testPowerFunction() {
    // operands are of type long, int, tinyint
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.0));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 4.0));
    assertEquals(16.0,
        new BeamSqlPowerExpression(operands).evaluate(record).getValue());
    // operands are of type decimal, double, big decimal
    // power(integer,integer) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    assertEquals(4L, new BeamSqlPowerExpression(operands).evaluate(record).getValue());

    // power(integer,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 3L));
    assertEquals(8L, new BeamSqlPowerExpression(operands).evaluate(record).getValue());

    // power(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    assertEquals(4L, new BeamSqlPowerExpression(operands).evaluate(record).getValue());

    // power(float, long) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 1.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(1.1, new BeamSqlPowerExpression(operands).evaluate(record).getValue());

    // power(double, long) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(1.1, new BeamSqlPowerExpression(operands).evaluate(record).getValue());

    // operand with a BeamSqlInputRefExpression
    operands.clear();
    BeamSqlInputRefExpression ref0 = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0);
    operands.add(ref0);
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));

    assertEquals(1524155677489L,
        new BeamSqlPowerExpression(operands).evaluate(record).getValue());
  }

}
