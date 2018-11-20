/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link BeamSqlMathUnaryExpression}. */
public class BeamSqlMathUnaryExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testForGreaterThanOneOperands() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // operands more than 1 not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    Assert.assertFalse(new BeamSqlAbsExpression(operands).accept());
  }

  @Test
  public void testForOperandsType() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // varchar operand not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2"));
    Assert.assertFalse(new BeamSqlAbsExpression(operands).accept());
  }

  @Test
  public void testForUnaryExpressions() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for sqrt function
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));

    // test for abs function
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, -28965734597L));
    Assert.assertEquals(
        28965734597L,
        new BeamSqlAbsExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForLnExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for LN function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.log(2),
        new BeamSqlLnExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // test for LN function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.log(2.4),
        new BeamSqlLnExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for LN function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.log(2.56),
        new BeamSqlLnExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForLog10Expression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for log10 function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.log10(2),
        new BeamSqlLogExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for log10 function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.log10(2.4),
        new BeamSqlLogExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for log10 function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.log10(2.56),
        new BeamSqlLogExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForExpExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.exp(2),
        new BeamSqlExpExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.exp(2.4),
        new BeamSqlExpExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.exp(2.56),
        new BeamSqlExpExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForAcosExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Double.NaN,
        new BeamSqlAcosExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.45));
    Assert.assertEquals(
        Math.acos(0.45),
        new BeamSqlAcosExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(-0.367)));
    Assert.assertEquals(
        Math.acos(-0.367),
        new BeamSqlAcosExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForAsinExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type double
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.45));
    Assert.assertEquals(
        Math.asin(0.45),
        new BeamSqlAsinExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(-0.367)));
    Assert.assertEquals(
        Math.asin(-0.367),
        new BeamSqlAsinExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForAtanExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type double
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.45));
    Assert.assertEquals(
        Math.atan(0.45),
        new BeamSqlAtanExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(-0.367)));
    Assert.assertEquals(
        Math.atan(-0.367),
        new BeamSqlAtanExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForCosExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type double
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.45));
    Assert.assertEquals(
        Math.cos(0.45),
        new BeamSqlCosExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(-0.367)));
    Assert.assertEquals(
        Math.cos(-0.367),
        new BeamSqlCosExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForCotExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type double
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, .45));
    Assert.assertEquals(
        1.0d / Math.tan(0.45),
        new BeamSqlCotExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(-.367)));
    Assert.assertEquals(
        1.0d / Math.tan(-0.367),
        new BeamSqlCotExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForDegreesExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.toDegrees(2),
        new BeamSqlDegreesExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.toDegrees(2.4),
        new BeamSqlDegreesExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.toDegrees(2.56),
        new BeamSqlDegreesExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForRadiansExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.toRadians(2),
        new BeamSqlRadiansExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.toRadians(2.4),
        new BeamSqlRadiansExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.toRadians(2.56),
        new BeamSqlRadiansExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForSinExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.sin(2),
        new BeamSqlSinExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.sin(2.4),
        new BeamSqlSinExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.sin(2.56),
        new BeamSqlSinExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForTanExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        Math.tan(2),
        new BeamSqlTanExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        Math.tan(2.4),
        new BeamSqlTanExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        Math.tan(2.56),
        new BeamSqlTanExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForSignExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for exp function with operand type smallint
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(
        (short) 1,
        new BeamSqlSignExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.4));
    Assert.assertEquals(
        1.0,
        new BeamSqlSignExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
    // test for exp function with operand type decimal
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DECIMAL, BigDecimal.valueOf(2.56)));
    Assert.assertEquals(
        BigDecimal.ONE,
        new BeamSqlSignExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForPi() {
    Assert.assertEquals(
        Math.PI,
        new BeamSqlPiExpression()
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForCeil() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.68687979));
    Assert.assertEquals(
        Math.ceil(2.68687979),
        new BeamSqlCeilExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testForFloor() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.68687979));
    Assert.assertEquals(
        Math.floor(2.68687979),
        new BeamSqlFloorExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }
}
