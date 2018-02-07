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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BeamSqlMathBinaryExpression}.
 */
public class BeamSqlMathBinaryExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test public void testForGreaterThanTwoOperands() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // operands more than 2 not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5));
    Assert.assertFalse(new BeamSqlRoundExpression(operands).accept());
  }

  @Test public void testForOneOperand() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // only one operand allowed in round function
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
  }

  @Test public void testForOperandsType() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // varchar operand not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    Assert.assertFalse(new BeamSqlRoundExpression(operands).accept());
  }

  @Test public void testRoundFunction() {
    // test round functions with operands of type bigint, int,
    // tinyint, smallint, double, decimal
    List<BeamSqlExpression> operands = new ArrayList<>();
    // round(double, double) => double
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.0));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 4.0));
    Assert.assertEquals(2.0,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());
    // round(integer,integer) => integer
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    Assert.assertEquals(2, new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // round(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 5L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 3L));
    Assert.assertEquals(5L, new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // round(short) => short
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, new Short("4")));
    Assert.assertEquals(SqlFunctions.toShort(4),
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // round(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    Assert.assertEquals(2L, new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // round(double, long) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    Assert.assertEquals(1.1,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.368768));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    Assert.assertEquals(2.37,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 3.78683686458));
    Assert.assertEquals(4.0,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 378.683686458));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, -2));
    Assert.assertEquals(400.0,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 378.683686458));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, -1));
    Assert.assertEquals(380.0,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // round(integer, double) => integer
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.2));
    Assert.assertEquals(2, new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());

    // operand with a BeamSqlInputRefExpression
    // to select a column value from row of a row
    operands.clear();
    BeamSqlInputRefExpression ref0 = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0);
    operands.add(ref0);
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));

    Assert.assertEquals(1234567L,
        new BeamSqlRoundExpression(operands).evaluate(row, null).getValue());
  }

  @Test public void testPowerFunction() {
    // test power functions with operands of type bigint, int,
    // tinyint, smallint, double, decimal
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.0));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 4.0));
    Assert.assertEquals(16.0,
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());
    // power(integer,integer) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    Assert.assertEquals(4.0,
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());
    // power(integer,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 3L));
    Assert.assertEquals(8.0
        , new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());

    // power(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    Assert.assertEquals(4.0,
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());

    // power(double, int) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    Assert.assertEquals(1.1,
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());

    // power(double, long) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    Assert.assertEquals(1.1,
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());

    // power(integer, double) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.2));
    Assert.assertEquals(Math.pow(2, 2.2),
        new BeamSqlPowerExpression(operands).evaluate(row, null).getValue());
  }

  @Test public void testForTruncate() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.0));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 4.0));
    Assert.assertEquals(2.0,
        new BeamSqlTruncateExpression(operands).evaluate(row, null).getValue());
    // truncate(double, integer) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.80685));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    Assert.assertEquals(2.8068,
        new BeamSqlTruncateExpression(operands).evaluate(row, null).getValue());
  }

  @Test public void testForAtan2() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.875));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 0.56));
    Assert.assertEquals(Math.atan2(0.875, 0.56),
        new BeamSqlAtan2Expression(operands).evaluate(row, null).getValue());
  }

}
