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

package org.apache.beam.dsls.sql.interpreter.operator.math;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.dsls.sql.interpreter.BeamSQLFnExecutorTestBase;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BeamSqlMathBinaryExpression}.
 */
public class BeamSqlMathBinaryExpressionTest extends BeamSQLFnExecutorTestBase {

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
    // operands are of type long, int, tinyint
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.0));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 4.0));
    assertEquals(2.0, new BeamSqlRoundExpression(operands).evaluate(record).getValue());
    // operands are of type decimal, double, big decimal
    // round(integer,integer) => integer
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    assertEquals(2, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // round(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 5L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 3L));
    assertEquals(5L, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // round(short) => short
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, new Short("4")));
    assertEquals(SqlFunctions.toShort(4),
        new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // round(long,long) => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    assertEquals(2L, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // round(double, long) => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(1.1, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.368768));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    assertEquals(2.37, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 3.78683686458));
    assertEquals(4.0, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    //378.683686458
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 378.683686458));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, -2));
    assertEquals(400.0, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 378.683686458));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, -1));
    assertEquals(380.0, new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // round(integer, double) => integer
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.2));
    assertEquals(2,
        new BeamSqlRoundExpression(operands).evaluate(record).getValue());

    // operand with a BeamSqlInputRefExpression
    operands.clear();
    BeamSqlInputRefExpression ref0 = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0);
    operands.add(ref0);
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));

    assertEquals(1234567L, new BeamSqlRoundExpression(operands).evaluate(record).getValue());
  }

}
