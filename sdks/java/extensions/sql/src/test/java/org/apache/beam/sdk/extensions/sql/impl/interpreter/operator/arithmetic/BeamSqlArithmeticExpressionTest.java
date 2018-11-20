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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Tests for {@code BeamSqlArithmeticExpression}. */
public class BeamSqlArithmeticExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testAccept_normal() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // byte, short
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TINYINT, Byte.valueOf("1")));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.MAX_VALUE));
    assertTrue(new BeamSqlPlusExpression(operands).accept());

    // integer, long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertTrue(new BeamSqlPlusExpression(operands).accept());

    // float, double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 1.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    assertTrue(new BeamSqlPlusExpression(operands).accept());

    // varchar
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 1.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "1"));
    assertFalse(new BeamSqlPlusExpression(operands).accept());
  }

  @Test
  public void testAccept_exception() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // more than 2 operands
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TINYINT, Byte.valueOf("1")));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.MAX_VALUE));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.MAX_VALUE));
    assertFalse(new BeamSqlPlusExpression(operands).accept());

    // boolean
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TINYINT, Byte.valueOf("1")));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true));
    assertFalse(new BeamSqlPlusExpression(operands).accept());
  }

  @Test
  public void testPlus() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // integer + integer => integer
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    assertEquals(
        2,
        new BeamSqlPlusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // integer + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlPlusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // long + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlPlusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // float + long => float
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 1.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        1.1F + 1,
        new BeamSqlPlusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // double + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 1.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1,
        new BeamSqlPlusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testMinus() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // integer + integer => long
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    assertEquals(
        1,
        new BeamSqlMinusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // integer + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        1L,
        new BeamSqlMinusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // long + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        1L,
        new BeamSqlMinusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // float + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 2.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1F - 1L,
        new BeamSqlMinusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue()
            .floatValue(),
        0.1);

    // double + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        1.1,
        new BeamSqlMinusExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testMultiply() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // integer + integer => integer
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    assertEquals(
        2,
        new BeamSqlMultiplyExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // integer + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlMultiplyExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // long + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlMultiplyExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // float + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 2.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1F * 1L,
        new BeamSqlMultiplyExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // double + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1,
        new BeamSqlMultiplyExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testDivide() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // integer + integer => integer
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    assertEquals(
        2,
        new BeamSqlDivideExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // integer + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlDivideExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // long + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2L,
        new BeamSqlDivideExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // float + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.FLOAT, 2.1F));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1F / 1,
        new BeamSqlDivideExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // double + long => double
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 2.1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1L));
    assertEquals(
        2.1,
        new BeamSqlDivideExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testMod() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // integer + integer => long
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    assertEquals(
        1,
        new BeamSqlModExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // integer + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    assertEquals(
        1L,
        new BeamSqlModExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    // long + long => long
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 3L));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, 2L));
    assertEquals(
        1L,
        new BeamSqlModExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }
}
