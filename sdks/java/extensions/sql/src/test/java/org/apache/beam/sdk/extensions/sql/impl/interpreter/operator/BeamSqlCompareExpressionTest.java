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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlCompareExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLikeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlNotEqualsExpression;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for the collections of {@link BeamSqlCompareExpression}. */
public class BeamSqlCompareExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testEqual() {
    BeamSqlEqualsExpression exp1 =
        new BeamSqlEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 100L)));
    Assert.assertEquals(
        false, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlEqualsExpression exp2 =
        new BeamSqlEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234567L)));
    Assert.assertEquals(
        true, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testLargerThan() {
    BeamSqlGreaterThanExpression exp1 =
        new BeamSqlGreaterThanExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234567L)));
    Assert.assertEquals(
        false, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlGreaterThanExpression exp2 =
        new BeamSqlGreaterThanExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234566L)));
    Assert.assertEquals(
        true, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testLargerThanEqual() {
    BeamSqlGreaterThanOrEqualsExpression exp1 =
        new BeamSqlGreaterThanOrEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234567L)));
    Assert.assertEquals(
        true, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlGreaterThanOrEqualsExpression exp2 =
        new BeamSqlGreaterThanOrEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234568L)));
    Assert.assertEquals(
        false, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testLessThan() {
    BeamSqlLessThanExpression exp1 =
        new BeamSqlLessThanExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.INTEGER, 1),
                BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1)));
    Assert.assertEquals(
        true, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlLessThanExpression exp2 =
        new BeamSqlLessThanExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.INTEGER, 1),
                BeamSqlPrimitive.of(SqlTypeName.INTEGER, -1)));
    Assert.assertEquals(
        false, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testLessThanEqual() {
    BeamSqlLessThanOrEqualsExpression exp1 =
        new BeamSqlLessThanOrEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.DOUBLE, 2),
                BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 8.9)));
    Assert.assertEquals(
        true, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlLessThanOrEqualsExpression exp2 =
        new BeamSqlLessThanOrEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.DOUBLE, 2),
                BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 8.0)));
    Assert.assertEquals(
        false, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testNotEqual() {
    BeamSqlNotEqualsExpression exp1 =
        new BeamSqlNotEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 3),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 1234567L)));
    Assert.assertEquals(
        false, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlNotEqualsExpression exp2 =
        new BeamSqlNotEqualsExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 3),
                BeamSqlPrimitive.of(SqlTypeName.BIGINT, 0L)));
    Assert.assertEquals(
        true, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }

  @Test
  public void testLike() {
    BeamSqlLikeExpression exp1 =
        new BeamSqlLikeExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.VARCHAR, 4),
                BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "This is an order_")));
    Assert.assertEquals(
        true, exp1.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());

    BeamSqlLikeExpression exp2 =
        new BeamSqlLikeExpression(
            Arrays.asList(
                new BeamSqlInputRefExpression(SqlTypeName.VARCHAR, 4),
                BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "This is not%")));
    Assert.assertEquals(
        false, exp2.evaluate(row, null, BeamSqlExpressionEnvironments.empty()).getValue());
  }
}
