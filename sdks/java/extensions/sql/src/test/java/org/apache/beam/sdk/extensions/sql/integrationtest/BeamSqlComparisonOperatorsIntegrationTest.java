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

package org.apache.beam.sdk.extensions.sql.integrationtest;

import java.math.BigDecimal;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.junit.Test;

/**
 * Integration test for comparison operators.
 */
public class BeamSqlComparisonOperatorsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {

  @Test
  public void testEquals() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_1 = c_tinyint_1", true)
        .addExpr("c_tinyint_1 = c_tinyint_2", false)
        .addExpr("c_smallint_1 = c_smallint_1", true)
        .addExpr("c_smallint_1 = c_smallint_2", false)
        .addExpr("c_integer_1 = c_integer_1", true)
        .addExpr("c_integer_1 = c_integer_2", false)
        .addExpr("c_bigint_1 = c_bigint_1", true)
        .addExpr("c_bigint_1 = c_bigint_2", false)
        .addExpr("c_float_1 = c_float_1", true)
        .addExpr("c_float_1 = c_float_2", false)
        .addExpr("c_double_1 = c_double_1", true)
        .addExpr("c_double_1 = c_double_2", false)
        .addExpr("c_decimal_1 = c_decimal_1", true)
        .addExpr("c_decimal_1 = c_decimal_2", false)
        .addExpr("c_varchar_1 = c_varchar_1", true)
        .addExpr("c_varchar_1 = c_varchar_2", false)
        .addExpr("c_boolean_true = c_boolean_true", true)
        .addExpr("c_boolean_true = c_boolean_false", false)

        ;
    checker.buildRunAndCheck();
  }

  @Test
  public void testNotEquals() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_1 <> c_tinyint_1", false)
        .addExpr("c_tinyint_1 <> c_tinyint_2", true)
        .addExpr("c_smallint_1 <> c_smallint_1", false)
        .addExpr("c_smallint_1 <> c_smallint_2", true)
        .addExpr("c_integer_1 <> c_integer_1", false)
        .addExpr("c_integer_1 <> c_integer_2", true)
        .addExpr("c_bigint_1 <> c_bigint_1", false)
        .addExpr("c_bigint_1 <> c_bigint_2", true)
        .addExpr("c_float_1 <> c_float_1", false)
        .addExpr("c_float_1 <> c_float_2", true)
        .addExpr("c_double_1 <> c_double_1", false)
        .addExpr("c_double_1 <> c_double_2", true)
        .addExpr("c_decimal_1 <> c_decimal_1", false)
        .addExpr("c_decimal_1 <> c_decimal_2", true)
        .addExpr("c_varchar_1 <> c_varchar_1", false)
        .addExpr("c_varchar_1 <> c_varchar_2", true)
        .addExpr("c_boolean_true <> c_boolean_true", false)
        .addExpr("c_boolean_true <> c_boolean_false", true)
        ;
    checker.buildRunAndCheck();
  }

  @Test
  public void testGreaterThan() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_2 > c_tinyint_1", true)
        .addExpr("c_tinyint_1 > c_tinyint_1", false)
        .addExpr("c_tinyint_1 > c_tinyint_2", false)

        .addExpr("c_smallint_2 > c_smallint_1", true)
        .addExpr("c_smallint_1 > c_smallint_1", false)
        .addExpr("c_smallint_1 > c_smallint_2", false)

        .addExpr("c_integer_2 > c_integer_1", true)
        .addExpr("c_integer_1 > c_integer_1", false)
        .addExpr("c_integer_1 > c_integer_2", false)

        .addExpr("c_bigint_2 > c_bigint_1", true)
        .addExpr("c_bigint_1 > c_bigint_1", false)
        .addExpr("c_bigint_1 > c_bigint_2", false)

        .addExpr("c_float_2 > c_float_1", true)
        .addExpr("c_float_1 > c_float_1", false)
        .addExpr("c_float_1 > c_float_2", false)

        .addExpr("c_double_2 > c_double_1", true)
        .addExpr("c_double_1 > c_double_1", false)
        .addExpr("c_double_1 > c_double_2", false)

        .addExpr("c_decimal_2 > c_decimal_1", true)
        .addExpr("c_decimal_1 > c_decimal_1", false)
        .addExpr("c_decimal_1 > c_decimal_2", false)

        .addExpr("c_varchar_2 > c_varchar_1", true)
        .addExpr("c_varchar_1 > c_varchar_1", false)
        .addExpr("c_varchar_1 > c_varchar_2", false)
        ;

    checker.buildRunAndCheck();
  }

  @Test(expected = RuntimeException.class)
  public void testGreaterThanException() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_false > c_boolean_true", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testGreaterThanOrEquals() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_2 >= c_tinyint_1", true)
        .addExpr("c_tinyint_1 >= c_tinyint_1", true)
        .addExpr("c_tinyint_1 >= c_tinyint_2", false)

        .addExpr("c_smallint_2 >= c_smallint_1", true)
        .addExpr("c_smallint_1 >= c_smallint_1", true)
        .addExpr("c_smallint_1 >= c_smallint_2", false)

        .addExpr("c_integer_2 >= c_integer_1", true)
        .addExpr("c_integer_1 >= c_integer_1", true)
        .addExpr("c_integer_1 >= c_integer_2", false)

        .addExpr("c_bigint_2 >= c_bigint_1", true)
        .addExpr("c_bigint_1 >= c_bigint_1", true)
        .addExpr("c_bigint_1 >= c_bigint_2", false)

        .addExpr("c_float_2 >= c_float_1", true)
        .addExpr("c_float_1 >= c_float_1", true)
        .addExpr("c_float_1 >= c_float_2", false)

        .addExpr("c_double_2 >= c_double_1", true)
        .addExpr("c_double_1 >= c_double_1", true)
        .addExpr("c_double_1 >= c_double_2", false)

        .addExpr("c_decimal_2 >= c_decimal_1", true)
        .addExpr("c_decimal_1 >= c_decimal_1", true)
        .addExpr("c_decimal_1 >= c_decimal_2", false)

        .addExpr("c_varchar_2 >= c_varchar_1", true)
        .addExpr("c_varchar_1 >= c_varchar_1", true)
        .addExpr("c_varchar_1 >= c_varchar_2", false)
        ;

    checker.buildRunAndCheck();
  }

  @Test(expected = RuntimeException.class)
  public void testGreaterThanOrEqualsException() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_false >= c_boolean_true", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testLessThan() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_2 < c_tinyint_1", false)
        .addExpr("c_tinyint_1 < c_tinyint_1", false)
        .addExpr("c_tinyint_1 < c_tinyint_2", true)

        .addExpr("c_smallint_2 < c_smallint_1", false)
        .addExpr("c_smallint_1 < c_smallint_1", false)
        .addExpr("c_smallint_1 < c_smallint_2", true)

        .addExpr("c_integer_2 < c_integer_1", false)
        .addExpr("c_integer_1 < c_integer_1", false)
        .addExpr("c_integer_1 < c_integer_2", true)

        .addExpr("c_bigint_2 < c_bigint_1", false)
        .addExpr("c_bigint_1 < c_bigint_1", false)
        .addExpr("c_bigint_1 < c_bigint_2", true)

        .addExpr("c_float_2 < c_float_1", false)
        .addExpr("c_float_1 < c_float_1", false)
        .addExpr("c_float_1 < c_float_2", true)

        .addExpr("c_double_2 < c_double_1", false)
        .addExpr("c_double_1 < c_double_1", false)
        .addExpr("c_double_1 < c_double_2", true)

        .addExpr("c_decimal_2 < c_decimal_1", false)
        .addExpr("c_decimal_1 < c_decimal_1", false)
        .addExpr("c_decimal_1 < c_decimal_2", true)

        .addExpr("c_varchar_2 < c_varchar_1", false)
        .addExpr("c_varchar_1 < c_varchar_1", false)
        .addExpr("c_varchar_1 < c_varchar_2", true)
        ;

    checker.buildRunAndCheck();
  }

  @Test(expected = RuntimeException.class)
  public void testLessThanException() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_false < c_boolean_true", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testLessThanOrEquals() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_2 <= c_tinyint_1", false)
        .addExpr("c_tinyint_1 <= c_tinyint_1", true)
        .addExpr("c_tinyint_1 <= c_tinyint_2", true)

        .addExpr("c_smallint_2 <= c_smallint_1", false)
        .addExpr("c_smallint_1 <= c_smallint_1", true)
        .addExpr("c_smallint_1 <= c_smallint_2", true)

        .addExpr("c_integer_2 <= c_integer_1", false)
        .addExpr("c_integer_1 <= c_integer_1", true)
        .addExpr("c_integer_1 <= c_integer_2", true)

        .addExpr("c_bigint_2 <= c_bigint_1", false)
        .addExpr("c_bigint_1 <= c_bigint_1", true)
        .addExpr("c_bigint_1 <= c_bigint_2", true)

        .addExpr("c_float_2 <= c_float_1", false)
        .addExpr("c_float_1 <= c_float_1", true)
        .addExpr("c_float_1 <= c_float_2", true)

        .addExpr("c_double_2 <= c_double_1", false)
        .addExpr("c_double_1 <= c_double_1", true)
        .addExpr("c_double_1 <= c_double_2", true)

        .addExpr("c_decimal_2 <= c_decimal_1", false)
        .addExpr("c_decimal_1 <= c_decimal_1", true)
        .addExpr("c_decimal_1 <= c_decimal_2", true)

        .addExpr("c_varchar_2 <= c_varchar_1", false)
        .addExpr("c_varchar_1 <= c_varchar_1", true)
        .addExpr("c_varchar_1 <= c_varchar_2", true)
        ;

    checker.buildRunAndCheck();
  }

  @Test(expected = RuntimeException.class)
  public void testLessThanOrEqualsException() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_false <= c_boolean_true", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testIsNullAndIsNotNull() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 IS NOT NULL", true)
        .addExpr("NULL IS NOT NULL", false)

        .addExpr("1 IS NULL", false)
        .addExpr("NULL IS NULL", true)
        ;

    checker.buildRunAndCheck();
  }

  @Override protected PCollection<Row> getTestPCollection() {
    RowType type = RowSqlType.builder()
        .withTinyIntField("c_tinyint_0")
        .withTinyIntField("c_tinyint_1")
        .withTinyIntField("c_tinyint_2")
        .withSmallIntField("c_smallint_0")
        .withSmallIntField("c_smallint_1")
        .withSmallIntField("c_smallint_2")
        .withIntegerField("c_integer_0")
        .withIntegerField("c_integer_1")
        .withIntegerField("c_integer_2")
        .withBigIntField("c_bigint_0")
        .withBigIntField("c_bigint_1")
        .withBigIntField("c_bigint_2")
        .withFloatField("c_float_0")
        .withFloatField("c_float_1")
        .withFloatField("c_float_2")
        .withDoubleField("c_double_0")
        .withDoubleField("c_double_1")
        .withDoubleField("c_double_2")
        .withDecimalField("c_decimal_0")
        .withDecimalField("c_decimal_1")
        .withDecimalField("c_decimal_2")
        .withVarcharField("c_varchar_0")
        .withVarcharField("c_varchar_1")
        .withVarcharField("c_varchar_2")
        .withBooleanField("c_boolean_false")
        .withBooleanField("c_boolean_true")
        .build();

    try {
      return MockedBoundedTable
          .of(type)
          .addRows(
              (byte) 0, (byte) 1, (byte) 2,
              (short) 0, (short) 1, (short) 2,
              0, 1, 2,
              0L, 1L, 2L,
              0.0f, 1.0f, 2.0f,
              0.0, 1.0, 2.0,
              BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ONE.add(BigDecimal.ONE),
              "a", "b", "c",
              false, true
          )
          .buildIOReader(pipeline)
          .setCoder(type.getRowCoder());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
