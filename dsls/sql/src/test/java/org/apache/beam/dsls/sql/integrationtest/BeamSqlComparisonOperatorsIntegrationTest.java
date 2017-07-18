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

package org.apache.beam.dsls.sql.integrationtest;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.values.PCollection;
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
        .addExpr("c_boolean_1 = c_boolean_1", true)
        .addExpr("c_boolean_1 = c_boolean_0", false)

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
        .addExpr("c_boolean_1 <> c_boolean_1", false)
        .addExpr("c_boolean_1 <> c_boolean_0", true)
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
  public void testGreaterThan_exception() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_0 > c_boolean_1", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testGreaterEqualThan() {
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
  public void testGreaterEqualThan_exception() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_0 >= c_boolean_1", false);
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
  public void testLessThan_exception() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_0 < c_boolean_1", false);
    checker.buildRunAndCheck();
  }

  @Test
  public void testLessEqualThan() {
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
  public void testLessEqualThan_exception() {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_boolean_0 <= c_boolean_1", false);
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

  @Override protected PCollection<BeamSqlRow> getTestPCollection() {
    BeamSqlRecordType type = BeamSqlRecordType.create(
        Arrays.asList(
            "c_tinyint_0", "c_tinyint_1", "c_tinyint_2",
            "c_smallint_0", "c_smallint_1", "c_smallint_2",
            "c_integer_0", "c_integer_1", "c_integer_2",
            "c_bigint_0", "c_bigint_1", "c_bigint_2",
            "c_float_0", "c_float_1", "c_float_2",
            "c_double_0", "c_double_1", "c_double_2",
            "c_decimal_0", "c_decimal_1", "c_decimal_2",
            "c_varchar_0", "c_varchar_1", "c_varchar_2",
            "c_boolean_0", "c_boolean_1"
            ),
        Arrays.asList(
            Types.TINYINT, Types.TINYINT, Types.TINYINT,
            Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
            Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.BIGINT, Types.BIGINT, Types.BIGINT,
            Types.FLOAT, Types.FLOAT, Types.FLOAT,
            Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
            Types.DECIMAL, Types.DECIMAL, Types.DECIMAL,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.BOOLEAN, Types.BOOLEAN
        )
    );
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
          .setCoder(new BeamSqlRowCoder(type));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
