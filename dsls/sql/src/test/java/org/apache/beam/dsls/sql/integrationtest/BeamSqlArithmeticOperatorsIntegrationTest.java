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
import org.junit.Test;

/**
 * Integration test for arithmetic operators.
 */
public class BeamSqlArithmeticOperatorsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {

  private static final BigDecimal ZERO = BigDecimal.valueOf(0.0);
  private static final BigDecimal ONE0 = BigDecimal.valueOf(1);
  private static final BigDecimal ONE = BigDecimal.valueOf(1.0);
  private static final BigDecimal ONE2 = BigDecimal.valueOf(1.0).multiply(BigDecimal.valueOf(1.0));
  private static final BigDecimal TWO = BigDecimal.valueOf(2.0);

  @Test
  public void testPlus() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 + 1", 2)
        .addExpr("1.0 + 1", TWO)
        .addExpr("1 + 1.0", TWO)
        .addExpr("1.0 + 1.0", TWO)
        .addExpr("c_tinyint + c_tinyint", (byte) 2)
        .addExpr("c_smallint + c_smallint", (short) 2)
        .addExpr("c_bigint + c_bigint", 2L)
        .addExpr("c_decimal + c_decimal", TWO)
        .addExpr("c_tinyint + c_decimal", TWO)
        .addExpr("c_float + c_decimal", 2.0)
        .addExpr("c_double + c_decimal", 2.0)
        .addExpr("c_float + c_float", 2.0f)
        .addExpr("c_double + c_float", 2.0)
        .addExpr("c_double + c_double", 2.0)
        .addExpr("c_float + c_bigint", 2.0f)
        .addExpr("c_double + c_bigint", 2.0)
        ;

    checker.buildRunAndCheck();
  }

  @Test
  public void testPlus_overflow() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("c_tinyint_max + c_tinyint_max", -2)
        .addExpr("c_smallint_max + c_smallint_max", -2)
        .addExpr("c_integer_max + c_integer_max", -2)
        // yeah, I know 384L is strange, but since it is already overflowed
        // what the actualy result is not so important, it is wrong any way.
        .addExpr("c_bigint_max + c_bigint_max", 384L)
        ;

    checker.buildRunAndCheck();
  }

  @Test
  public void testMinus() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 - 1", 0)
        .addExpr("1.0 - 1", ZERO)
        .addExpr("1 - 0.0", ONE)
        .addExpr("1.0 - 1.0", ZERO)
        .addExpr("c_tinyint - c_tinyint", (byte) 0)
        .addExpr("c_smallint - c_smallint", (short) 0)
        .addExpr("c_bigint - c_bigint", 0L)
        .addExpr("c_decimal - c_decimal", ZERO)
        .addExpr("c_tinyint - c_decimal", ZERO)
        .addExpr("c_float - c_decimal", 0.0)
        .addExpr("c_double - c_decimal", 0.0)
        .addExpr("c_float - c_float", 0.0f)
        .addExpr("c_double - c_float", 0.0)
        .addExpr("c_double - c_double", 0.0)
        .addExpr("c_float - c_bigint", 0.0f)
        .addExpr("c_double - c_bigint", 0.0)
        ;

    checker.buildRunAndCheck();
  }

  @Test
  public void testMultiply() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 * 1", 1)
        .addExpr("1.0 * 1", ONE2)
        .addExpr("1 * 1.0", ONE2)
        .addExpr("1.0 * 1.0", ONE2)
        .addExpr("c_tinyint * c_tinyint", (byte) 1)
        .addExpr("c_smallint * c_smallint", (short) 1)
        .addExpr("c_bigint * c_bigint", 1L)
        .addExpr("c_decimal * c_decimal", ONE2)
        .addExpr("c_tinyint * c_decimal", ONE2)
        .addExpr("c_float * c_decimal", 1.0)
        .addExpr("c_double * c_decimal", 1.0)
        .addExpr("c_float * c_float", 1.0f)
        .addExpr("c_double * c_float", 1.0)
        .addExpr("c_double * c_double", 1.0)
        .addExpr("c_float * c_bigint", 1.0f)
        .addExpr("c_double * c_bigint", 1.0)
        ;

    checker.buildRunAndCheck();
  }

  @Test
  public void testDivide() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 / 1", 1)
        .addExpr("1.0 / 1", ONE0)
        .addExpr("1 / 1.0", ONE0)
        .addExpr("1.0 / 1.0", ONE0)
        .addExpr("c_tinyint / c_tinyint", (byte) 1)
        .addExpr("c_smallint / c_smallint", (short) 1)
        .addExpr("c_bigint / c_bigint", 1L)
        .addExpr("c_decimal / c_decimal", ONE0)
        .addExpr("c_tinyint / c_decimal", ONE0)
        .addExpr("c_float / c_decimal", 1.0)
        .addExpr("c_double / c_decimal", 1.0)
        .addExpr("c_float / c_float", 1.0f)
        .addExpr("c_double / c_float", 1.0)
        .addExpr("c_double / c_double", 1.0)
        .addExpr("c_float / c_bigint", 1.0f)
        .addExpr("c_double / c_bigint", 1.0)
        ;

    checker.buildRunAndCheck();
  }

  @Test
  public void testMod() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("mod(1, 1)", 0)
        .addExpr("mod(1.0, 1)", 0)
        .addExpr("mod(1, 1.0)", ZERO)
        .addExpr("mod(1.0, 1.0)", ZERO)
        .addExpr("mod(c_tinyint, c_tinyint)", (byte) 0)
        .addExpr("mod(c_smallint, c_smallint)", (short) 0)
        .addExpr("mod(c_bigint, c_bigint)", 0L)
        .addExpr("mod(c_decimal, c_decimal)", ZERO)
        .addExpr("mod(c_tinyint, c_decimal)", ZERO)
        ;

    checker.buildRunAndCheck();
  }
}
