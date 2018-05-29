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
import java.util.Random;
import org.apache.calcite.runtime.SqlFunctions;
import org.junit.Test;

/** Integration test for built-in MATH functions. */
public class BeamSqlMathFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final int INTEGER_VALUE = 1;
  private static final long LONG_VALUE = 1L;
  private static final short SHORT_VALUE = 1;
  private static final byte BYTE_VALUE = 1;
  private static final double DOUBLE_VALUE = 1.0;
  private static final float FLOAT_VALUE = 1.0f;
  private static final BigDecimal DECIMAL_VALUE = BigDecimal.ONE;

  @Test
  public void testAbs() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ABS(c_integer)", Math.abs(INTEGER_VALUE))
            .addExpr("ABS(c_bigint)", Math.abs(LONG_VALUE))
            .addExpr("ABS(c_smallint)", (short) Math.abs(SHORT_VALUE))
            .addExpr("ABS(c_tinyint)", (byte) Math.abs(BYTE_VALUE))
            .addExpr("ABS(c_double)", Math.abs(DOUBLE_VALUE))
            .addExpr("ABS(c_float)", Math.abs(FLOAT_VALUE))
            .addExpr("ABS(c_decimal)", new BigDecimal(Math.abs(DECIMAL_VALUE.doubleValue())));

    checker.buildRunAndCheck();
  }

  @Test
  public void testSqrt() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SQRT(c_integer)", Math.sqrt(INTEGER_VALUE))
            .addExpr("SQRT(c_bigint)", Math.sqrt(LONG_VALUE))
            .addExpr("SQRT(c_smallint)", Math.sqrt(SHORT_VALUE))
            .addExpr("SQRT(c_tinyint)", Math.sqrt(BYTE_VALUE))
            .addExpr("SQRT(c_double)", Math.sqrt(DOUBLE_VALUE))
            .addExpr("SQRT(c_float)", Math.sqrt(FLOAT_VALUE))
            .addExpr("SQRT(c_decimal)", Math.sqrt(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testRound() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ROUND(c_integer, 0)", SqlFunctions.sround(INTEGER_VALUE, 0))
            .addExpr("ROUND(c_bigint, 0)", SqlFunctions.sround(LONG_VALUE, 0))
            .addExpr("ROUND(c_smallint, 0)", (short) SqlFunctions.sround(SHORT_VALUE, 0))
            .addExpr("ROUND(c_tinyint, 0)", (byte) SqlFunctions.sround(BYTE_VALUE, 0))
            .addExpr("ROUND(c_double, 0)", SqlFunctions.sround(DOUBLE_VALUE, 0))
            .addExpr("ROUND(c_float, 0)", (float) SqlFunctions.sround(FLOAT_VALUE, 0))
            .addExpr(
                "ROUND(c_decimal, 0)",
                new BigDecimal(SqlFunctions.sround(DECIMAL_VALUE.doubleValue(), 0)));

    checker.buildRunAndCheck();
  }

  @Test
  public void testLn() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("LN(c_integer)", Math.log(INTEGER_VALUE))
            .addExpr("LN(c_bigint)", Math.log(LONG_VALUE))
            .addExpr("LN(c_smallint)", Math.log(SHORT_VALUE))
            .addExpr("LN(c_tinyint)", Math.log(BYTE_VALUE))
            .addExpr("LN(c_double)", Math.log(DOUBLE_VALUE))
            .addExpr("LN(c_float)", Math.log(FLOAT_VALUE))
            .addExpr("LN(c_decimal)", Math.log(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testLog10() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("LOG10(c_integer)", Math.log10(INTEGER_VALUE))
            .addExpr("LOG10(c_bigint)", Math.log10(LONG_VALUE))
            .addExpr("LOG10(c_smallint)", Math.log10(SHORT_VALUE))
            .addExpr("LOG10(c_tinyint)", Math.log10(BYTE_VALUE))
            .addExpr("LOG10(c_double)", Math.log10(DOUBLE_VALUE))
            .addExpr("LOG10(c_float)", Math.log10(FLOAT_VALUE))
            .addExpr("LOG10(c_decimal)", Math.log10(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testExp() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("EXP(c_integer)", Math.exp(INTEGER_VALUE))
            .addExpr("EXP(c_bigint)", Math.exp(LONG_VALUE))
            .addExpr("EXP(c_smallint)", Math.exp(SHORT_VALUE))
            .addExpr("EXP(c_tinyint)", Math.exp(BYTE_VALUE))
            .addExpr("EXP(c_double)", Math.exp(DOUBLE_VALUE))
            .addExpr("EXP(c_float)", Math.exp(FLOAT_VALUE))
            .addExpr("EXP(c_decimal)", Math.exp(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testAcos() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ACOS(c_integer)", Math.acos(INTEGER_VALUE))
            .addExpr("ACOS(c_bigint)", Math.acos(LONG_VALUE))
            .addExpr("ACOS(c_smallint)", Math.acos(SHORT_VALUE))
            .addExpr("ACOS(c_tinyint)", Math.acos(BYTE_VALUE))
            .addExpr("ACOS(c_double)", Math.acos(DOUBLE_VALUE))
            .addExpr("ACOS(c_float)", Math.acos(FLOAT_VALUE))
            .addExpr("ACOS(c_decimal)", Math.acos(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testAsin() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ASIN(c_integer)", Math.asin(INTEGER_VALUE))
            .addExpr("ASIN(c_bigint)", Math.asin(LONG_VALUE))
            .addExpr("ASIN(c_smallint)", Math.asin(SHORT_VALUE))
            .addExpr("ASIN(c_tinyint)", Math.asin(BYTE_VALUE))
            .addExpr("ASIN(c_double)", Math.asin(DOUBLE_VALUE))
            .addExpr("ASIN(c_float)", Math.asin(FLOAT_VALUE))
            .addExpr("ASIN(c_decimal)", Math.asin(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testAtan() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ATAN(c_integer)", Math.atan(INTEGER_VALUE))
            .addExpr("ATAN(c_bigint)", Math.atan(LONG_VALUE))
            .addExpr("ATAN(c_smallint)", Math.atan(SHORT_VALUE))
            .addExpr("ATAN(c_tinyint)", Math.atan(BYTE_VALUE))
            .addExpr("ATAN(c_double)", Math.atan(DOUBLE_VALUE))
            .addExpr("ATAN(c_float)", Math.atan(FLOAT_VALUE))
            .addExpr("ATAN(c_decimal)", Math.atan(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testCot() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("COT(c_integer)", 1.0d / Math.tan(INTEGER_VALUE))
            .addExpr("COT(c_bigint)", 1.0d / Math.tan(LONG_VALUE))
            .addExpr("COT(c_smallint)", 1.0d / Math.tan(SHORT_VALUE))
            .addExpr("COT(c_tinyint)", 1.0d / Math.tan(BYTE_VALUE))
            .addExpr("COT(c_double)", 1.0d / Math.tan(DOUBLE_VALUE))
            .addExpr("COT(c_float)", 1.0d / Math.tan(FLOAT_VALUE))
            .addExpr("COT(c_decimal)", 1.0d / Math.tan(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testDegrees() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("DEGREES(c_integer)", Math.toDegrees(INTEGER_VALUE))
            .addExpr("DEGREES(c_bigint)", Math.toDegrees(LONG_VALUE))
            .addExpr("DEGREES(c_smallint)", Math.toDegrees(SHORT_VALUE))
            .addExpr("DEGREES(c_tinyint)", Math.toDegrees(BYTE_VALUE))
            .addExpr("DEGREES(c_double)", Math.toDegrees(DOUBLE_VALUE))
            .addExpr("DEGREES(c_float)", Math.toDegrees(FLOAT_VALUE))
            .addExpr("DEGREES(c_decimal)", Math.toDegrees(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testRadians() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("RADIANS(c_integer)", Math.toRadians(INTEGER_VALUE))
            .addExpr("RADIANS(c_bigint)", Math.toRadians(LONG_VALUE))
            .addExpr("RADIANS(c_smallint)", Math.toRadians(SHORT_VALUE))
            .addExpr("RADIANS(c_tinyint)", Math.toRadians(BYTE_VALUE))
            .addExpr("RADIANS(c_double)", Math.toRadians(DOUBLE_VALUE))
            .addExpr("RADIANS(c_float)", Math.toRadians(FLOAT_VALUE))
            .addExpr("RADIANS(c_decimal)", Math.toRadians(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testCos() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("COS(c_integer)", Math.cos(INTEGER_VALUE))
            .addExpr("COS(c_bigint)", Math.cos(LONG_VALUE))
            .addExpr("COS(c_smallint)", Math.cos(SHORT_VALUE))
            .addExpr("COS(c_tinyint)", Math.cos(BYTE_VALUE))
            .addExpr("COS(c_double)", Math.cos(DOUBLE_VALUE))
            .addExpr("COS(c_float)", Math.cos(FLOAT_VALUE))
            .addExpr("COS(c_decimal)", Math.cos(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testSin() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SIN(c_integer)", Math.sin(INTEGER_VALUE))
            .addExpr("SIN(c_bigint)", Math.sin(LONG_VALUE))
            .addExpr("SIN(c_smallint)", Math.sin(SHORT_VALUE))
            .addExpr("SIN(c_tinyint)", Math.sin(BYTE_VALUE))
            .addExpr("SIN(c_double)", Math.sin(DOUBLE_VALUE))
            .addExpr("SIN(c_float)", Math.sin(FLOAT_VALUE))
            .addExpr("SIN(c_decimal)", Math.sin(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testTan() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("TAN(c_integer)", Math.tan(INTEGER_VALUE))
            .addExpr("TAN(c_bigint)", Math.tan(LONG_VALUE))
            .addExpr("TAN(c_smallint)", Math.tan(SHORT_VALUE))
            .addExpr("TAN(c_tinyint)", Math.tan(BYTE_VALUE))
            .addExpr("TAN(c_double)", Math.tan(DOUBLE_VALUE))
            .addExpr("TAN(c_float)", Math.tan(FLOAT_VALUE))
            .addExpr("TAN(c_decimal)", Math.tan(DECIMAL_VALUE.doubleValue()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testSign() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SIGN(c_integer)", Integer.signum(INTEGER_VALUE))
            .addExpr("SIGN(c_bigint)", (long) (Long.signum(LONG_VALUE)))
            .addExpr("SIGN(c_smallint)", (short) (Integer.signum(SHORT_VALUE)))
            .addExpr("SIGN(c_tinyint)", (byte) Integer.signum(BYTE_VALUE))
            .addExpr("SIGN(c_double)", Math.signum(DOUBLE_VALUE))
            .addExpr("SIGN(c_float)", Math.signum(FLOAT_VALUE))
            .addExpr("SIGN(c_decimal)", BigDecimal.valueOf(DECIMAL_VALUE.signum()));

    checker.buildRunAndCheck();
  }

  @Test
  public void testPower() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("POWER(c_integer, 2)", Math.pow(INTEGER_VALUE, 2))
            .addExpr("POWER(c_bigint, 2)", Math.pow(LONG_VALUE, 2))
            .addExpr("POWER(c_smallint, 2)", Math.pow(SHORT_VALUE, 2))
            .addExpr("POWER(c_tinyint, 2)", Math.pow(BYTE_VALUE, 2))
            .addExpr("POWER(c_double, 2)", Math.pow(DOUBLE_VALUE, 2))
            .addExpr("POWER(c_float, 2)", Math.pow(FLOAT_VALUE, 2))
            .addExpr("POWER(c_decimal, 2)", Math.pow(DECIMAL_VALUE.doubleValue(), 2));

    checker.buildRunAndCheck();
  }

  @Test
  public void testPi() throws Exception {
    ExpressionChecker checker = new ExpressionChecker().addExpr("PI", Math.PI);

    checker.buildRunAndCheck();
  }

  @Test
  public void testAtan2() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ATAN2(c_integer, 2)", Math.atan2(INTEGER_VALUE, 2))
            .addExpr("ATAN2(c_bigint, 2)", Math.atan2(LONG_VALUE, 2))
            .addExpr("ATAN2(c_smallint, 2)", Math.atan2(SHORT_VALUE, 2))
            .addExpr("ATAN2(c_tinyint, 2)", Math.atan2(BYTE_VALUE, 2))
            .addExpr("ATAN2(c_double, 2)", Math.atan2(DOUBLE_VALUE, 2))
            .addExpr("ATAN2(c_float, 2)", Math.atan2(FLOAT_VALUE, 2))
            .addExpr("ATAN2(c_decimal, 2)", Math.atan2(DECIMAL_VALUE.doubleValue(), 2));

    checker.buildRunAndCheck();
  }

  @Test
  public void testTruncate() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("TRUNCATE(c_integer, 2)", SqlFunctions.struncate(INTEGER_VALUE, 2))
            .addExpr("TRUNCATE(c_bigint, 2)", SqlFunctions.struncate(LONG_VALUE, 2))
            .addExpr("TRUNCATE(c_smallint, 2)", (short) SqlFunctions.struncate(SHORT_VALUE, 2))
            .addExpr("TRUNCATE(c_tinyint, 2)", (byte) SqlFunctions.struncate(BYTE_VALUE, 2))
            .addExpr("TRUNCATE(c_double, 2)", SqlFunctions.struncate(DOUBLE_VALUE, 2))
            .addExpr("TRUNCATE(c_float, 2)", (float) SqlFunctions.struncate(FLOAT_VALUE, 2))
            .addExpr("TRUNCATE(c_decimal, 2)", SqlFunctions.struncate(DECIMAL_VALUE, 2));

    checker.buildRunAndCheck();
  }

  @Test
  public void testRand() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker().addExpr("RAND(c_integer)", new Random(INTEGER_VALUE).nextDouble());

    checker.buildRunAndCheck();
  }

  @Test
  public void testRandInteger() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr(
                "RAND_INTEGER(c_integer, c_integer)",
                new Random(INTEGER_VALUE).nextInt(INTEGER_VALUE));

    checker.buildRunAndCheck();
  }
}
