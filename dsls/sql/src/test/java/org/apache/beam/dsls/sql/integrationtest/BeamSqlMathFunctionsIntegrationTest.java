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
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.dsls.sql.BeamSqlDslBase;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.runtime.SqlFunctions;
import org.junit.Test;

/**
 * Integration test for built-in MATH functions.
 */
public class BeamSqlMathFunctionsIntegrationTest extends BeamSqlDslBase {
  private static final int INTEGER_VALUE = 1;
  private static final long LONG_VALUE = 1000L;
  private static final short SHORT_VALUE = 1;
  private static final byte BYTE_VALUE = 1;
  private static final double DOUBLE_VALUE = 1.0;
  private static final float FLOAT_VALUE = 1.0f;
  private static final BigDecimal DECIMAL_VALUE = new BigDecimal(1);

  @Test
  public void testAbs() throws Exception{
    String sql = "SELECT "
        + "ABS(f_int) as abs_int,"
        + "ABS(f_long) as abs_long,"
        + "ABS(f_short) as abs_short,"
        + "ABS(f_byte) as abs_byte,"
        + "ABS(f_double) as abs_double,"
        + "ABS(f_float) as abs_float, "
        + "ABS(f_decimal) as abs_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.INTEGER, "abs_int",
            Types.BIGINT, "abs_long",
            Types.SMALLINT, "abs_short",
            Types.TINYINT, "abs_byte",
            Types.DOUBLE, "abs_double",
            Types.FLOAT, "abs_float",
            Types.DECIMAL, "abs_decimal"
        ).addRows(
            Math.abs(INTEGER_VALUE), Math.abs(LONG_VALUE), (short) Math.abs(SHORT_VALUE),
            (byte) Math.abs(BYTE_VALUE), Math.abs(DOUBLE_VALUE),
            Math.abs(FLOAT_VALUE), new BigDecimal(Math.abs(DECIMAL_VALUE.doubleValue()))
        ).getRows());
    pipeline.run();
  }

  @Test
  public void testSqrt() throws Exception{
    String sql = "SELECT "
        + "SQRT(f_int) as sqrt_int,"
        + "SQRT(f_long) as sqrt_long,"
        + "SQRT(f_short) as sqrt_short,"
        + "SQRT(f_byte) as sqrt_byte,"
        + "SQRT(f_double) as sqrt_double,"
        + "SQRT(f_float) as sqrt_float, "
        + "SQRT(f_decimal) as sqrt_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "sqrt_int",
            Types.DOUBLE, "sqrt_long",
            Types.DOUBLE, "sqrt_short",
            Types.DOUBLE, "sqrt_byte",
            Types.DOUBLE, "sqrt_double",
            Types.DOUBLE, "sqrt_float",
            Types.DOUBLE, "sqrt_decimal"
        ).addRows(
            Math.sqrt(INTEGER_VALUE), Math.sqrt(LONG_VALUE), Math.sqrt(SHORT_VALUE),
            Math.sqrt(BYTE_VALUE), Math.sqrt(DOUBLE_VALUE),
            Math.sqrt(FLOAT_VALUE), Math.sqrt(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRound() throws Exception{
    String sql = "SELECT "
        + "ROUND(f_int, 0) as round_int,"
        + "ROUND(f_long, 0) as round_long,"
        + "ROUND(f_short, 0) as round_short,"
        + "ROUND(f_byte, 0) as round_byte,"
        + "ROUND(f_double, 0) as round_double,"
        + "ROUND(f_float, 0) as round_float, "
        + "ROUND(f_decimal, 0) as round_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.INTEGER, "round_int",
            Types.BIGINT, "round_long",
            Types.SMALLINT, "round_short",
            Types.TINYINT, "round_byte",
            Types.DOUBLE, "round_double",
            Types.FLOAT, "round_float",
            Types.DECIMAL, "round_decimal"
        ).addRows(
            SqlFunctions.sround(INTEGER_VALUE, 0), SqlFunctions.sround(LONG_VALUE, 0),
            (short) SqlFunctions.sround(SHORT_VALUE, 0), (byte) SqlFunctions.sround(BYTE_VALUE, 0),
            SqlFunctions.sround(DOUBLE_VALUE, 0), (float) SqlFunctions.sround(FLOAT_VALUE, 0),
            new BigDecimal(SqlFunctions.sround(DECIMAL_VALUE.doubleValue(), 0))
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLn() throws Exception{
    String sql = "SELECT "
        + "LN(f_int) as ln_int,"
        + "LN(f_long) as ln_long,"
        + "LN(f_short) as ln_short,"
        + "LN(f_byte) as ln_byte,"
        + "LN(f_double) as ln_double,"
        + "LN(f_float) as ln_float, "
        + "LN(f_decimal) as ln_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "ln_int",
            Types.DOUBLE, "ln_long",
            Types.DOUBLE, "ln_short",
            Types.DOUBLE, "ln_byte",
            Types.DOUBLE, "ln_double",
            Types.DOUBLE, "ln_float",
            Types.DOUBLE, "ln_decimal"
        ).addRows(
            Math.log(INTEGER_VALUE), Math.log(LONG_VALUE), Math.log(SHORT_VALUE),
            Math.log(BYTE_VALUE), Math.log(DOUBLE_VALUE), Math.log(FLOAT_VALUE),
            Math.log(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLog10() throws Exception{
    String sql = "SELECT "
        + "LOG10(f_int) as log10_int,"
        + "LOG10(f_long) as log10_long,"
        + "LOG10(f_short) as log10_short,"
        + "LOG10(f_byte) as log10_byte,"
        + "LOG10(f_double) as log10_double,"
        + "LOG10(f_float) as log10_float, "
        + "LOG10(f_decimal) as log10_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "log10_int",
            Types.DOUBLE, "log10_long",
            Types.DOUBLE, "log10_short",
            Types.DOUBLE, "log10_byte",
            Types.DOUBLE, "log10_double",
            Types.DOUBLE, "log10_float",
            Types.DOUBLE, "log10_decimal"
        ).addRows(
            Math.log10(INTEGER_VALUE), Math.log10(LONG_VALUE), Math.log10(SHORT_VALUE),
            Math.log10(BYTE_VALUE), Math.log10(DOUBLE_VALUE), Math.log10(FLOAT_VALUE),
            Math.log10(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExp() throws Exception{
    String sql = "SELECT "
        + "EXP(f_int) as exp_int,"
        + "EXP(f_long) as exp_long,"
        + "EXP(f_short) as exp_short,"
        + "EXP(f_byte) as exp_byte,"
        + "EXP(f_double) as exp_double,"
        + "EXP(f_float) as exp_float, "
        + "EXP(f_decimal) as exp_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "exp_int",
            Types.DOUBLE, "exp_long",
            Types.DOUBLE, "exp_short",
            Types.DOUBLE, "exp_byte",
            Types.DOUBLE, "exp_double",
            Types.DOUBLE, "exp_float",
            Types.DOUBLE, "exp_decimal"
        ).addRows(
            Math.exp(INTEGER_VALUE), Math.exp(LONG_VALUE), Math.exp(SHORT_VALUE),
            Math.exp(BYTE_VALUE), Math.exp(DOUBLE_VALUE), Math.exp(FLOAT_VALUE),
            Math.exp(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAcos() throws Exception{
    String sql = "SELECT "
        + "ACOS(f_int) as acos_int,"
        + "ACOS(f_long) as acos_long,"
        + "ACOS(f_short) as acos_short,"
        + "ACOS(f_byte) as acos_byte,"
        + "ACOS(f_double) as acos_double,"
        + "ACOS(f_float) as acos_float, "
        + "ACOS(f_decimal) as acos_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "acos_int",
            Types.DOUBLE, "acos_long",
            Types.DOUBLE, "acos_short",
            Types.DOUBLE, "acos_byte",
            Types.DOUBLE, "acos_double",
            Types.DOUBLE, "acos_float",
            Types.DOUBLE, "acos_decimal"
        ).addRows(
            Math.acos(INTEGER_VALUE), Math.acos(LONG_VALUE), Math.acos(SHORT_VALUE),
            Math.acos(BYTE_VALUE), Math.acos(DOUBLE_VALUE), Math.acos(FLOAT_VALUE),
            Math.acos(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAsin() throws Exception{
    String sql = "SELECT "
        + "ASIN(f_int) as asin_int,"
        + "ASIN(f_long) as asin_long,"
        + "ASIN(f_short) as asin_short,"
        + "ASIN(f_byte) as asin_byte,"
        + "ASIN(f_double) as asin_double,"
        + "ASIN(f_float) as asin_float, "
        + "ASIN(f_decimal) as asin_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "asin_int",
            Types.DOUBLE, "asin_long",
            Types.DOUBLE, "asin_short",
            Types.DOUBLE, "asin_byte",
            Types.DOUBLE, "asin_double",
            Types.DOUBLE, "asin_float",
            Types.DOUBLE, "asin_decimal"
        ).addRows(
            Math.asin(INTEGER_VALUE), Math.asin(LONG_VALUE), Math.asin(SHORT_VALUE),
            Math.asin(BYTE_VALUE), Math.asin(DOUBLE_VALUE), Math.asin(FLOAT_VALUE),
            Math.asin(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAtan() throws Exception{
    String sql = "SELECT "
        + "ATAN(f_int) as atan_int,"
        + "ATAN(f_long) as atan_long,"
        + "ATAN(f_short) as atan_short,"
        + "ATAN(f_byte) as atan_byte,"
        + "ATAN(f_double) as atan_double,"
        + "ATAN(f_float) as atan_float, "
        + "ATAN(f_decimal) as atan_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "atan_int",
            Types.DOUBLE, "atan_long",
            Types.DOUBLE, "atan_short",
            Types.DOUBLE, "atan_byte",
            Types.DOUBLE, "atan_double",
            Types.DOUBLE, "atan_float",
            Types.DOUBLE, "atan_decimal"
        ).addRows(
            Math.atan(INTEGER_VALUE), Math.atan(LONG_VALUE), Math.atan(SHORT_VALUE),
            Math.atan(BYTE_VALUE), Math.atan(DOUBLE_VALUE), Math.atan(FLOAT_VALUE),
            Math.atan(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCot() throws Exception{
    String sql = "SELECT "
        + "COT(f_int) as cot_int,"
        + "COT(f_long) as cot_long,"
        + "COT(f_short) as cot_short,"
        + "COT(f_byte) as cot_byte,"
        + "COT(f_double) as cot_double,"
        + "COT(f_float) as cot_float, "
        + "COT(f_decimal) as cot_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "cot_int",
            Types.DOUBLE, "cot_long",
            Types.DOUBLE, "cot_short",
            Types.DOUBLE, "cot_byte",
            Types.DOUBLE, "cot_double",
            Types.DOUBLE, "cot_float",
            Types.DOUBLE, "cot_decimal"
        ).addRows(
            1.0d / Math.tan(INTEGER_VALUE), 1.0d / Math.tan(LONG_VALUE),
            1.0d / Math.tan(SHORT_VALUE), 1.0d / Math.tan(BYTE_VALUE),
            1.0d / Math.tan(DOUBLE_VALUE), 1.0d / Math.tan(FLOAT_VALUE),
            1.0d / Math.tan(DECIMAL_VALUE.doubleValue())).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDegrees() throws Exception{
    String sql = "SELECT "
        + "DEGREES(f_int) as degrees_int,"
        + "DEGREES(f_long) as degrees_long,"
        + "DEGREES(f_short) as degrees_short,"
        + "DEGREES(f_byte) as degrees_byte,"
        + "DEGREES(f_double) as degrees_double,"
        + "DEGREES(f_float) as degrees_float, "
        + "DEGREES(f_decimal) as degrees_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "degrees_int",
            Types.DOUBLE, "degrees_long",
            Types.DOUBLE, "degrees_short",
            Types.DOUBLE, "degrees_byte",
            Types.DOUBLE, "degrees_double",
            Types.DOUBLE, "degrees_float",
            Types.DOUBLE, "degrees_decimal"
        ).addRows(
            Math.toDegrees(INTEGER_VALUE), Math.toDegrees(LONG_VALUE), Math.toDegrees(SHORT_VALUE),
            Math.toDegrees(BYTE_VALUE), Math.toDegrees(DOUBLE_VALUE), Math.toDegrees(FLOAT_VALUE),
            Math.toDegrees(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRadians() throws Exception{
    String sql = "SELECT "
        + "RADIANS(f_int) as radians_int,"
        + "RADIANS(f_long) as radians_long,"
        + "RADIANS(f_short) as radians_short,"
        + "RADIANS(f_byte) as radians_byte,"
        + "RADIANS(f_double) as radians_double,"
        + "RADIANS(f_float) as radians_float, "
        + "RADIANS(f_decimal) as radians_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "radians_int",
            Types.DOUBLE, "radians_long",
            Types.DOUBLE, "radians_short",
            Types.DOUBLE, "radians_byte",
            Types.DOUBLE, "radians_double",
            Types.DOUBLE, "radians_float",
            Types.DOUBLE, "radians_decimal"
        ).addRows(
            Math.toRadians(INTEGER_VALUE), Math.toRadians(LONG_VALUE), Math.toRadians(SHORT_VALUE),
            Math.toRadians(BYTE_VALUE), Math.toRadians(DOUBLE_VALUE), Math.toRadians(FLOAT_VALUE),
            Math.toRadians(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCos() throws Exception{
    String sql = "SELECT "
        + "COS(f_int) as cos_int,"
        + "COS(f_long) as cos_long,"
        + "COS(f_short) as cos_short,"
        + "COS(f_byte) as cos_byte,"
        + "COS(f_double) as cos_double,"
        + "COS(f_float) as cos_float, "
        + "COS(f_decimal) as cos_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "cos_int",
            Types.DOUBLE, "cos_long",
            Types.DOUBLE, "cos_short",
            Types.DOUBLE, "cos_byte",
            Types.DOUBLE, "cos_double",
            Types.DOUBLE, "cos_float",
            Types.DOUBLE, "cos_decimal"
        ).addRows(
            Math.cos(INTEGER_VALUE), Math.cos(LONG_VALUE), Math.cos(SHORT_VALUE),
            Math.cos(BYTE_VALUE), Math.cos(DOUBLE_VALUE), Math.cos(FLOAT_VALUE),
            Math.cos(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSin() throws Exception{
    String sql = "SELECT "
        + "SIN(f_int) as sin_int,"
        + "SIN(f_long) as sin_long,"
        + "SIN(f_short) as sin_short,"
        + "SIN(f_byte) as sin_byte,"
        + "SIN(f_double) as sin_double,"
        + "SIN(f_float) as sin_float, "
        + "SIN(f_decimal) as sin_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "sin_int",
            Types.DOUBLE, "sin_long",
            Types.DOUBLE, "sin_short",
            Types.DOUBLE, "sin_byte",
            Types.DOUBLE, "sin_double",
            Types.DOUBLE, "sin_float",
            Types.DOUBLE, "sin_decimal"
        ).addRows(
            Math.sin(INTEGER_VALUE), Math.sin(LONG_VALUE), Math.sin(SHORT_VALUE),
            Math.sin(BYTE_VALUE), Math.sin(DOUBLE_VALUE), Math.sin(FLOAT_VALUE),
            Math.sin(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTan() throws Exception{
    String sql = "SELECT "
        + "TAN(f_int) as tan_int,"
        + "TAN(f_long) as tan_long,"
        + "TAN(f_short) as tan_short,"
        + "TAN(f_byte) as tan_byte,"
        + "TAN(f_double) as tan_double,"
        + "TAN(f_float) as tan_float, "
        + "TAN(f_decimal) as tan_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "tan_int",
            Types.DOUBLE, "tan_long",
            Types.DOUBLE, "tan_short",
            Types.DOUBLE, "tan_byte",
            Types.DOUBLE, "tan_double",
            Types.DOUBLE, "tan_float",
            Types.DOUBLE, "tan_decimal"
        ).addRows(
            Math.tan(INTEGER_VALUE), Math.tan(LONG_VALUE), Math.tan(SHORT_VALUE),
            Math.tan(BYTE_VALUE), Math.tan(DOUBLE_VALUE), Math.tan(FLOAT_VALUE),
            Math.tan(DECIMAL_VALUE.doubleValue())
        ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSign() throws Exception{
    String sql = "SELECT "
        + "SIGN(f_int) as sign_int,"
        + "SIGN(f_long) as sign_long,"
        + "SIGN(f_short) as sign_short,"
        + "SIGN(f_byte) as sign_byte,"
        + "SIGN(f_double) as sign_double,"
        + "SIGN(f_float) as sign_float, "
        + "SIGN(f_decimal) as sign_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.INTEGER, "sign_int",
            Types.BIGINT, "sign_long",
            Types.SMALLINT, "sign_short",
            Types.TINYINT, "sign_byte",
            Types.DOUBLE, "sign_double",
            Types.FLOAT, "sign_float",
            Types.DECIMAL, "sign_decimal"
        ).addRows(
            Integer.signum(INTEGER_VALUE), (long) (Long.signum(LONG_VALUE)),
            (short) (Integer.signum(SHORT_VALUE)), (byte) Integer.signum(BYTE_VALUE),
            Math.signum(DOUBLE_VALUE), Math.signum(FLOAT_VALUE),
            BigDecimal.valueOf(DECIMAL_VALUE.signum())).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPower() throws Exception{
    String sql = "SELECT "
        + "POWER(f_int, 1) as power_int,"
        + "POWER(f_long, 1) as power_long,"
        + "POWER(f_short, 1) as power_short,"
        + "POWER(f_byte, 1) as power_byte,"
        + "POWER(f_double, 1) as power_double,"
        + "POWER(f_float, 1) as power_float, "
        + "POWER(f_decimal, 1) as power_decimal "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.DOUBLE, "power_int",
            Types.DOUBLE, "power_long",
            Types.DOUBLE, "power_short",
            Types.DOUBLE, "power_byte",
            Types.DOUBLE, "power_double",
            Types.DOUBLE, "power_float",
            Types.DOUBLE, "power_decimal"
        ).addRows(
            Math.pow(INTEGER_VALUE, 1), Math.pow(LONG_VALUE, 1), Math.pow(SHORT_VALUE, 1),
            Math.pow(BYTE_VALUE, 1), Math.pow(DOUBLE_VALUE, 1), Math.pow(FLOAT_VALUE, 1),
            Math.pow(DECIMAL_VALUE.doubleValue(), 1)
        ).getRows());
    pipeline.run().waitUntilFinish();
  }
}
