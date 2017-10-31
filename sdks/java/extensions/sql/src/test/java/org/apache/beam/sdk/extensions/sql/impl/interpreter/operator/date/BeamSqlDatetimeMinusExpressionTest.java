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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link BeamSqlDatetimeMinusExpression}.
 */
public class BeamSqlDatetimeMinusExpressionTest {
  private static final BeamRecord NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  private static final Date DATE = new Date(2017, 3, 4, 3, 2, 1);
  private static final Date DATE_MINUS_2_SEC = new DateTime(DATE).minusSeconds(2).toDate();
  private static final Date DATE_MINUS_3_MIN = new DateTime(DATE).minusMinutes(3).toDate();
  private static final Date DATE_MINUS_4_HOURS = new DateTime(DATE).minusHours(4).toDate();
  private static final Date DATE_MINUS_7_DAYS = new DateTime(DATE).minusDays(7).toDate();
  private static final Date DATE_MINUS_2_MONTHS = new DateTime(DATE).minusMonths(2).toDate();
  private static final Date DATE_MINUS_1_YEAR = new DateTime(DATE).minusYears(1).toDate();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void testOutputTypeIsBigint() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE));

    assertEquals(SqlTypeName.BIGINT, minusExpression.getOutputType());
  }

  @Test public void testAccepts2Timestamps() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE));

    assertTrue(minusExpression.accept());
  }

  @Test public void testDoesNotAccept3Timestamps() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE_MINUS_1_YEAR),
            timestamp(DATE));

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAccept1Timestamp() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE));

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptUnsupportedIntervalToCount() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY_MINUTE,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE));

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptNotTimestampAsOperandOne() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3),
            timestamp(DATE));

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptNotTimestampAsOperandTwo() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE),
            BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3));

    assertFalse(minusExpression.accept());
  }

  @Test public void testEvaluateDiffSeconds() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_SECOND,
            timestamp(DATE),
            timestamp(DATE_MINUS_2_SEC));

    long expectedResult = applyMultiplier(2L, TimeUnit.SECOND);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateDiffMinutes() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_MINUTE,
            timestamp(DATE),
            timestamp(DATE_MINUS_3_MIN));

    long expectedResult = applyMultiplier(3L, TimeUnit.MINUTE);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateDiffHours() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_HOUR,
            timestamp(DATE),
            timestamp(DATE_MINUS_4_HOURS));

    long expectedResult = applyMultiplier(4L, TimeUnit.HOUR);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateDiffDays() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY,
            timestamp(DATE),
            timestamp(DATE_MINUS_7_DAYS));

    long expectedResult = applyMultiplier(7L, TimeUnit.DAY);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateDiffMonths() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_MONTH,
            timestamp(DATE),
            timestamp(DATE_MINUS_2_MONTHS));

    long expectedResult = applyMultiplier(2L, TimeUnit.MONTH);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateDiffYears() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_YEAR,
            timestamp(DATE),
            timestamp(DATE_MINUS_1_YEAR));

    long expectedResult = applyMultiplier(1L, TimeUnit.YEAR);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateNegativeDiffSeconds() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_SECOND,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE));

    long expectedResult = applyMultiplier(-2L, TimeUnit.SECOND);
    assertEquals(expectedResult, eval(minusExpression));
  }

  @Test public void testEvaluateThrowsForUnsupportedIntervalType() {

    thrown.expect(IllegalArgumentException.class);

    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(
            SqlTypeName.INTERVAL_DAY_MINUTE,
            timestamp(DATE_MINUS_2_SEC),
            timestamp(DATE));

    eval(minusExpression);
  }

  private static BeamSqlDatetimeMinusExpression minusExpression(
      SqlTypeName intervalsToCount, BeamSqlExpression ... operands) {
    return new BeamSqlDatetimeMinusExpression(Arrays.asList(operands), intervalsToCount);
  }

  private BeamSqlExpression timestamp(Date date) {
    return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, date);
  }

  private long eval(BeamSqlDatetimeMinusExpression minusExpression) {
    return minusExpression.evaluate(NULL_ROW, NULL_WINDOW).getLong();
  }

  private long applyMultiplier(long value, TimeUnit timeUnit) {
    return value * timeUnit.multiplier.longValue();
  }
}
