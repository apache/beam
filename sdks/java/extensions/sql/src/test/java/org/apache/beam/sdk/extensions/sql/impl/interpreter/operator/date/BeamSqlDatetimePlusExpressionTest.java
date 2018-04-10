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

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for {@link BeamSqlDatetimePlusExpression}.
 */
public class BeamSqlDatetimePlusExpressionTest extends BeamSqlDateExpressionTestBase {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Row NULL_INPUT_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;
  private static final DateTime DATE = str2DateTime("1984-04-19 01:02:03");

  private static final DateTime DATE_PLUS_15_SECONDS = DATE.plusSeconds(15);
  private static final DateTime DATE_PLUS_10_MINUTES = DATE.plusMinutes(10);
  private static final DateTime DATE_PLUS_7_HOURS = DATE.plusHours(7);
  private static final DateTime DATE_PLUS_3_DAYS = DATE.plusDays(3);
  private static final DateTime DATE_PLUS_2_MONTHS = DATE.plusMonths(2);
  private static final DateTime DATE_PLUS_11_YEARS = DATE.plusYears(11);

  private static final BeamSqlExpression SQL_INTERVAL_15_SECONDS =
      interval(SqlTypeName.INTERVAL_SECOND, 15);
  private static final BeamSqlExpression SQL_INTERVAL_10_MINUTES =
      interval(SqlTypeName.INTERVAL_MINUTE, 10);
  private static final BeamSqlExpression SQL_INTERVAL_7_HOURS =
      interval(SqlTypeName.INTERVAL_HOUR, 7);
  private static final BeamSqlExpression SQL_INTERVAL_3_DAYS =
      interval(SqlTypeName.INTERVAL_DAY, 3);
  private static final BeamSqlExpression SQL_INTERVAL_2_MONTHS =
      interval(SqlTypeName.INTERVAL_MONTH, 2);
  private static final BeamSqlExpression SQL_INTERVAL_4_MONTHS =
      interval(SqlTypeName.INTERVAL_MONTH, 4);
  private static final BeamSqlExpression SQL_INTERVAL_11_YEARS =
      interval(SqlTypeName.INTERVAL_YEAR, 11);

  private static final BeamSqlExpression SQL_TIMESTAMP =
      BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, DATE);

  @Test public void testHappyPath_outputTypeAndAccept() {
    BeamSqlExpression plusExpression = dateTimePlus(SQL_TIMESTAMP, SQL_INTERVAL_3_DAYS);

    assertEquals(SqlTypeName.TIMESTAMP, plusExpression.getOutputType());
    assertTrue(plusExpression.accept());
  }

  @Test public void testDoesNotAcceptTreeOperands() {
    BeamSqlDatetimePlusExpression plusExpression =
        dateTimePlus(SQL_TIMESTAMP, SQL_INTERVAL_3_DAYS, SQL_INTERVAL_4_MONTHS);

    assertEquals(SqlTypeName.TIMESTAMP, plusExpression.getOutputType());
    assertFalse(plusExpression.accept());
  }

  @Test public void testDoesNotAcceptWithoutTimestampOperand() {
    BeamSqlDatetimePlusExpression plusExpression =
        dateTimePlus(SQL_INTERVAL_3_DAYS, SQL_INTERVAL_4_MONTHS);

    assertEquals(SqlTypeName.TIMESTAMP, plusExpression.getOutputType());
    assertFalse(plusExpression.accept());
  }

  @Test public void testDoesNotAcceptWithoutIntervalOperand() {
    BeamSqlDatetimePlusExpression plusExpression =
        dateTimePlus(SQL_TIMESTAMP, SQL_TIMESTAMP);

    assertEquals(SqlTypeName.TIMESTAMP, plusExpression.getOutputType());
    assertFalse(plusExpression.accept());
  }

  @Test public void testEvaluate() {
    assertEquals(DATE_PLUS_15_SECONDS, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_15_SECONDS));
    assertEquals(DATE_PLUS_10_MINUTES, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_10_MINUTES));
    assertEquals(DATE_PLUS_7_HOURS, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_7_HOURS));
    assertEquals(DATE_PLUS_3_DAYS, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_3_DAYS));
    assertEquals(DATE_PLUS_2_MONTHS, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_2_MONTHS));
    assertEquals(DATE_PLUS_11_YEARS, evalDatetimePlus(SQL_TIMESTAMP, SQL_INTERVAL_11_YEARS));
  }

  @Test public void testEvaluateThrowsForUnsupportedIntervalType() {
    thrown.expect(UnsupportedOperationException.class);

    BeamSqlPrimitive unsupportedInterval = BeamSqlPrimitive.of(SqlTypeName.INTERVAL_YEAR_MONTH, 3);
    evalDatetimePlus(SQL_TIMESTAMP, unsupportedInterval);
  }

  private static ReadableInstant evalDatetimePlus(
      BeamSqlExpression date, BeamSqlExpression interval) {
    return dateTimePlus(date, interval).evaluate(NULL_INPUT_ROW, NULL_WINDOW).getDate();
  }

  private static BeamSqlDatetimePlusExpression dateTimePlus(BeamSqlExpression ... operands) {
    return new BeamSqlDatetimePlusExpression(Arrays.asList(operands));
  }

  private static BeamSqlExpression interval(SqlTypeName type, int multiplier) {
    return BeamSqlPrimitive.of(type,
        timeUnitInternalMultiplier(type)
            .multiply(new BigDecimal(multiplier)));
  }

  private static BigDecimal timeUnitInternalMultiplier(final SqlTypeName sqlIntervalType) {
    switch (sqlIntervalType) {
      case INTERVAL_SECOND:
        return TimeUnit.SECOND.multiplier;
      case INTERVAL_MINUTE:
        return TimeUnit.MINUTE.multiplier;
      case INTERVAL_HOUR:
        return TimeUnit.HOUR.multiplier;
      case INTERVAL_DAY:
        return TimeUnit.DAY.multiplier;
      case INTERVAL_MONTH:
        return TimeUnit.MONTH.multiplier;
      case INTERVAL_YEAR:
        return TimeUnit.YEAR.multiplier;
      default:
        throw new IllegalArgumentException("Interval " + sqlIntervalType
            + " cannot be converted to TimeUnit");
    }
  }
}
