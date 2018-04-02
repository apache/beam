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
import org.junit.Test;

/**
 * Unit tests for {@link BeamSqlDatetimeMinusExpression}.
 */
public class BeamSqlDatetimeMinusExpressionTest {

  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  private static final DateTime DATE = new DateTime(329281L);
  private static final DateTime DATE_MINUS_2_SEC = DATE.minusSeconds(2);

  private static final BeamSqlPrimitive TIMESTAMP = BeamSqlPrimitive.of(
      SqlTypeName.TIMESTAMP, DATE);

  private static final BeamSqlPrimitive TIMESTAMP_MINUS_2_SEC = BeamSqlPrimitive.of(
      SqlTypeName.TIMESTAMP, DATE_MINUS_2_SEC);

  private static final BeamSqlPrimitive INTERVAL_2_SEC = BeamSqlPrimitive.of(
      SqlTypeName.INTERVAL_SECOND, TimeUnit.SECOND.multiplier.multiply(new BigDecimal(2)));

  private static final BeamSqlPrimitive STRING = BeamSqlPrimitive.of(
      SqlTypeName.VARCHAR, "hello");

  @Test public void testOutputType() {
    BeamSqlDatetimeMinusExpression minusExpression1 =
        minusExpression(SqlTypeName.TIMESTAMP, TIMESTAMP, INTERVAL_2_SEC);
    BeamSqlDatetimeMinusExpression minusExpression2 =
        minusExpression(SqlTypeName.BIGINT, TIMESTAMP, TIMESTAMP_MINUS_2_SEC);

    assertEquals(SqlTypeName.TIMESTAMP, minusExpression1.getOutputType());
    assertEquals(SqlTypeName.BIGINT, minusExpression2.getOutputType());
  }

  @Test public void testAcceptsTimestampMinusTimestamp() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.INTERVAL_SECOND, TIMESTAMP, TIMESTAMP_MINUS_2_SEC);

    assertTrue(minusExpression.accept());
  }

  @Test public void testAcceptsTimestampMinusInteval() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, TIMESTAMP, INTERVAL_2_SEC);

    assertTrue(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptUnsupportedReturnType() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.BIGINT, TIMESTAMP, INTERVAL_2_SEC);

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptUnsupportedFirstOperand() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, STRING, INTERVAL_2_SEC);

    assertFalse(minusExpression.accept());
  }

  @Test public void testDoesNotAcceptUnsupportedSecondOperand() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, TIMESTAMP, STRING);

    assertFalse(minusExpression.accept());
  }

  @Test public void testEvaluateTimestampMinusTimestamp() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.INTERVAL_SECOND, TIMESTAMP, TIMESTAMP_MINUS_2_SEC);

    BeamSqlPrimitive subtractionResult = minusExpression.evaluate(NULL_ROW, NULL_WINDOW);

    assertEquals(SqlTypeName.BIGINT, subtractionResult.getOutputType());
    assertEquals(2L * TimeUnit.SECOND.multiplier.longValue(), subtractionResult.getLong());
  }

  @Test public void testEvaluateTimestampMinusInteval() {
    BeamSqlDatetimeMinusExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, TIMESTAMP, INTERVAL_2_SEC);

    BeamSqlPrimitive subtractionResult = minusExpression.evaluate(NULL_ROW, NULL_WINDOW);

    assertEquals(SqlTypeName.TIMESTAMP, subtractionResult.getOutputType());
    assertEquals(DATE_MINUS_2_SEC, subtractionResult.getDate());
  }

  private static BeamSqlDatetimeMinusExpression minusExpression(
      SqlTypeName outputType, BeamSqlExpression ... operands) {
    return new BeamSqlDatetimeMinusExpression(Arrays.asList(operands), outputType);
  }
}
