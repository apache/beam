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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link BeamSqlDatetimeMinusIntervalExpression}. */
public class BeamSqlDatetimeMinusIntervalExpressionTest {
  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  private static final DateTime DATE = new DateTime(329281L);
  private static final DateTime DATE_MINUS_2_SEC = DATE.minusSeconds(2);

  private static final BeamSqlPrimitive DATETIME = BeamSqlPrimitive.of(SqlTypeName.DATE, DATE);

  private static final BeamSqlPrimitive INTERVAL_2_SEC =
      BeamSqlPrimitive.of(
          SqlTypeName.INTERVAL_SECOND, TimeUnit.SECOND.multiplier.multiply(new BigDecimal(2)));

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAcceptsHappyPath() {
    BeamSqlDatetimeMinusIntervalExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, DATETIME, INTERVAL_2_SEC);

    assertTrue(minusExpression.accept());
  }

  @Test
  public void testDoesNotAcceptWrongOutputType() {
    Set<SqlTypeName> unsupportedTypes = new HashSet<>(SqlTypeName.ALL_TYPES);
    unsupportedTypes.remove(SqlTypeName.TIMESTAMP);
    unsupportedTypes.remove(SqlTypeName.DATE);
    unsupportedTypes.remove(SqlTypeName.TIME);

    for (SqlTypeName unsupportedType : unsupportedTypes) {
      BeamSqlDatetimeMinusIntervalExpression minusExpression =
          minusExpression(unsupportedType, DATETIME, INTERVAL_2_SEC);

      assertFalse(minusExpression.accept());
    }
  }

  @Test
  public void testDoesNotAcceptWrongFirstOperand() {
    Set<SqlTypeName> unsupportedTypes = new HashSet<>(SqlTypeName.ALL_TYPES);
    unsupportedTypes.removeAll(SqlTypeName.DATETIME_TYPES);

    for (SqlTypeName unsupportedType : unsupportedTypes) {
      BeamSqlPrimitive unsupportedOperand = mock(BeamSqlPrimitive.class);
      doReturn(unsupportedType).when(unsupportedOperand).getOutputType();

      BeamSqlDatetimeMinusIntervalExpression minusExpression =
          minusExpression(SqlTypeName.TIMESTAMP, unsupportedOperand, INTERVAL_2_SEC);

      assertFalse(minusExpression.accept());
    }
  }

  @Test
  public void testDoesNotAcceptWrongSecondOperand() {
    Set<SqlTypeName> unsupportedTypes = new HashSet<>(SqlTypeName.ALL_TYPES);
    unsupportedTypes.removeAll(TimeUnitUtils.INTERVALS_DURATIONS_TYPES.keySet());

    for (SqlTypeName unsupportedType : unsupportedTypes) {
      BeamSqlPrimitive unsupportedOperand = mock(BeamSqlPrimitive.class);
      doReturn(unsupportedType).when(unsupportedOperand).getOutputType();

      BeamSqlDatetimeMinusIntervalExpression minusExpression =
          minusExpression(SqlTypeName.TIMESTAMP, DATETIME, unsupportedOperand);

      assertFalse(minusExpression.accept());
    }
  }

  @Test
  public void testAcceptsAllSupportedIntervalTypes() {
    for (SqlTypeName unsupportedType : TimeUnitUtils.INTERVALS_DURATIONS_TYPES.keySet()) {
      BeamSqlPrimitive unsupportedOperand = mock(BeamSqlPrimitive.class);
      doReturn(unsupportedType).when(unsupportedOperand).getOutputType();

      BeamSqlDatetimeMinusIntervalExpression minusExpression =
          minusExpression(SqlTypeName.TIMESTAMP, DATETIME, unsupportedOperand);

      assertTrue(minusExpression.accept());
    }
  }

  @Test
  public void testEvaluateHappyPath() {
    BeamSqlDatetimeMinusIntervalExpression minusExpression =
        minusExpression(SqlTypeName.TIMESTAMP, DATETIME, INTERVAL_2_SEC);

    BeamSqlPrimitive subtractionResult =
        minusExpression.evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty());

    assertEquals(SqlTypeName.TIMESTAMP, subtractionResult.getOutputType());
    assertEquals(DATE_MINUS_2_SEC, subtractionResult.getDate());
  }

  private static BeamSqlDatetimeMinusIntervalExpression minusExpression(
      SqlTypeName intervalsToCount, BeamSqlExpression... operands) {
    return new BeamSqlDatetimeMinusIntervalExpression(Arrays.asList(operands), intervalsToCount);
  }
}
