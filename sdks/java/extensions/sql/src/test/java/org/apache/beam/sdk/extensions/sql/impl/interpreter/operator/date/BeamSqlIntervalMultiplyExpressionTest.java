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

import static org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.TimeUnitUtils.timeUnitInternalMultiplier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Test for BeamSqlIntervalMultiplyExpression. */
public class BeamSqlIntervalMultiplyExpressionTest {
  private static final Row NULL_INPUT_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;
  private static final BigDecimal DECIMAL_THREE = new BigDecimal(3);
  private static final BigDecimal DECIMAL_FOUR = new BigDecimal(4);

  private static final BeamSqlExpression SQL_INTERVAL_DAY =
      BeamSqlPrimitive.of(SqlTypeName.INTERVAL_DAY, DECIMAL_THREE);

  private static final BeamSqlExpression SQL_INTERVAL_MONTH =
      BeamSqlPrimitive.of(SqlTypeName.INTERVAL_MONTH, DECIMAL_FOUR);

  private static final BeamSqlExpression SQL_INTEGER_FOUR =
      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4);

  private static final BeamSqlExpression SQL_INTEGER_FIVE =
      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5);

  @Test
  public void testHappyPath_outputTypeAndAccept() {
    BeamSqlExpression multiplyExpression =
        newMultiplyExpression(SQL_INTERVAL_DAY, SQL_INTEGER_FOUR);

    assertEquals(SqlTypeName.INTERVAL_DAY, multiplyExpression.getOutputType());
    assertTrue(multiplyExpression.accept());
  }

  @Test
  public void testDoesNotAcceptTreeOperands() {
    BeamSqlIntervalMultiplyExpression multiplyExpression =
        newMultiplyExpression(SQL_INTERVAL_DAY, SQL_INTEGER_FIVE, SQL_INTEGER_FOUR);

    assertEquals(SqlTypeName.INTERVAL_DAY, multiplyExpression.getOutputType());
    assertFalse(multiplyExpression.accept());
  }

  @Test
  public void testDoesNotAcceptWithoutIntervalOperand() {
    BeamSqlIntervalMultiplyExpression multiplyExpression =
        newMultiplyExpression(SQL_INTEGER_FOUR, SQL_INTEGER_FIVE);

    assertNull(multiplyExpression.getOutputType());
    assertFalse(multiplyExpression.accept());
  }

  @Test
  public void testDoesNotAcceptWithoutIntegerOperand() {
    BeamSqlIntervalMultiplyExpression multiplyExpression =
        newMultiplyExpression(SQL_INTERVAL_DAY, SQL_INTERVAL_MONTH);

    assertEquals(SqlTypeName.INTERVAL_DAY, multiplyExpression.getOutputType());
    assertFalse(multiplyExpression.accept());
  }

  @Test
  public void testEvaluate_integerOperand() {
    BeamSqlIntervalMultiplyExpression multiplyExpression =
        newMultiplyExpression(SQL_INTERVAL_DAY, SQL_INTEGER_FOUR);

    BeamSqlPrimitive multiplicationResult =
        multiplyExpression.evaluate(
            NULL_INPUT_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty());

    BigDecimal expectedResult =
        DECIMAL_FOUR.multiply(timeUnitInternalMultiplier(SqlTypeName.INTERVAL_DAY));

    assertEquals(expectedResult, multiplicationResult.getDecimal());
    assertEquals(SqlTypeName.INTERVAL_DAY, multiplicationResult.getOutputType());
  }

  private BeamSqlIntervalMultiplyExpression newMultiplyExpression(BeamSqlExpression... operands) {
    return new BeamSqlIntervalMultiplyExpression(Arrays.asList(operands));
  }
}
