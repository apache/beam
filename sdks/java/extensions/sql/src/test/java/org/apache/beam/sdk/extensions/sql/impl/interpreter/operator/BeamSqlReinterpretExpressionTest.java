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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret.BeamSqlReinterpretExpression;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.junit.Test;

/** Test for {@code BeamSqlReinterpretExpression}. */
public class BeamSqlReinterpretExpressionTest extends BeamSqlFnExecutorTestBase {
  private static final long DATE_LONG = 1000L;
  private static final DateTime DATE = new DateTime(DATE_LONG);
  private static final DateTime TIME = new DateTime().withDate(2019, 8, 9);

  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  private static final BeamSqlExpression DATE_PRIMITIVE =
      BeamSqlPrimitive.of(SqlTypeName.DATE, DATE);

  private static final BeamSqlExpression TIME_PRIMITIVE =
      BeamSqlPrimitive.of(SqlTypeName.TIME, TIME);

  private static final BeamSqlExpression TIMESTAMP_PRIMITIVE =
      BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, DATE);

  private static final BeamSqlExpression TINYINT_PRIMITIVE_5 =
      BeamSqlPrimitive.of(SqlTypeName.TINYINT, (byte) 5);

  private static final BeamSqlExpression SMALLINT_PRIMITIVE_6 =
      BeamSqlPrimitive.of(SqlTypeName.SMALLINT, (short) 6);

  private static final BeamSqlExpression INTEGER_PRIMITIVE_8 =
      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 8);

  private static final BeamSqlExpression BIGINT_PRIMITIVE_15 =
      BeamSqlPrimitive.of(SqlTypeName.BIGINT, 15L);

  private static final BeamSqlExpression VARCHAR_PRIMITIVE =
      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello");

  @Test
  public void testAcceptsDateTypes() throws Exception {
    assertTrue(reinterpretExpression(DATE_PRIMITIVE).accept());
    assertTrue(reinterpretExpression(TIMESTAMP_PRIMITIVE).accept());
  }

  @Test
  public void testAcceptsTime() {
    assertTrue(reinterpretExpression(TIME_PRIMITIVE).accept());
  }

  @Test
  public void testAcceptsIntTypes() {
    assertTrue(reinterpretExpression(TINYINT_PRIMITIVE_5).accept());
    assertTrue(reinterpretExpression(SMALLINT_PRIMITIVE_6).accept());
    assertTrue(reinterpretExpression(INTEGER_PRIMITIVE_8).accept());
    assertTrue(reinterpretExpression(BIGINT_PRIMITIVE_15).accept());
  }

  @Test
  public void testDoesNotAcceptUnsupportedType() {
    assertFalse(reinterpretExpression(VARCHAR_PRIMITIVE).accept());
  }

  @Test
  public void testHasCorrectOutputType() {
    BeamSqlReinterpretExpression reinterpretExpression1 =
        new BeamSqlReinterpretExpression(Arrays.asList(DATE_PRIMITIVE), SqlTypeName.BIGINT);
    assertEquals(SqlTypeName.BIGINT, reinterpretExpression1.getOutputType());

    BeamSqlReinterpretExpression reinterpretExpression2 =
        new BeamSqlReinterpretExpression(Arrays.asList(DATE_PRIMITIVE), SqlTypeName.INTERVAL_YEAR);
    assertEquals(SqlTypeName.INTERVAL_YEAR, reinterpretExpression2.getOutputType());
  }

  @Test
  public void evaluateDate() {
    assertEquals(DATE_LONG, evaluateReinterpretExpression(DATE_PRIMITIVE));
    assertEquals(DATE_LONG, evaluateReinterpretExpression(TIMESTAMP_PRIMITIVE));
  }

  @Test
  public void evaluateTime() {
    assertEquals(TIME.getMillis(), evaluateReinterpretExpression(TIME_PRIMITIVE));
  }

  @Test
  public void evaluateInts() {
    assertEquals(5L, evaluateReinterpretExpression(TINYINT_PRIMITIVE_5));
    assertEquals(6L, evaluateReinterpretExpression(SMALLINT_PRIMITIVE_6));
    assertEquals(8L, evaluateReinterpretExpression(INTEGER_PRIMITIVE_8));
    assertEquals(15L, evaluateReinterpretExpression(BIGINT_PRIMITIVE_15));
  }

  private static long evaluateReinterpretExpression(BeamSqlExpression operand) {
    return reinterpretExpression(operand)
        .evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty())
        .getLong();
  }

  private static BeamSqlReinterpretExpression reinterpretExpression(BeamSqlExpression... operands) {
    return new BeamSqlReinterpretExpression(Arrays.asList(operands), SqlTypeName.BIGINT);
  }
}
