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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.junit.Test;

/** Test for {@code BeamSqlExtractExpression}. */
public class BeamSqlExtractExpressionTest extends BeamSqlDateExpressionTestBase {
  @Test
  public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();
    DateTime time = str2DateTime("2017-05-22 16:17:18");

    // YEAR
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.YEAR));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        2017L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // MONTH
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.MONTH));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        5L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // DAY
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DAY));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        22L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // DAY_OF_WEEK
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DOW));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        2L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // DAY_OF_YEAR
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DOY));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        142L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // WEEK
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.WEEK));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        21L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());

    // QUARTER
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.QUARTER));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, time));
    assertEquals(
        2L,
        new BeamSqlExtractExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null, ImmutableMap.of())
            .getValue());
  }
}
