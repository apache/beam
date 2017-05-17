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

package org.apache.beam.dsls.sql.interpreter.operator.date;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * Test for {@code BeamSqlExtractExpression}.
 */
public class BeamSqlExtractExpressionTest extends BeamSqlDateExpressionTestBase {
  @Test public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();
    long time = str2LongTime("2017-05-22 16:17:18");

    // YEAR
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.YEAR));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(2017,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // MONTH
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.MONTH));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(5,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // DAY
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DAY));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(22,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // HOUR
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.HOUR));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(16,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // MINUTE
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.MINUTE));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(17,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // SECOND
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.SECOND));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(18,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // DAY_OF_WEEK
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DOW));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(2,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // DAY_OF_YEAR
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.DOY));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(142,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // WEEK
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.WEEK));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(21,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

    // QUARTER
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.QUARTER));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT,
        time));
    assertEquals(2,
        new BeamSqlExtractExpression(operands).evaluate(record).getValue());

  }
}
