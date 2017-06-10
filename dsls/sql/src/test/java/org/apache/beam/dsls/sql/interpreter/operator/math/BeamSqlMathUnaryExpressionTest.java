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

package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for {@link BeamSqlMathUnaryExpression}.
 */
public class BeamSqlMathUnaryExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test public void testForGreaterThanOneOperands() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // operands more than 1 not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4));
    Assert.assertFalse(new BeamSqlAbsExpression(operands).accept());
    Assert.assertFalse(new BeamSqlSqrtExpression(operands).accept());
  }

  @Test public void testForOperandsType() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // varchar operand not allowed
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2"));
    Assert.assertFalse(new BeamSqlAbsExpression(operands).accept());
    Assert.assertFalse(new BeamSqlSqrtExpression(operands).accept());
  }

  @Test public void testForUnaryExpressions() {
    List<BeamSqlExpression> operands = new ArrayList<>();

    // test for sqrt function
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SMALLINT, Short.valueOf("2")));
    Assert.assertEquals(1.4142135623730951,
        new BeamSqlSqrtExpression(operands).evaluate(record).getValue());

    // test for abs function
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BIGINT, -28965734597L));
    Assert
        .assertEquals(28965734597L, new BeamSqlAbsExpression(operands).evaluate(record).getValue());
  }

}
