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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlAndExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlOrExpression;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for {@link BeamSqlAndExpression}, {@link BeamSqlOrExpression}. */
public class BeamSqlAndOrExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testAnd() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true));

    Assert.assertTrue(
        new BeamSqlAndExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, false));

    Assert.assertFalse(
        new BeamSqlAndExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testOr() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, false));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, false));

    Assert.assertFalse(
        new BeamSqlOrExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());

    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true));

    Assert.assertTrue(
        new BeamSqlOrExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }
}
