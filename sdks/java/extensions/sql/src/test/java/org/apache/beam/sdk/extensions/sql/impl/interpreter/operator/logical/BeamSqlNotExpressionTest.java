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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@code BeamSqlNotExpression}. */
public class BeamSqlNotExpressionTest extends BeamSqlFnExecutorTestBase {
  @Test
  public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, false));
    Assert.assertTrue(
        new BeamSqlNotExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getBoolean());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true));
    Assert.assertFalse(
        new BeamSqlNotExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getBoolean());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, null));
    Assert.assertNull(
        new BeamSqlNotExpression(operands)
            .evaluate(row, null, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }
}
