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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for BeamSqlConcatExpression.
 */
public class BeamSqlConcatExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test public void accept() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"));
    assertTrue(new BeamSqlConcatExpression(operands).accept());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"));
    assertFalse(new BeamSqlConcatExpression(operands).accept());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"));
    assertFalse(new BeamSqlConcatExpression(operands).accept());
  }

  @Test public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " world"));
    Assert.assertEquals("hello world",
        new BeamSqlConcatExpression(operands).evaluate(row, null).getValue());
  }

}
