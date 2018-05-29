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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/** Test for BeamSqlTrimExpression. */
public class BeamSqlTrimExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void accept() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " hello "));
    assertTrue(new BeamSqlTrimExpression(operands).accept());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.BOTH));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"));
    assertTrue(new BeamSqlTrimExpression(operands).accept());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"));
    assertFalse(new BeamSqlTrimExpression(operands).accept());
  }

  @Test
  public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.LEADING));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"));
    Assert.assertEquals(
        "__hehe",
        new BeamSqlTrimExpression(operands).evaluate(row, null, ImmutableMap.of()).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.TRAILING));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"));
    Assert.assertEquals(
        "hehe__",
        new BeamSqlTrimExpression(operands).evaluate(row, null, ImmutableMap.of()).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.BOTH));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "__"));
    Assert.assertEquals(
        "__",
        new BeamSqlTrimExpression(operands).evaluate(row, null, ImmutableMap.of()).getValue());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " hello "));
    Assert.assertEquals(
        "hello",
        new BeamSqlTrimExpression(operands).evaluate(row, null, ImmutableMap.of()).getValue());
  }

  @Test
  public void leadingTrim() throws Exception {
    assertEquals("__hehe", BeamSqlTrimExpression.leadingTrim("hehe__hehe", "he"));
  }

  @Test
  public void trailingTrim() throws Exception {
    assertEquals("hehe__", BeamSqlTrimExpression.trailingTrim("hehe__hehe", "he"));
  }

  @Test
  public void trim() throws Exception {
    assertEquals(
        "__",
        BeamSqlTrimExpression.leadingTrim(
            BeamSqlTrimExpression.trailingTrim("hehe__hehe", "he"), "he"));
  }
}
