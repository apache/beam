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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Unit tests for {@link BeamSqlSingleElementExpression}. */
public class BeamSqlSingleElementExpressionTest {

  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  @Test
  public void testReturnsSingleElement() {
    List<BeamSqlExpression> input =
        ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.ARRAY, Arrays.asList("aaa")));

    BeamSqlSingleElementExpression expression =
        new BeamSqlSingleElementExpression(input, SqlTypeName.VARCHAR);

    assertEquals(
        "aaa",
        expression
            .evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testReturnsNullForEmptyInput() {
    List<BeamSqlExpression> input =
        ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.ARRAY, Arrays.asList()));

    BeamSqlSingleElementExpression expression =
        new BeamSqlSingleElementExpression(input, SqlTypeName.VARCHAR);

    assertNull(
        expression
            .evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testAcceptsOneOperand() {
    List<BeamSqlExpression> input =
        ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.ARRAY, Arrays.asList("aaa")));

    BeamSqlSingleElementExpression expression =
        new BeamSqlSingleElementExpression(input, SqlTypeName.VARCHAR);

    assertTrue(expression.accept());
  }

  @Test
  public void testRejectsMoreThanOneOperand() {
    List<BeamSqlExpression> input =
        ImmutableList.of(
            BeamSqlPrimitive.of(SqlTypeName.ARRAY, Arrays.asList("aaa")),
            BeamSqlPrimitive.of(SqlTypeName.ARRAY, Arrays.asList("aaa")));

    BeamSqlSingleElementExpression expression =
        new BeamSqlSingleElementExpression(input, SqlTypeName.VARCHAR);

    assertFalse(expression.accept());
  }
}
