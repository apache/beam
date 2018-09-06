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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link BeamSqlDotExpression}. */
public class BeamSqlDotExpressionTest {

  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testReturnsFieldValue() {
    Schema schema = Schema.builder().addStringField("f_string").addInt32Field("f_int").build();

    List<BeamSqlExpression> elements =
        ImmutableList.of(
            BeamSqlPrimitive.of(
                SqlTypeName.ROW, Row.withSchema(schema).addValues("aaa", 14).build()),
            BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "f_string"));

    BeamSqlDotExpression arrayExpression = new BeamSqlDotExpression(elements, SqlTypeName.VARCHAR);

    assertEquals(
        "aaa",
        arrayExpression
            .evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty())
            .getValue());
  }

  @Test
  public void testThrowsForNonExistentField() {
    Schema schema = Schema.builder().addStringField("f_string").addInt32Field("f_int").build();

    List<BeamSqlExpression> elements =
        ImmutableList.of(
            BeamSqlPrimitive.of(
                SqlTypeName.ROW, Row.withSchema(schema).addValues("aaa", 14).build()),
            BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "f_nonExistent"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot find field");

    new BeamSqlDotExpression(elements, SqlTypeName.VARCHAR)
        .evaluate(NULL_ROW, NULL_WINDOW, BeamSqlExpressionEnvironments.empty());
  }
}
