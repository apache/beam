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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.row;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link BeamSqlFieldAccessExpression}.
 */
public class BeamSqlFieldAccessExpressionTest {

  private static final Row NULL_ROW = null;
  private static final BoundedWindow NULL_WINDOW = null;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAccessesFieldOfArray() {
    BeamSqlPrimitive<List<String>> targetArray =
        BeamSqlPrimitive.of(
            SqlTypeName.ARRAY,
            Arrays.asList("aaa", "bbb", "ccc"));

    BeamSqlFieldAccessExpression arrayExpression =
        new BeamSqlFieldAccessExpression(targetArray, 1, SqlTypeName.VARCHAR);

    assertEquals("bbb", arrayExpression.evaluate(NULL_ROW, NULL_WINDOW).getValue());
  }

  @Test
  public void testAccessesFieldOfRow() {
    RowType rowType =
        RowSqlType
            .builder()
            .withVarcharField("f_string1")
            .withVarcharField("f_string2")
            .withVarcharField("f_string3")
            .build();

    BeamSqlPrimitive<Row> targetRow =
        BeamSqlPrimitive.of(
            SqlTypeName.ROW,
            Row
                .withRowType(rowType)
                .addValues("aa", "bb", "cc")
                .build());

    BeamSqlFieldAccessExpression arrayExpression =
        new BeamSqlFieldAccessExpression(targetRow, 1, SqlTypeName.VARCHAR);

    assertEquals("bb", arrayExpression.evaluate(NULL_ROW, NULL_WINDOW).getValue());
  }

  @Test
  public void testThrowsForUnsupportedType() {
    BeamSqlPrimitive<Integer> targetRow = BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("unsupported type");

    new BeamSqlFieldAccessExpression(targetRow, 1, SqlTypeName.VARCHAR)
        .evaluate(NULL_ROW, NULL_WINDOW).getValue();
  }
}
