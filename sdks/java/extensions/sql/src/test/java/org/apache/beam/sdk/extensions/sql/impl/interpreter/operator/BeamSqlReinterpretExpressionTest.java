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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * Test for {@code BeamSqlReinterpretExpression}.
 */
public class BeamSqlReinterpretExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test public void accept() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    operands.add(BeamSqlPrimitive.of(SqlTypeName.DATE, new Date()));
    assertTrue(new BeamSqlReinterpretExpression(operands, SqlTypeName.BIGINT).accept());

    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, new Date()));
    assertTrue(new BeamSqlReinterpretExpression(operands, SqlTypeName.BIGINT).accept());

    operands.clear();
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.setTime(new Date());
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIME, calendar));
    assertTrue(new BeamSqlReinterpretExpression(operands, SqlTypeName.BIGINT).accept());

    // currently only support reinterpret DATE
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"));
    assertFalse(new BeamSqlReinterpretExpression(operands, SqlTypeName.BIGINT).accept());

    // currently only support convert to BIGINT
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIME, calendar));
    assertFalse(new BeamSqlReinterpretExpression(operands, SqlTypeName.TINYINT).accept());
  }

  @Test public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();

    Date d = new Date();
    d.setTime(1000);
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DATE, d));
    assertEquals(1000L, new BeamSqlReinterpretExpression(operands, SqlTypeName.BIGINT)
        .evaluate(record).getValue());
  }

}
