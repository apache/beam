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

package org.apache.beam.dsls.sql.interpreter.operator;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.beam.dsls.sql.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BeamSqlCastExpression}.
 */
public class BeamSqlCastExpressionTest extends BeamSqlFnExecutorTestBase {

  @Test public void testForOperands() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1));
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "aaa"));

    Assert.assertFalse(new BeamSqlCastExpression(operands, SqlTypeName.BIGINT).accept());
  }

  @Test public void testForTypeCasting() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5));

    Assert.assertEquals(5L,
        new BeamSqlCastExpression(operands, SqlTypeName.BIGINT).evaluate(record).getLong());
    // test for double to bigint cast
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 5.45));
    Assert.assertEquals(5L,
        new BeamSqlCastExpression(operands, SqlTypeName.BIGINT).evaluate(record).getLong());
  }

  @Test public void testForDateCast() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    // test for yyyyMMdd format
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 20170521));
    Assert.assertEquals(Date.valueOf("2017-05-21"),
        new BeamSqlCastExpression(operands, SqlTypeName.DATE).evaluate(record).getValue());
    //test for yyyy-MM-dd format
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2017-05-21"));
    Assert.assertEquals(Date.valueOf("2017-05-21"),
        new BeamSqlCastExpression(operands, SqlTypeName.DATE).evaluate(record).getValue());
    // test for yy.MM.dd format
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "17.05.21"));
    Assert.assertEquals(Date.valueOf("2017-05-21"),
        new BeamSqlCastExpression(operands, SqlTypeName.DATE).evaluate(record).getValue());
  }

  @Test public void testForOutputTypeForCastExpression() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 20170521));
    Assert.assertEquals(SqlTypeName.DATE,
        new BeamSqlCastExpression(operands, SqlTypeName.DATE).evaluate(record).getOutputType());
    // second test for timestamp type
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "17-05-21 23:59:59.989"));
    Assert.assertEquals(SqlTypeName.TIMESTAMP,
        new BeamSqlCastExpression(operands, SqlTypeName.TIMESTAMP).evaluate(record)
            .getOutputType());
  }

  @Test public void testForDateTimeCast() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2017-05-21 23:59:59.989"));
    Assert.assertEquals(Timestamp.valueOf("2017-05-22 00:00:00.0"),
        new BeamSqlCastExpression(operands, SqlTypeName.TIMESTAMP).evaluate(record).getValue());
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2017-05-21 23:59:59.89079 PST"));
    Assert.assertEquals(Timestamp.valueOf("2017-05-22 00:00:00.0"),
        new BeamSqlCastExpression(operands, SqlTypeName.TIMESTAMP).evaluate(record).getValue());
    operands.clear();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "2017-05-21 23:59:59"));
    Assert.assertEquals(Timestamp.valueOf("2017-05-21 23:59:59"),
        new BeamSqlCastExpression(operands, SqlTypeName.TIMESTAMP).evaluate(record).getValue());
  }

  @Test(expected = RuntimeException.class) public void testForCastTypeNotSupported() {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.TIME, Calendar.getInstance().getTime()));
    Assert.assertEquals(Timestamp.valueOf("2017-05-22 00:00:00.0"),
        new BeamSqlCastExpression(operands, SqlTypeName.TIMESTAMP).evaluate(record).getValue());
  }

}
