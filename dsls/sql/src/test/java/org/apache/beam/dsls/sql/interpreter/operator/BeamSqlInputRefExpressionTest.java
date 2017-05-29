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

import org.apache.beam.dsls.sql.exception.BeamInvalidOperatorException;
import org.apache.beam.dsls.sql.interpreter.BeamSQLFnExecutorTestBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link BeamSqlInputRefExpression}.
 */
public class BeamSqlInputRefExpressionTest extends BeamSQLFnExecutorTestBase {

  @Test
  public void testRefInRange() {
    BeamSqlInputRefExpression ref0 = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 0);
    Assert.assertEquals(record.getLong(0), ref0.evaluate(record).getValue());

    BeamSqlInputRefExpression ref1 = new BeamSqlInputRefExpression(SqlTypeName.INTEGER, 1);
    Assert.assertEquals(record.getInteger(1), ref1.evaluate(record).getValue());

    BeamSqlInputRefExpression ref2 = new BeamSqlInputRefExpression(SqlTypeName.DOUBLE, 2);
    Assert.assertEquals(record.getDouble(2), ref2.evaluate(record).getValue());

    BeamSqlInputRefExpression ref3 = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 3);
    Assert.assertEquals(record.getLong(3), ref3.evaluate(record).getValue());
  }


  @Test(expected = IndexOutOfBoundsException.class)
  public void testRefOutOfRange(){
    BeamSqlInputRefExpression ref = new BeamSqlInputRefExpression(SqlTypeName.BIGINT, 4);
    ref.evaluate(record).getValue();
  }

  @Test(expected = BeamInvalidOperatorException.class)
  public void testTypeUnMatch(){
    BeamSqlInputRefExpression ref = new BeamSqlInputRefExpression(SqlTypeName.INTEGER, 0);
    ref.evaluate(record).getValue();
  }
}
