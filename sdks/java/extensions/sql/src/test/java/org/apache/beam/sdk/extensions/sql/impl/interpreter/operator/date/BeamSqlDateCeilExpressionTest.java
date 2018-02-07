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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@code BeamSqlDateCeilExpression}.
 */
public class BeamSqlDateCeilExpressionTest extends BeamSqlDateExpressionTestBase {
  @Test public void evaluate() throws Exception {
    List<BeamSqlExpression> operands = new ArrayList<>();
    operands.add(BeamSqlPrimitive.of(SqlTypeName.DATE,
        str2DateTime("2017-05-22 09:10:11")));
    // YEAR
    operands.add(BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.YEAR));
    Assert.assertEquals(str2DateTime("2018-01-01 00:00:00"),
        new BeamSqlDateCeilExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null).getDate());

    operands.set(1, BeamSqlPrimitive.of(SqlTypeName.SYMBOL, TimeUnitRange.MONTH));
    Assert.assertEquals(str2DateTime("2017-06-01 00:00:00"),
        new BeamSqlDateCeilExpression(operands)
            .evaluate(BeamSqlFnExecutorTestBase.row, null).getDate());
  }
}
