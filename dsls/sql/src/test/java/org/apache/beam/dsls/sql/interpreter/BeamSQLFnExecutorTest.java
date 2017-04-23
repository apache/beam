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
package org.apache.beam.dsls.sql.interpreter;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlAndExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLessThanEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.rel.BeamFilterRel;
import org.apache.beam.dsls.sql.rel.BeamProjectRel;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test cases for {@link BeamSQLFnExecutor}.
 */
public class BeamSQLFnExecutorTest extends BeamSQLFnExecutorTestBase {

  @Test
  public void testBeamFilterRel() {
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        Arrays.asList(
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                Arrays.asList(rexBuilder.makeInputRef(relDataType, 0),
                    rexBuilder.makeBigintLiteral(new BigDecimal(1000L)))),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                Arrays.asList(rexBuilder.makeInputRef(relDataType, 1),
                    rexBuilder.makeExactLiteral(new BigDecimal(0))))));

    BeamFilterRel beamFilterRel = new BeamFilterRel(cluster, RelTraitSet.createEmpty(), null,
        condition);

    BeamSQLFnExecutor executor = new BeamSQLFnExecutor(beamFilterRel);
    executor.prepare();

    Assert.assertEquals(1, executor.exps.size());

    BeamSqlExpression l1Exp = executor.exps.get(0);
    Assert.assertTrue(l1Exp instanceof BeamSqlAndExpression);
    Assert.assertEquals(SqlTypeName.BOOLEAN, l1Exp.getOutputType());

    Assert.assertEquals(2, l1Exp.getOperands().size());
    BeamSqlExpression l1Left = (BeamSqlExpression) l1Exp.getOperands().get(0);
    BeamSqlExpression l1Right = (BeamSqlExpression) l1Exp.getOperands().get(1);

    Assert.assertTrue(l1Left instanceof BeamSqlLessThanEqualExpression);
    Assert.assertTrue(l1Right instanceof BeamSqlEqualExpression);

    Assert.assertEquals(2, l1Left.getOperands().size());
    BeamSqlExpression l1LeftLeft = (BeamSqlExpression) l1Left.getOperands().get(0);
    BeamSqlExpression l1LeftRight = (BeamSqlExpression) l1Left.getOperands().get(1);
    Assert.assertTrue(l1LeftLeft instanceof BeamSqlInputRefExpression);
    Assert.assertTrue(l1LeftRight instanceof BeamSqlPrimitive);

    Assert.assertEquals(2, l1Right.getOperands().size());
    BeamSqlExpression l1RightLeft = (BeamSqlExpression) l1Right.getOperands().get(0);
    BeamSqlExpression l1RightRight = (BeamSqlExpression) l1Right.getOperands().get(1);
    Assert.assertTrue(l1RightLeft instanceof BeamSqlInputRefExpression);
    Assert.assertTrue(l1RightRight instanceof BeamSqlPrimitive);
  }

  @Test
  public void testBeamProjectRel() {
    BeamRelNode relNode = new BeamProjectRel(cluster, RelTraitSet.createEmpty(),
        relBuilder.values(relDataType, 1234567L, 0, 8.9, null).build(),
        rexBuilder.identityProjects(relDataType), relDataType);
    BeamSQLFnExecutor executor = new BeamSQLFnExecutor(relNode);

    executor.prepare();
    Assert.assertEquals(4, executor.exps.size());
    Assert.assertTrue(executor.exps.get(0) instanceof BeamSqlInputRefExpression);
    Assert.assertTrue(executor.exps.get(1) instanceof BeamSqlInputRefExpression);
    Assert.assertTrue(executor.exps.get(2) instanceof BeamSqlInputRefExpression);
    Assert.assertTrue(executor.exps.get(3) instanceof BeamSqlInputRefExpression);
  }

}
