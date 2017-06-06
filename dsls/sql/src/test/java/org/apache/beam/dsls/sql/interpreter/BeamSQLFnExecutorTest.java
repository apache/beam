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

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlAndExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlCaseExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLessThanEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.interpreter.operator.arithmetic.BeamSqlDivideExpression;
import org.apache.beam.dsls.sql.interpreter.operator.arithmetic.BeamSqlMinusExpression;
import org.apache.beam.dsls.sql.interpreter.operator.arithmetic.BeamSqlModExpression;
import org.apache.beam.dsls.sql.interpreter.operator.arithmetic.BeamSqlMultiplyExpression;
import org.apache.beam.dsls.sql.interpreter.operator.arithmetic.BeamSqlPlusExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlCharLengthExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlConcatExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlInitCapExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlLowerExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlOverlayExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlPositionExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlSubstringExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlTrimExpression;
import org.apache.beam.dsls.sql.interpreter.operator.string.BeamSqlUpperExpression;
import org.apache.beam.dsls.sql.rel.BeamFilterRel;
import org.apache.beam.dsls.sql.rel.BeamProjectRel;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
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
    assertTrue(l1Exp instanceof BeamSqlAndExpression);
    Assert.assertEquals(SqlTypeName.BOOLEAN, l1Exp.getOutputType());

    Assert.assertEquals(2, l1Exp.getOperands().size());
    BeamSqlExpression l1Left = (BeamSqlExpression) l1Exp.getOperands().get(0);
    BeamSqlExpression l1Right = (BeamSqlExpression) l1Exp.getOperands().get(1);

    assertTrue(l1Left instanceof BeamSqlLessThanEqualExpression);
    assertTrue(l1Right instanceof BeamSqlEqualExpression);

    Assert.assertEquals(2, l1Left.getOperands().size());
    BeamSqlExpression l1LeftLeft = (BeamSqlExpression) l1Left.getOperands().get(0);
    BeamSqlExpression l1LeftRight = (BeamSqlExpression) l1Left.getOperands().get(1);
    assertTrue(l1LeftLeft instanceof BeamSqlInputRefExpression);
    assertTrue(l1LeftRight instanceof BeamSqlPrimitive);

    Assert.assertEquals(2, l1Right.getOperands().size());
    BeamSqlExpression l1RightLeft = (BeamSqlExpression) l1Right.getOperands().get(0);
    BeamSqlExpression l1RightRight = (BeamSqlExpression) l1Right.getOperands().get(1);
    assertTrue(l1RightLeft instanceof BeamSqlInputRefExpression);
    assertTrue(l1RightRight instanceof BeamSqlPrimitive);
  }

  @Test
  public void testBeamProjectRel() {
    BeamRelNode relNode = new BeamProjectRel(cluster, RelTraitSet.createEmpty(),
        relBuilder.values(relDataType, 1234567L, 0, 8.9, null).build(),
        rexBuilder.identityProjects(relDataType), relDataType);
    BeamSQLFnExecutor executor = new BeamSQLFnExecutor(relNode);

    executor.prepare();
    Assert.assertEquals(4, executor.exps.size());
    assertTrue(executor.exps.get(0) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(1) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(2) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(3) instanceof BeamSqlInputRefExpression);
  }


  @Test
  public void testBuildExpression_arithmetic() {
    testBuildArithmeticExpression(SqlStdOperatorTable.PLUS, BeamSqlPlusExpression.class);
    testBuildArithmeticExpression(SqlStdOperatorTable.MINUS, BeamSqlMinusExpression.class);
    testBuildArithmeticExpression(SqlStdOperatorTable.MULTIPLY, BeamSqlMultiplyExpression.class);
    testBuildArithmeticExpression(SqlStdOperatorTable.DIVIDE, BeamSqlDivideExpression.class);
    testBuildArithmeticExpression(SqlStdOperatorTable.MOD, BeamSqlModExpression.class);
  }

  private void testBuildArithmeticExpression(SqlOperator fn,
      Class<? extends BeamSqlExpression> clazz) {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(fn, Arrays.asList(
        rexBuilder.makeBigintLiteral(new BigDecimal(1L)),
        rexBuilder.makeBigintLiteral(new BigDecimal(1L))
    ));
    exp = BeamSQLFnExecutor.buildExpression(rexNode);

    assertTrue(exp.getClass().equals(clazz));
  }

  public void testBuildExpression_string()  {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT,
        Arrays.asList(
            rexBuilder.makeLiteral("hello "),
            rexBuilder.makeLiteral("world")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlConcatExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.POSITION,
        Arrays.asList(
            rexBuilder.makeLiteral("hello"),
            rexBuilder.makeLiteral("worldhello")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlPositionExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.POSITION,
        Arrays.asList(
            rexBuilder.makeLiteral("hello"),
            rexBuilder.makeLiteral("worldhello"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlPositionExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCharLengthExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.UPPER,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlUpperExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LOWER,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlLowerExpression);


    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.INITCAP,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlInitCapExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.TRIM,
        Arrays.asList(
            rexBuilder.makeLiteral("BOTH"),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlTrimExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlSubstringExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlSubstringExpression);


    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OVERLAY,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOverlayExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OVERLAY,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOverlayExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSQLFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCaseExpression);
  }
}
