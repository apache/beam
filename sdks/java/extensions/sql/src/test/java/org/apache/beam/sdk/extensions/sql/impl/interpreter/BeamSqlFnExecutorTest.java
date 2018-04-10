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
package org.apache.beam.sdk.extensions.sql.impl.interpreter;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCaseExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlDivideExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlModExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlPlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentDateExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimestampExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDateCeilExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDateFloorExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimeMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimePlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlExtractExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlIntervalMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlAndExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlNotExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlOrExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlCharLengthExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlConcatExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlInitCapExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlLowerExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlOverlayExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlPositionExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlSubstringExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlTrimExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlUpperExpression;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamFilterRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamProjectRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test cases for {@link BeamSqlFnExecutor}.
 */
public class BeamSqlFnExecutorTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testBeamFilterRel() {
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        Arrays.asList(
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                Arrays.asList(rexBuilder.makeInputRef(relDataType, 0),
                    rexBuilder.makeBigintLiteral(new BigDecimal(1000L)))),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                Arrays.asList(rexBuilder.makeInputRef(relDataType, 1),
                    rexBuilder.makeExactLiteral(BigDecimal.ZERO)))));

    BeamFilterRel beamFilterRel = new BeamFilterRel(cluster, RelTraitSet.createEmpty(), null,
        condition);

    BeamSqlFnExecutor executor = new BeamSqlFnExecutor(beamFilterRel);
    executor.prepare();

    Assert.assertEquals(1, executor.exps.size());

    BeamSqlExpression l1Exp = executor.exps.get(0);
    assertTrue(l1Exp instanceof BeamSqlAndExpression);
    Assert.assertEquals(SqlTypeName.BOOLEAN, l1Exp.getOutputType());

    Assert.assertEquals(2, l1Exp.getOperands().size());
    BeamSqlExpression l1Left = (BeamSqlExpression) l1Exp.getOperands().get(0);
    BeamSqlExpression l1Right = (BeamSqlExpression) l1Exp.getOperands().get(1);

    assertTrue(l1Left instanceof BeamSqlLessThanOrEqualsExpression);
    assertTrue(l1Right instanceof BeamSqlEqualsExpression);

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
    BeamSqlFnExecutor executor = new BeamSqlFnExecutor(relNode);

    executor.prepare();
    Assert.assertEquals(4, executor.exps.size());
    assertTrue(executor.exps.get(0) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(1) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(2) instanceof BeamSqlInputRefExpression);
    assertTrue(executor.exps.get(3) instanceof BeamSqlInputRefExpression);
  }


  @Test
  public void testBuildExpression_logical() {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral(false)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlAndExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral(false)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOrExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT,
        Arrays.asList(
            rexBuilder.makeLiteral(true)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlNotExpression);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_logical_andOr_invalidOperand() {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral("hello")
        )
    );
    BeamSqlFnExecutor.buildExpression(rexNode);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_logical_not_invalidOperand() {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    BeamSqlFnExecutor.buildExpression(rexNode);
  }


  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_logical_not_invalidOperandCount() {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral(true)
        )
    );
    BeamSqlFnExecutor.buildExpression(rexNode);
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
        rexBuilder.makeBigintLiteral(BigDecimal.ONE),
        rexBuilder.makeBigintLiteral(BigDecimal.ONE)
    ));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);

    assertTrue(exp.getClass().equals(clazz));
  }

  @Test
  public void testBuildExpression_string()  {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT,
        Arrays.asList(
            rexBuilder.makeLiteral("hello "),
            rexBuilder.makeLiteral("world")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlConcatExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.POSITION,
        Arrays.asList(
            rexBuilder.makeLiteral("hello"),
            rexBuilder.makeLiteral("worldhello")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlPositionExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.POSITION,
        Arrays.asList(
            rexBuilder.makeLiteral("hello"),
            rexBuilder.makeLiteral("worldhello"),
            rexBuilder.makeCast(BeamQueryPlanner.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE))
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlPositionExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCharLengthExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.UPPER,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlUpperExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LOWER,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlLowerExpression);


    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.INITCAP,
        Arrays.asList(
            rexBuilder.makeLiteral("hello")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlInitCapExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.TRIM,
        Arrays.asList(
            rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlTrimExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlSubstringExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlSubstringExpression);


    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OVERLAY,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOverlayExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OVERLAY,
        Arrays.asList(
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
            rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOverlayExpression);

    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        Arrays.asList(
            rexBuilder.makeLiteral(true),
            rexBuilder.makeLiteral("HELLO"),
            rexBuilder.makeLiteral("HELLO")
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCaseExpression);
  }

  @Test
  public void testBuildExpression_date() {
    RexNode rexNode;
    BeamSqlExpression exp;
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
    calendar.setTime(new Date());

    // CEIL
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CEIL,
        Arrays.asList(
            rexBuilder.makeDateLiteral(calendar),
            rexBuilder.makeFlag(TimeUnitRange.MONTH)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDateCeilExpression);

    // FLOOR
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.FLOOR,
        Arrays.asList(
            rexBuilder.makeDateLiteral(calendar),
            rexBuilder.makeFlag(TimeUnitRange.MONTH)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDateFloorExpression);

    // EXTRACT == EXTRACT_DATE?
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
        Arrays.asList(
            rexBuilder.makeFlag(TimeUnitRange.MONTH),
            rexBuilder.makeDateLiteral(calendar)
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlExtractExpression);

    // CURRENT_DATE
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_DATE, Arrays.asList());
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCurrentDateExpression);

    // LOCALTIME
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME, Arrays.asList());
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCurrentTimeExpression);

    // LOCALTIMESTAMP
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIMESTAMP, Arrays.asList());
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlCurrentTimestampExpression);

    // DATETIME_PLUS
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS,
        Arrays.<RexNode>asList(
            rexBuilder.makeDateLiteral(calendar),
            rexBuilder.makeIntervalLiteral(
                BigDecimal.TEN,
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO))
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDatetimePlusExpression);

    // * for intervals
    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
        Arrays.<RexNode>asList(
            rexBuilder.makeExactLiteral(BigDecimal.ONE),
            rexBuilder.makeIntervalLiteral(
                BigDecimal.TEN,
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO))
        )
    );
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlIntervalMultiplyExpression);

    // minus for dates
    rexNode =
        rexBuilder.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.INTERVAL_DAY),
            SqlStdOperatorTable.MINUS,
            Arrays.asList(
                rexBuilder.makeTimestampLiteral(Calendar.getInstance(), 1000),
                rexBuilder.makeTimestampLiteral(Calendar.getInstance(), 1000)));

    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDatetimeMinusExpression);
  }
}
