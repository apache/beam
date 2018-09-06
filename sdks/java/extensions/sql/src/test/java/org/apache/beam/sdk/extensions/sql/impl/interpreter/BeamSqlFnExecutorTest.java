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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlDivideExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlModExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlPlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentDateExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimestampExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimeMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimePlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlIntervalMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlAndExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlNotExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlOrExpression;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Unit test cases for {@link BeamSqlFnExecutor}. */
public class BeamSqlFnExecutorTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testBuildExpression_logical() {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            Arrays.asList(rexBuilder.makeLiteral(true), rexBuilder.makeLiteral(false)));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlAndExpression);

    rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            Arrays.asList(rexBuilder.makeLiteral(true), rexBuilder.makeLiteral(false)));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlOrExpression);

    rexNode =
        rexBuilder.makeCall(SqlStdOperatorTable.NOT, Arrays.asList(rexBuilder.makeLiteral(true)));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlNotExpression);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_logical_andOr_invalidOperand() {
    RexNode rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            Arrays.asList(rexBuilder.makeLiteral(true), rexBuilder.makeLiteral("hello")));
    BeamSqlFnExecutor.buildExpression(rexNode);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_logical_not_invalidOperand() {
    RexNode rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT, Arrays.asList(rexBuilder.makeLiteral("hello")));
    BeamSqlFnExecutor.buildExpression(rexNode);
  }

  @Test
  public void testBuildExpression_literal() {
    RelDataType realType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);

    RexLiteral rexNode = RexLiteral.fromJdbcString(realType, SqlTypeName.DOUBLE, "1.0");
    BeamSqlExpression exp = BeamSqlFnExecutor.buildExpression(rexNode);

    assertTrue(exp instanceof BeamSqlPrimitive);
    assertEquals(SqlTypeName.INTEGER, exp.getOutputType());
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildExpression_literal_mismatch_type() {
    RelDataType realType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR);

    RexLiteral rexNode = RexLiteral.fromJdbcString(realType, SqlTypeName.DOUBLE, "1.0");
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

  private void testBuildArithmeticExpression(
      SqlOperator fn, Class<? extends BeamSqlExpression> clazz) {
    RexNode rexNode;
    BeamSqlExpression exp;
    rexNode =
        rexBuilder.makeCall(
            fn,
            Arrays.asList(
                rexBuilder.makeBigintLiteral(BigDecimal.ONE),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE)));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);

    assertTrue(exp.getClass().equals(clazz));
  }

  @Test
  public void testBuildExpression_date() {
    RexNode rexNode;
    BeamSqlExpression exp;
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
    calendar.setTime(new Date());

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
    rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DATETIME_PLUS,
            Arrays.<RexNode>asList(
                rexBuilder.makeDateLiteral(calendar),
                rexBuilder.makeIntervalLiteral(
                    BigDecimal.TEN,
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO))));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDatetimePlusExpression);

    // * for intervals
    rexNode =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            Arrays.<RexNode>asList(
                rexBuilder.makeExactLiteral(BigDecimal.ONE),
                rexBuilder.makeIntervalLiteral(
                    BigDecimal.TEN,
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO))));
    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlIntervalMultiplyExpression);

    // minus for dates
    rexNode =
        rexBuilder.makeCall(
            TYPE_FACTORY.createSqlIntervalType(
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO)),
            SqlStdOperatorTable.MINUS,
            Arrays.asList(
                rexBuilder.makeTimestampLiteral(Calendar.getInstance(), 1000),
                rexBuilder.makeTimestampLiteral(Calendar.getInstance(), 1000)));

    exp = BeamSqlFnExecutor.buildExpression(rexNode);
    assertTrue(exp instanceof BeamSqlDatetimeMinusExpression);
  }
}
