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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCaseExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCastExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlDefaultExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlUdfExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlWindowEndExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlWindowExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlWindowStartExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlDivideExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlModExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlPlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlIsNotNullExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlIsNullExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlNotEqualsExpression;
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
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlAbsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlAcosExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlAsinExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlAtan2Expression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlAtanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlCeilExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlCosExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlCotExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlDegreesExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlExpExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlFloorExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlLnExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlLogExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlPiExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlPowerExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlRadiansExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlRandExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlRandIntegerExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlRoundExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlSignExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlSinExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlTanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math.BeamSqlTruncateExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret.BeamSqlReinterpretExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlCharLengthExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlConcatExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlInitCapExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlLowerExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlOverlayExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlPositionExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlSubstringExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlTrimExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string.BeamSqlUpperExpression;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamFilterRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamProjectRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NlsString;

/**
 * Executor based on {@link BeamSqlExpression} and {@link BeamSqlPrimitive}.
 * {@code BeamSqlFnExecutor} converts a {@link BeamRelNode} to a {@link BeamSqlExpression},
 * which can be evaluated against the {@link Row}.
 *
 */
public class BeamSqlFnExecutor implements BeamSqlExpressionExecutor {
  protected List<BeamSqlExpression> exps;

  public BeamSqlFnExecutor(BeamRelNode relNode) {
    this.exps = new ArrayList<>();
    if (relNode instanceof BeamFilterRel) {
      BeamFilterRel filterNode = (BeamFilterRel) relNode;
      RexNode condition = filterNode.getCondition();
      exps.add(buildExpression(condition));
    } else if (relNode instanceof BeamProjectRel) {
      BeamProjectRel projectNode = (BeamProjectRel) relNode;
      List<RexNode> projects = projectNode.getProjects();
      for (RexNode rexNode : projects) {
        exps.add(buildExpression(rexNode));
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported yet!", relNode.getClass().toString()));
    }
  }

  /**
   * {@link #buildExpression(RexNode)} visits the operands of {@link RexNode} recursively,
   * and represent each {@link SqlOperator} with a corresponding {@link BeamSqlExpression}.
   */
  static BeamSqlExpression buildExpression(RexNode rexNode) {
    BeamSqlExpression ret = null;
    if (rexNode instanceof RexLiteral) {
      RexLiteral node = (RexLiteral) rexNode;
      SqlTypeName type = node.getTypeName();
      Object value = node.getValue();

      if (SqlTypeName.CHAR_TYPES.contains(type)
          && node.getValue() instanceof NlsString) {
        // NlsString is not serializable, we need to convert
        // it to string explicitly.
        return BeamSqlPrimitive.of(type, ((NlsString) value).getValue());
      } else if (isDateNode(type, value)) {
        // does this actually make sense?
        // Calcite actually treat Calendar as the java type of Date Literal
        return BeamSqlPrimitive.of(type, ((Calendar) value).getTime());
      } else {
        // node.getType().getSqlTypeName() and node.getSqlTypeName() can be different
        // e.g. sql: "select 1"
        // here the literal 1 will be parsed as a RexLiteral where:
        //     node.getType().getSqlTypeName() = INTEGER (the display type)
        //     node.getSqlTypeName() = DECIMAL (the actual internal storage format)
        // So we need to do a convert here.
        // check RexBuilder#makeLiteral for more information.
        SqlTypeName realType = node.getType().getSqlTypeName();
        Object realValue = value;
        if (type == SqlTypeName.DECIMAL) {
          BigDecimal rawValue = (BigDecimal) value;
          switch (realType) {
            case TINYINT:
              realValue = (byte) rawValue.intValue();
              break;
            case SMALLINT:
              realValue = (short) rawValue.intValue();
              break;
            case INTEGER:
              realValue = rawValue.intValue();
              break;
            case BIGINT:
              realValue = rawValue.longValue();
              break;
            case DECIMAL:
              realValue = rawValue;
              break;
            default:
              throw new IllegalStateException("type/realType mismatch: "
                  + type + " VS " + realType);
          }
        } else if (type == SqlTypeName.DOUBLE) {
          Double rawValue = (Double) value;
          if (realType == SqlTypeName.FLOAT) {
            realValue = rawValue.floatValue();
          }
        }
        return BeamSqlPrimitive.of(realType, realValue);
      }
    } else if (rexNode instanceof RexInputRef) {
      RexInputRef node = (RexInputRef) rexNode;
      ret = new BeamSqlInputRefExpression(node.getType().getSqlTypeName(), node.getIndex());
    } else if (rexNode instanceof RexCall) {
      RexCall node = (RexCall) rexNode;
      String opName = node.op.getName();
      List<BeamSqlExpression> subExps = new ArrayList<>();
      for (RexNode subNode : node.getOperands()) {
        subExps.add(buildExpression(subNode));
      }
      switch (opName) {
        // logical operators
        case "AND":
          ret = new BeamSqlAndExpression(subExps);
          break;
        case "OR":
          ret = new BeamSqlOrExpression(subExps);
          break;
        case "NOT":
          ret = new BeamSqlNotExpression(subExps);
          break;
        case "=":
          ret = new BeamSqlEqualsExpression(subExps);
          break;
        case "<>":
          ret = new BeamSqlNotEqualsExpression(subExps);
          break;
        case ">":
          ret = new BeamSqlGreaterThanExpression(subExps);
          break;
        case ">=":
          ret = new BeamSqlGreaterThanOrEqualsExpression(subExps);
          break;
        case "<":
          ret = new BeamSqlLessThanExpression(subExps);
          break;
        case "<=":
          ret = new BeamSqlLessThanOrEqualsExpression(subExps);
          break;

        // arithmetic operators
        case "+":
          ret = new BeamSqlPlusExpression(subExps);
          break;
        case "-":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            ret = new BeamSqlMinusExpression(subExps);
          } else {
            ret = new BeamSqlDatetimeMinusExpression(subExps, node.type.getSqlTypeName());
          }
          break;
        case "*":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            ret = new BeamSqlMultiplyExpression(subExps);
          } else {
            ret = new BeamSqlIntervalMultiplyExpression(subExps);
          }
          break;
        case "/":
        case "/INT":
          ret = new BeamSqlDivideExpression(subExps);
          break;
        case "MOD":
          ret = new BeamSqlModExpression(subExps);
          break;

        case "ABS":
          ret = new BeamSqlAbsExpression(subExps);
          break;
        case "ROUND":
          ret = new BeamSqlRoundExpression(subExps);
          break;
        case "LN":
          ret = new BeamSqlLnExpression(subExps);
          break;
        case "LOG10":
          ret = new BeamSqlLogExpression(subExps);
          break;
        case "EXP":
          ret = new BeamSqlExpExpression(subExps);
          break;
        case "ACOS":
          ret = new BeamSqlAcosExpression(subExps);
          break;
        case "ASIN":
          ret = new BeamSqlAsinExpression(subExps);
          break;
        case "ATAN":
          ret = new BeamSqlAtanExpression(subExps);
          break;
        case "COT":
          ret = new BeamSqlCotExpression(subExps);
          break;
        case "DEGREES":
          ret = new BeamSqlDegreesExpression(subExps);
          break;
        case "RADIANS":
          ret = new BeamSqlRadiansExpression(subExps);
          break;
        case "COS":
          ret = new BeamSqlCosExpression(subExps);
          break;
        case "SIN":
          ret = new BeamSqlSinExpression(subExps);
          break;
        case "TAN":
          ret = new BeamSqlTanExpression(subExps);
          break;
        case "SIGN":
          ret = new BeamSqlSignExpression(subExps);
          break;
        case "POWER":
          ret = new BeamSqlPowerExpression(subExps);
          break;
        case "PI":
          ret = new BeamSqlPiExpression();
          break;
        case "ATAN2":
          ret = new BeamSqlAtan2Expression(subExps);
          break;
        case "TRUNCATE":
          ret = new BeamSqlTruncateExpression(subExps);
          break;
        case "RAND":
          ret = new BeamSqlRandExpression(subExps);
          break;
        case "RAND_INTEGER":
          ret = new BeamSqlRandIntegerExpression(subExps);
          break;

        // string operators
        case "||":
          ret = new BeamSqlConcatExpression(subExps);
          break;
        case "POSITION":
          ret = new BeamSqlPositionExpression(subExps);
          break;
        case "CHAR_LENGTH":
        case "CHARACTER_LENGTH":
          ret = new BeamSqlCharLengthExpression(subExps);
          break;
        case "UPPER":
          ret = new BeamSqlUpperExpression(subExps);
          break;
        case "LOWER":
          ret = new BeamSqlLowerExpression(subExps);
          break;
        case "TRIM":
          ret = new BeamSqlTrimExpression(subExps);
          break;
        case "SUBSTRING":
          ret = new BeamSqlSubstringExpression(subExps);
          break;
        case "OVERLAY":
          ret = new BeamSqlOverlayExpression(subExps);
          break;
        case "INITCAP":
          ret = new BeamSqlInitCapExpression(subExps);
          break;

        // date functions
        case "Reinterpret":
          return new BeamSqlReinterpretExpression(subExps, node.type.getSqlTypeName());
        case "CEIL":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            return new BeamSqlCeilExpression(subExps);
          } else {
            return new BeamSqlDateCeilExpression(subExps);
          }
        case "FLOOR":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            return new BeamSqlFloorExpression(subExps);
          } else {
            return new BeamSqlDateFloorExpression(subExps);
          }
        case "EXTRACT_DATE":
        case "EXTRACT":
          return new BeamSqlExtractExpression(subExps);

        case "LOCALTIME":
        case "CURRENT_TIME":
          return new BeamSqlCurrentTimeExpression(subExps);

        case "CURRENT_TIMESTAMP":
        case "LOCALTIMESTAMP":
          return new BeamSqlCurrentTimestampExpression(subExps);

        case "CURRENT_DATE":
          return new BeamSqlCurrentDateExpression();

        case "DATETIME_PLUS":
          return new BeamSqlDatetimePlusExpression(subExps);

        //DEFAULT keyword for UDF with optional parameter
        case "DEFAULT":
          return new BeamSqlDefaultExpression();

        case "CASE":
          ret = new BeamSqlCaseExpression(subExps);
          break;
        case "CAST":
          ret = new BeamSqlCastExpression(subExps, node.type.getSqlTypeName());
          break;

        case "IS NULL":
          ret = new BeamSqlIsNullExpression(subExps.get(0));
          break;
        case "IS NOT NULL":
          ret = new BeamSqlIsNotNullExpression(subExps.get(0));
          break;

        case "HOP":
        case "TUMBLE":
        case "SESSION":
          ret = new BeamSqlWindowExpression(subExps, node.type.getSqlTypeName());
          break;
        case "HOP_START":
        case "TUMBLE_START":
        case "SESSION_START":
          ret = new BeamSqlWindowStartExpression();
          break;
        case "HOP_END":
        case "TUMBLE_END":
        case "SESSION_END":
          ret = new BeamSqlWindowEndExpression();
          break;
        default:
          //handle UDF
          if (((RexCall) rexNode).getOperator() instanceof SqlUserDefinedFunction) {
            SqlUserDefinedFunction udf = (SqlUserDefinedFunction) ((RexCall) rexNode).getOperator();
            ScalarFunctionImpl fn = (ScalarFunctionImpl) udf.getFunction();
            ret = new BeamSqlUdfExpression(fn.method, subExps,
              ((RexCall) rexNode).type.getSqlTypeName());
        } else {
          throw new UnsupportedOperationException("Operator: " + opName + " is not supported yet!");
        }
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported yet!", rexNode.getClass().toString()));
    }

    if (ret != null && !ret.accept()) {
      throw new IllegalStateException(ret.getClass().getSimpleName()
          + " does not accept the operands.(" + rexNode + ")");
    }

    return ret;
  }

  private static boolean isDateNode(SqlTypeName type, Object value) {
    return (type == SqlTypeName.DATE || type == SqlTypeName.TIMESTAMP)
        && value instanceof Calendar;
  }

  @Override
  public void prepare() {
  }

  @Override
  public List<Object> execute(Row inputRow, BoundedWindow window) {
    List<Object> results = new ArrayList<>();
    for (BeamSqlExpression exp : exps) {
      results.add(exp.evaluate(inputRow, window).getValue());
    }
    return results;
  }

  @Override
  public void close() {
  }

}
