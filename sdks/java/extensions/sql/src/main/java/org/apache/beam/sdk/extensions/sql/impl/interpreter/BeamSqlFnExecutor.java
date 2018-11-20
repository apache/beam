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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.UdfImpl;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCaseExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCastExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlCorrelVariableExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlDefaultExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlDotExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlLocalRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlOperatorExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlUdfExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.DateOperators;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.StringOperators;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlDivideExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlModExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic.BeamSqlPlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.array.BeamSqlArrayExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.array.BeamSqlArrayItemExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.collection.BeamSqlCardinalityExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.collection.BeamSqlSingleElementExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlGreaterThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlIsNotNullExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlIsNullExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLessThanOrEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlLikeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlNotEqualsExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison.BeamSqlNotLikeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentDateExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimeExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlCurrentTimestampExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimeMinusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimePlusExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlIntervalMultiplyExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlAndExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlNotExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical.BeamSqlOrExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.map.BeamSqlMapExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.map.BeamSqlMapItemExpression;
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
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.row.BeamSqlFieldAccessExpression;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

/**
 * Executor based on {@link BeamSqlExpression} and {@link BeamSqlPrimitive}. {@code
 * BeamSqlFnExecutor} converts a {@link BeamRelNode} to a {@link BeamSqlExpression}, which can be
 * evaluated against the {@link Row}.
 */
public class BeamSqlFnExecutor implements BeamSqlExpressionExecutor {
  private List<BeamSqlExpression> exprs;
  private BeamSqlExpression filterCondition;
  private List<BeamSqlExpression> projections;

  public BeamSqlFnExecutor(RexProgram program) {
    this.exprs =
        program
            .getExprList()
            .stream()
            .map(BeamSqlFnExecutor::buildExpression)
            .collect(Collectors.toList());

    this.filterCondition =
        program.getCondition() == null
            ? BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true)
            : buildExpression(program.getCondition());

    this.projections =
        program
            .getProjectList()
            .stream()
            .map(BeamSqlFnExecutor::buildExpression)
            .collect(Collectors.toList());
  }

  /**
   * {@link #buildExpression(RexNode)} visits the operands of {@link RexNode} recursively, and
   * represent each {@link SqlOperator} with a corresponding {@link BeamSqlExpression}.
   */
  static BeamSqlExpression buildExpression(RexNode rexNode) {
    BeamSqlExpression ret = getBeamSqlExpression(rexNode);

    if (!ret.accept()) {
      throw new IllegalStateException(
          ret.getClass().getSimpleName() + " does not accept the operands.(" + rexNode + ")");
    }

    return ret;
  }

  private static BeamSqlExpression getBeamSqlExpression(RexNode rexNode) {
    BeamSqlExpression ret;
    if (rexNode instanceof RexLiteral) {
      RexLiteral node = (RexLiteral) rexNode;
      SqlTypeName type = node.getTypeName();
      Object value = node.getValue();

      if (SqlTypeName.CHAR_TYPES.contains(type) && node.getValue() instanceof NlsString) {
        // NlsString is not serializable, we need to convert
        // it to string explicitly.
        ret = BeamSqlPrimitive.of(type, ((NlsString) value).getValue());
      } else if (isDateNode(type, value)) {
        ret = BeamSqlPrimitive.of(type, new DateTime(value));
      } else {
        // node.getTypeName().getSqlTypeName() and node.getSqlTypeName() can be different
        // e.g. sql: "select 1"
        // here the literal 1 will be parsed as a RexLiteral where:
        //     node.getTypeName().getSqlTypeName() = INTEGER (the display type)
        //     node.getSqlTypeName() = DECIMAL (the actual internal storage format)
        // So we need to do a convert here.
        // check RexBuilder#makeLiteral for more information.

        SqlTypeName realType = node.getType().getSqlTypeName();
        Object realValue = value;

        if (SqlTypeName.NUMERIC_TYPES.contains(type)) {
          switch (realType) {
            case TINYINT:
              realValue = SqlFunctions.toByte(value);
              break;
            case SMALLINT:
              realValue = SqlFunctions.toShort(value);
              break;
            case INTEGER:
              realValue = SqlFunctions.toInt(value);
              break;
            case BIGINT:
              realValue = SqlFunctions.toLong(value);
              break;
            case FLOAT:
              realValue = SqlFunctions.toFloat(value);
              break;
            case DOUBLE:
              realValue = SqlFunctions.toDouble(value);
              break;
            case DECIMAL:
              realValue = SqlFunctions.toBigDecimal(value);
              break;
            default:
              throw new IllegalStateException(
                  "Unsupported conversion: Attempted convert node "
                      + node.toString()
                      + " of type "
                      + type
                      + "to "
                      + realType);
          }
        }

        ret = BeamSqlPrimitive.of(realType, realValue);
      }
    } else if (rexNode instanceof RexInputRef) {
      RexInputRef node = (RexInputRef) rexNode;
      ret = new BeamSqlInputRefExpression(node.getType().getSqlTypeName(), node.getIndex());
    } else if (rexNode instanceof RexCorrelVariable) {
      RexCorrelVariable correlVariable = (RexCorrelVariable) rexNode;
      ret =
          new BeamSqlCorrelVariableExpression(
              correlVariable.getType().getSqlTypeName(), correlVariable.id.getId());
    } else if (rexNode instanceof RexLocalRef) {
      RexLocalRef localRef = (RexLocalRef) rexNode;
      ret = new BeamSqlLocalRefExpression(localRef.getType().getSqlTypeName(), localRef.getIndex());
    } else if (rexNode instanceof RexFieldAccess) {
      RexFieldAccess fieldAccessNode = (RexFieldAccess) rexNode;
      BeamSqlExpression referenceExpression = buildExpression(fieldAccessNode.getReferenceExpr());
      int nestedFieldIndex = fieldAccessNode.getField().getIndex();
      SqlTypeName nestedFieldType = fieldAccessNode.getField().getType().getSqlTypeName();

      ret =
          new BeamSqlFieldAccessExpression(referenceExpression, nestedFieldIndex, nestedFieldType);
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
        case "LIKE":
          ret = new BeamSqlLikeExpression(subExps);
          break;
        case "NOT LIKE":
          ret = new BeamSqlNotLikeExpression(subExps);
          break;
          // arithmetic operators
        case "+":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            ret = new BeamSqlPlusExpression(subExps);
          } else {
            ret = new BeamSqlDatetimePlusExpression(subExps);
          }
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
          ret = new BeamSqlOperatorExpression(StringOperators.CONCAT, subExps);
          break;
        case "POSITION":
          ret = new BeamSqlOperatorExpression(StringOperators.POSITION, subExps);
          break;
        case "CHAR_LENGTH":
        case "CHARACTER_LENGTH":
          ret = new BeamSqlOperatorExpression(StringOperators.CHAR_LENGTH, subExps);
          break;
        case "UPPER":
          ret = new BeamSqlOperatorExpression(StringOperators.UPPER, subExps);
          break;
        case "LOWER":
          ret = new BeamSqlOperatorExpression(StringOperators.LOWER, subExps);
          break;
        case "TRIM":
          ret = new BeamSqlOperatorExpression(StringOperators.TRIM, subExps);
          break;
        case "SUBSTRING":
          ret = new BeamSqlOperatorExpression(StringOperators.SUBSTRING, subExps);
          break;
        case "OVERLAY":
          ret = new BeamSqlOperatorExpression(StringOperators.OVERLAY, subExps);
          break;
        case "INITCAP":
          ret = new BeamSqlOperatorExpression(StringOperators.INIT_CAP, subExps);
          break;

          // date functions
        case "Reinterpret":
          ret = new BeamSqlReinterpretExpression(subExps, node.type.getSqlTypeName());
          break;
        case "CEIL":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            ret = new BeamSqlCeilExpression(subExps);
          } else {
            ret = new BeamSqlOperatorExpression(DateOperators.DATETIME_CEIL, subExps);
          }
          break;

        case "FLOOR":
          if (SqlTypeName.NUMERIC_TYPES.contains(node.type.getSqlTypeName())) {
            ret = new BeamSqlFloorExpression(subExps);
          } else {
            ret = new BeamSqlOperatorExpression(DateOperators.DATETIME_FLOOR, subExps);
          }
          break;

        case "EXTRACT_DATE":
        case "EXTRACT":
          ret = new BeamSqlOperatorExpression(DateOperators.EXTRACT, subExps);
          break;

        case "LOCALTIME":
        case "CURRENT_TIME":
          ret = new BeamSqlCurrentTimeExpression(subExps);
          break;

        case "CURRENT_TIMESTAMP":
        case "LOCALTIMESTAMP":
          ret = new BeamSqlCurrentTimestampExpression(subExps);
          break;

        case "CURRENT_DATE":
          ret = new BeamSqlCurrentDateExpression();
          break;

        case "DATETIME_PLUS":
          ret = new BeamSqlDatetimePlusExpression(subExps);
          break;

          // array functions
        case "ARRAY":
          ret = new BeamSqlArrayExpression(subExps);
          break;
          // map functions
        case "MAP":
          ret = new BeamSqlMapExpression(subExps);
          break;

        case "ITEM":
          switch (subExps.get(0).getOutputType()) {
            case MAP:
              ret = new BeamSqlMapItemExpression(subExps, node.type.getSqlTypeName());
              break;
            case ARRAY:
              ret = new BeamSqlArrayItemExpression(subExps, node.type.getSqlTypeName());
              break;
            default:
              throw new UnsupportedOperationException(
                  "Operator: " + opName + " is not supported yet");
          }
          break;

          // collections functions
        case "ELEMENT":
          ret = new BeamSqlSingleElementExpression(subExps, node.type.getSqlTypeName());
          break;

        case "CARDINALITY":
          ret = new BeamSqlCardinalityExpression(subExps, node.type.getSqlTypeName());
          break;

        case "DOT":
          ret = new BeamSqlDotExpression(subExps, node.type.getSqlTypeName());
          break;

          // DEFAULT keyword for UDF with optional parameter
        case "DEFAULT":
          ret = new BeamSqlDefaultExpression();
          break;

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

        default:
          // handle UDF
          if (((RexCall) rexNode).getOperator() instanceof SqlUserDefinedFunction) {
            SqlUserDefinedFunction udf = (SqlUserDefinedFunction) ((RexCall) rexNode).getOperator();
            UdfImpl fn = (UdfImpl) udf.getFunction();
            ret =
                new BeamSqlUdfExpression(
                    fn.method, subExps, ((RexCall) rexNode).type.getSqlTypeName());
          } else {
            throw new UnsupportedOperationException(
                "Operator: " + opName + " is not supported yet");
          }
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported yet", rexNode.getClass().toString()));
    }
    return ret;
  }

  private static boolean isDateNode(SqlTypeName type, Object value) {
    return (type == SqlTypeName.DATE || type == SqlTypeName.TIME || type == SqlTypeName.TIMESTAMP)
        && value instanceof Calendar;
  }

  @Override
  public void prepare() {}

  @Override
  public @Nullable List<Object> execute(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {

    final BeamSqlExpressionEnvironment localEnv = env.copyWithLocalRefExprs(exprs);

    boolean conditionResult = filterCondition.evaluate(inputRow, window, localEnv).getBoolean();

    if (conditionResult) {
      return projections
          .stream()
          .map(project -> project.evaluate(inputRow, window, localEnv).getValue())
          .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  @Override
  public void close() {}
}
