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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlBasicCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.util.NaNUtil;

public class FilterUtils {

  private static final Map<SqlKind, Operation> FILTERS =
      ImmutableMap.<SqlKind, Operation>builder()
          .put(SqlKind.IS_TRUE, Operation.TRUE)
          .put(SqlKind.IS_NOT_FALSE, Operation.TRUE)
          .put(SqlKind.IS_FALSE, Operation.FALSE)
          .put(SqlKind.IS_NOT_TRUE, Operation.FALSE)
          .put(SqlKind.IS_NULL, Operation.IS_NULL)
          .put(SqlKind.IS_NOT_NULL, Operation.NOT_NULL)
          .put(SqlKind.LESS_THAN, Operation.LT)
          .put(SqlKind.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
          .put(SqlKind.GREATER_THAN, Operation.GT)
          .put(SqlKind.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
          .put(SqlKind.EQUALS, Operation.EQ)
          .put(SqlKind.NOT_EQUALS, Operation.NOT_EQ)
          .put(SqlKind.IN, Operation.IN)
          .put(SqlKind.NOT_IN, Operation.NOT_IN)
          .put(SqlKind.AND, Operation.AND)
          .put(SqlKind.OR, Operation.OR)
          .build();

  public static Expression convert(String filter) throws SqlParseException {
    SqlParser parser = SqlParser.create(filter);
    return convert(parser.parseExpression());
  }

  private static Expression convert(SqlNode expression) throws SqlParseException {
    Preconditions.checkArgument(expression instanceof SqlBasicCall);
    SqlBasicCall call = (SqlBasicCall) expression;

    SqlOperator op = call.getOperator();
    SqlKind kind = op.getKind();

    Operation operation =
        checkArgumentNotNull(
            FILTERS.get(kind),
            "Unable to convert filter to Iceberg expression: %s",
            expression.toString());

    switch (operation) {
      case TRUE:
        return Expressions.alwaysTrue();
      case FALSE:
        return Expressions.alwaysFalse();
      case IS_NULL:
        return Expressions.isNull(getOnlyChild(call).toString());
      case NOT_NULL:
        return Expressions.notNull(getOnlyChild(call).toString());
      case LT:
        return convertFieldAndLiteral(Expressions::lessThan, Expressions::greaterThan, call);
      case LT_EQ:
        return convertFieldAndLiteral(
            Expressions::lessThanOrEqual, Expressions::greaterThanOrEqual, call);
      case GT:
        return convertFieldAndLiteral(Expressions::greaterThan, Expressions::lessThan, call);
      case GT_EQ:
        return convertFieldAndLiteral(
            Expressions::greaterThanOrEqual, Expressions::lessThanOrEqual, call);
      case EQ:
        return convertFieldAndLiteral(
            (ref, lit) -> {
              if (lit == null) {
                return Expressions.isNull(ref);
              } else if (NaNUtil.isNaN(lit)) {
                return Expressions.isNaN(ref);
              } else {
                return Expressions.equal(ref, lit);
              }
            },
            call);
      case NOT_EQ:
        return convertFieldAndLiteral(
            (ref, lit) -> {
              if (lit == null) {
                return Expressions.notNull(ref);
              } else if (NaNUtil.isNaN(lit)) {
                return Expressions.notNaN(ref);
              } else {
                return Expressions.notEqual(ref, lit);
              }
            },
            call);
      case IN:
        return convertFieldInLiteral(Expressions::in, call);
      case NOT_IN:
        return convertFieldInLiteral(Expressions::notIn, call);
      case AND:
        return convertLogicalExpr(Expressions::and, call);
      case OR:
        return convertLogicalExpr(Expressions::or, call);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported operation '%s' in filter expression: %s", operation, call));
    }
  }

  private static SqlNode getOnlyChild(SqlBasicCall call) {
    Preconditions.checkArgument(
        call.operandCount() == 1,
        "Expected only 1 operand but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    return call.operand(0);
  }

  private static SqlNode getLeftChild(SqlBasicCall call) {
    Preconditions.checkArgument(
        call.operandCount() == 2,
        "Expected 2 operands but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    return call.operand(0);
  }

  private static SqlNode getRightChild(SqlBasicCall call) {
    Preconditions.checkArgument(
        call.operandCount() == 2,
        "Expected 2 operands but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    return call.operand(1);
  }

  private static Expression convertLogicalExpr(
      BiFunction<Expression, Expression, Expression> expr, SqlBasicCall call)
      throws SqlParseException {
    SqlNode left = getLeftChild(call);
    SqlNode right = getRightChild(call);
    return expr.apply(convert(left), convert(right));
  }

  private static Expression convertFieldInLiteral(
      BiFunction<String, Object, Expression> expr, SqlBasicCall call) {
    List<SqlNode> operands = call.getOperandList();
    operands = operands.subList(1, operands.size());
    SqlNode term = call.operand(0);
    Preconditions.checkArgument(
        term instanceof SqlIdentifier && operands.stream().allMatch(o -> o instanceof SqlLiteral));
    String field = ((SqlIdentifier) term).getSimple();
    List<Object> values =
        operands.stream()
            .map(o -> convertLiteral((SqlLiteral) o, field))
            .collect(Collectors.toList());
    return expr.apply(field, values);
  }

  private static Expression convertFieldAndLiteral(
      BiFunction<String, Object, Expression> expr, SqlBasicCall call) {
    return convertFieldAndLiteral(expr, expr, call);
  }

  private static Expression convertFieldAndLiteral(
      BiFunction<String, Object, Expression> convertLR,
      BiFunction<String, Object, Expression> convertRL,
      SqlBasicCall call) {
    SqlNode left = getLeftChild(call);
    SqlNode right = getRightChild(call);
    if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
      String field = ((SqlIdentifier) left).getSimple();
      Object value = convertLiteral((SqlLiteral) right, field);
      return convertLR.apply(field, value);
    } else if (left instanceof SqlLiteral && right instanceof SqlIdentifier) {
      String field = ((SqlIdentifier) right).getSimple();
      Object value = convertLiteral((SqlLiteral) left, field);
      return convertRL.apply(field, value);
    } else {
      throw new IllegalArgumentException("Unsupported operands for expression: " + call);
    }
  }

  private static Object convertLiteral(SqlLiteral literal, String field) {
    switch (literal.getTypeName()) {
      case BOOLEAN:
        return literal.getValueAs(Boolean.class);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return literal.getValueAs(Integer.class);
      case BIGINT:
        return literal.getValueAs(Long.class);
      case FLOAT:
        return literal.getValueAs(Float.class);
      case DOUBLE:
        return literal.getValueAs(Double.class);
      case DECIMAL:
        return literal.getValueAs(BigDecimal.class);
      case CHAR:
      case VARCHAR:
        return literal.getValueAs(String.class);
      case DATE:
        Date sqlDate = literal.getValueAs(Date.class);
        return sqlDate.toLocalDate();
      case TIME:
        Time sqlTime = literal.getValueAs(Time.class);
        return sqlTime.toLocalTime();
      case TIMESTAMP:
        Timestamp ts = literal.getValueAs(Timestamp.class);
        return ts.toLocalDateTime();
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported literal type in field '%s': %s", field, literal.getTypeName()));
    }
  }
}
