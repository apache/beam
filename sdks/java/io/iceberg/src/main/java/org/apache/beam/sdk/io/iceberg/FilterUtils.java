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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlBasicCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities that convert between a SQL filter expression and an Iceberg {@link Expression}. Uses
 * Apache Calcite semantics.
 *
 * <p>Note: Only supports top-level fields (i.e. cannot reference nested fields).
 */
class FilterUtils {
  private static final Map<SqlKind, Operation> FILTERS =
      ImmutableMap.<SqlKind, Operation>builder()
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

  static Expression convert(@Nullable String filter, Schema schema) {
    if (filter == null) {
      return Expressions.alwaysTrue();
    }

    SqlParser parser = SqlParser.create(filter);
    try {
      SqlNode expression = parser.parseExpression();
      return convert(expression, schema);
    } catch (Exception exception) {
      throw new RuntimeException(
          String.format("Encountered an error when parsing filter: '%s'", filter), exception);
    }
  }

  private static Expression convert(SqlNode expression, Schema schema) throws SqlParseException {
    checkArgument(expression instanceof SqlBasicCall);
    SqlBasicCall call = (SqlBasicCall) expression;

    SqlOperator op = call.getOperator();
    SqlKind kind = op.getKind();

    Operation operation =
        checkArgumentNotNull(
            FILTERS.get(kind),
            "Unable to convert SQL operation '%s' in Iceberg expression: %s",
            kind,
            expression.toString());

    switch (operation) {
      case IS_NULL:
        return Expressions.isNull(getOnlyChildName(call));
      case NOT_NULL:
        return Expressions.notNull(getOnlyChildName(call));
      case LT:
        return convertFieldAndLiteral(
            Expressions::lessThan, Expressions::greaterThan, call, schema);
      case LT_EQ:
        return convertFieldAndLiteral(
            Expressions::lessThanOrEqual, Expressions::greaterThanOrEqual, call, schema);
      case GT:
        return convertFieldAndLiteral(
            Expressions::greaterThan, Expressions::lessThan, call, schema);
      case GT_EQ:
        return convertFieldAndLiteral(
            Expressions::greaterThanOrEqual, Expressions::lessThanOrEqual, call, schema);
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
            call,
            schema);
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
            call,
            schema);
      case IN:
        return convertFieldInLiteral(Operation.IN, call, schema);
      case NOT_IN:
        return convertFieldInLiteral(Operation.NOT_IN, call, schema);
      case AND:
        return convertLogicalExpr(Expressions::and, call, schema);
      case OR:
        return convertLogicalExpr(Expressions::or, call, schema);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported operation '%s' in filter expression: %s", operation, call));
    }
  }

  private static String getOnlyChildName(SqlBasicCall call) {
    checkArgument(
        call.operandCount() == 1,
        "Expected only 1 operand but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    SqlNode ref = call.operand(0);
    Preconditions.checkState(
        ref instanceof SqlIdentifier, "Expected operand '%s' to be a reference.", ref);
    return ((SqlIdentifier) ref).getSimple();
  }

  private static SqlNode getLeftChild(SqlBasicCall call) {
    checkArgument(
        call.operandCount() == 2,
        "Expected 2 operands but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    return call.operand(0);
  }

  private static SqlNode getRightChild(SqlBasicCall call) {
    checkArgument(
        call.operandCount() == 2,
        "Expected 2 operands but got %s in filter: %s",
        call.getOperandList(),
        call.toString());
    return call.operand(1);
  }

  private static Expression convertLogicalExpr(
      BiFunction<Expression, Expression, Expression> expr, SqlBasicCall call, Schema schema)
      throws SqlParseException {
    SqlNode left = getLeftChild(call);
    SqlNode right = getRightChild(call);
    return expr.apply(convert(left, schema), convert(right, schema));
  }

  private static Expression convertFieldInLiteral(Operation op, SqlBasicCall call, Schema schema) {
    checkArgument(
        call.operandCount() == 2,
        "Expected only 2 operands but got %s: %s",
        call.operandCount(),
        call);
    SqlNode term = call.operand(0);
    SqlNode value = call.operand(1);
    checkArgument(
        term instanceof SqlIdentifier,
        "Expected left hand side to be a field identifier but got " + term.getClass());
    checkArgument(
        value instanceof SqlNodeList,
        "Expected right hand side to be a list but got " + value.getClass());
    String name = ((SqlIdentifier) term).getSimple();
    TypeID type = schema.findType(name).typeId();
    List<SqlNode> list =
        ((SqlNodeList) value)
            .getList().stream().filter(Objects::nonNull).collect(Collectors.toList());
    checkArgument(list.stream().allMatch(o -> o instanceof SqlLiteral));
    List<Object> values =
        list.stream()
            .map(o -> convertLiteral((SqlLiteral) o, name, type))
            .collect(Collectors.toList());
    return op == Operation.IN ? Expressions.in(name, values) : Expressions.notIn(name, values);
  }

  private static Expression convertFieldAndLiteral(
      BiFunction<String, Object, Expression> expr, SqlBasicCall call, Schema schema) {
    return convertFieldAndLiteral(expr, expr, call, schema);
  }

  private static Expression convertFieldAndLiteral(
      BiFunction<String, Object, Expression> convertLR,
      BiFunction<String, Object, Expression> convertRL,
      SqlBasicCall call,
      Schema schema) {
    SqlNode left = getLeftChild(call);
    SqlNode right = getRightChild(call);
    if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
      String name = ((SqlIdentifier) left).getSimple();
      TypeID type = schema.findType(name).typeId();
      Object value = convertLiteral((SqlLiteral) right, name, type);
      return convertLR.apply(name, value);
    } else if (left instanceof SqlLiteral && right instanceof SqlIdentifier) {
      String name = ((SqlIdentifier) right).getSimple();
      TypeID type = schema.findType(name).typeId();
      Object value = convertLiteral((SqlLiteral) left, name, type);
      return convertRL.apply(name, value);
    } else {
      throw new IllegalArgumentException("Unsupported operands for expression: " + call);
    }
  }

  private static Object convertLiteral(SqlLiteral literal, String field, TypeID type) {
    switch (type) {
      case BOOLEAN:
        return literal.getValueAs(Boolean.class);
      case INTEGER:
        return literal.getValueAs(Integer.class);
      case LONG:
        return literal.getValueAs(Long.class);
      case FLOAT:
        return literal.getValueAs(Float.class);
      case DOUBLE:
        return literal.getValueAs(Double.class);
      case DECIMAL:
        return literal.getValueAs(BigDecimal.class);
      case STRING:
        return literal.getValueAs(String.class);
      case DATE:
        LocalDate date = LocalDate.parse(literal.getValueAs(String.class));
        return DateTimeUtil.daysFromDate(date);
      case TIME:
        LocalTime time = LocalTime.parse(literal.getValueAs(String.class));
        return DateTimeUtil.microsFromTime(time);
      case TIMESTAMP:
        LocalDateTime dateTime = LocalDateTime.parse(literal.getValueAs(String.class));
        return DateTimeUtil.microsFromTimestamp(dateTime);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported filter type in field '%s': %s", field, type));
    }
  }
}
