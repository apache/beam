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
package org.apache.beam.sdk.io.parquet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.io.api.Binary;

public class ParquetFilterFactory {
  private static final ImmutableMap<SqlKind, FilterCombiner> COMBINERS =
      ImmutableMap.of(SqlKind.AND, FilterApi::and, SqlKind.OR, FilterApi::or);

  private static final Set<SqlKind> COMPARISON_KINDS =
      ImmutableSet.of(
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL);

  public static ParquetFilter create(List<RexNode> expressions, Schema beamSchema) {
    FilterPredicate internalPredicate = toFilterPredicate(expressions, beamSchema);
    return new ParquetFilterImpl(internalPredicate);
  }

  public static ParquetFilter fromPredicate(FilterPredicate predicate) {
    return new ParquetFilterImpl(predicate);
  }

  /** Private implementation of our filter interface that holds the real Parquet object. */
  static class ParquetFilterImpl implements ParquetFilter {
    private final @Nullable FilterPredicate predicate;

    ParquetFilterImpl(@Nullable FilterPredicate predicate) {
      this.predicate = predicate;
    }

    // This method allows ParquetIO to "unwrap" the filter.
    @Nullable
    FilterPredicate getPredicate() {
      return predicate;
    }
  }

  @Nullable
  private static FilterPredicate toFilterPredicate(List<RexNode> expressions, Schema beamSchema) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    }

    List<FilterPredicate> predicates = new ArrayList<>();
    for (RexNode expr : expressions) {
      FilterPredicate p = convert(expr, beamSchema);
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;
    }
    return predicates.stream().reduce(FilterApi::and).orElse(null);
  }

  @Nullable
  private static FilterPredicate convert(RexNode e, Schema beamSchema) {
    SqlKind kind = e.getKind();
    if (COMBINERS.containsKey(kind)) {
      return combine((RexCall) e, beamSchema);
    }

    switch (kind) {
      case IN:
        return toInPredicate((RexCall) e, beamSchema, false);
      case NOT_IN:
        return toInPredicate((RexCall) e, beamSchema, true);
      case NOT:
        FilterPredicate inner = convert(((RexCall) e).getOperands().get(0), beamSchema);
        return (inner == null) ? null : FilterApi.not(inner);
      case IS_NULL:
      case IS_NOT_NULL:
        return toUnaryPredicate((RexCall) e, beamSchema);
      default:
        if (COMPARISON_KINDS.contains(kind)) {
          return toBinaryPredicate((RexCall) e, beamSchema);
        }
        return null;
    }
  }

  @Nullable
  private static FilterPredicate combine(RexCall call, Schema beamSchema) {
    FilterCombiner combiner = COMBINERS.get(call.getKind());
    if (combiner == null) {
      return null;
    }

    List<FilterPredicate> predicates = new ArrayList<>();
    for (RexNode op : call.getOperands()) {
      FilterPredicate p = convert(op, beamSchema);
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;
    }
    return predicates.stream().reduce(combiner::combine).orElse(null);
  }

  @Nullable
  private static FilterPredicate toInPredicate(RexCall call, Schema beamSchema, boolean isNotIn) {
    RexInputRef columnRef = (RexInputRef) call.getOperands().get(0);
    List<RexNode> valueNodes = call.getOperands().subList(1, call.getOperands().size());
    SqlKind comparison = isNotIn ? SqlKind.NOT_EQUALS : SqlKind.EQUALS;

    // CHANGE: Use an explicit loop
    List<FilterPredicate> predicates = new ArrayList<>();
    for (RexNode valueNode : valueNodes) {
      FilterPredicate p =
          createSingleComparison(comparison, columnRef, (RexLiteral) valueNode, beamSchema);
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;
    }
    return predicates.stream().reduce(isNotIn ? FilterApi::and : FilterApi::or).orElse(null);
  }

  @Nullable
  private static FilterPredicate toUnaryPredicate(RexCall call, Schema beamSchema) {
    RexNode operand = call.getOperands().get(0);
    if (!(operand instanceof RexInputRef)) {
      return null;
    }
    RexInputRef columnRef = (RexInputRef) operand;
    String columnName = getColumnName(columnRef, beamSchema);
    SqlTypeName type = columnRef.getType().getSqlTypeName();
    boolean isNull = call.getKind() == SqlKind.IS_NULL;
    switch (type) {
      case INTEGER:
        return isNull
            ? FilterApi.eq(FilterApi.intColumn(columnName), null)
            : FilterApi.notEq(FilterApi.intColumn(columnName), null);
      case BIGINT:
        return isNull
            ? FilterApi.eq(FilterApi.longColumn(columnName), null)
            : FilterApi.notEq(FilterApi.longColumn(columnName), null);
      case FLOAT:
        return isNull
            ? FilterApi.eq(FilterApi.floatColumn(columnName), null)
            : FilterApi.notEq(FilterApi.floatColumn(columnName), null);
      case DOUBLE:
        return isNull
            ? FilterApi.eq(FilterApi.doubleColumn(columnName), null)
            : FilterApi.notEq(FilterApi.doubleColumn(columnName), null);
      case BOOLEAN:
        return isNull
            ? FilterApi.eq(FilterApi.booleanColumn(columnName), null)
            : FilterApi.notEq(FilterApi.booleanColumn(columnName), null);
      case VARCHAR:
      case CHAR:
      case DECIMAL:
        return isNull
            ? FilterApi.eq(FilterApi.binaryColumn(columnName), null)
            : FilterApi.notEq(FilterApi.binaryColumn(columnName), null);
      default:
        return null;
    }
  }

  @Nullable
  private static FilterPredicate toBinaryPredicate(RexCall call, Schema beamSchema) {
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    if (left.getKind() == SqlKind.CAST) {
      left = ((RexCall) left).getOperands().get(0);
    }
    if (right.getKind() == SqlKind.CAST) {
      right = ((RexCall) right).getOperands().get(0);
    }
    if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) {
      return null;
    }
    return createSingleComparison(
        call.getKind(), (RexInputRef) left, (RexLiteral) right, beamSchema);
  }

  @Nullable
  private static FilterPredicate createSingleComparison(
      SqlKind kind, RexInputRef columnRef, RexLiteral literal, Schema beamSchema) {

    String columnName = getColumnName(columnRef, beamSchema);
    SqlTypeName columnType = columnRef.getType().getSqlTypeName();

    Comparable<?> value = literal.getValueAs(Comparable.class);
    if (value == null) {
      return null;
    }

    switch (columnType) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return createIntPredicate(kind, columnName, ((Number) value).intValue());
      case BIGINT:
        return createLongPredicate(kind, columnName, ((Number) value).longValue());
      case FLOAT:
        return createFloatPredicate(kind, columnName, ((Number) value).floatValue());
      case DOUBLE:
        return createDoublePredicate(kind, columnName, ((Number) value).doubleValue());
      case BOOLEAN:
        return createBooleanPredicate(kind, columnName, (Boolean) value);
      case CHAR:
      case VARCHAR:
        return createStringPredicate(kind, columnName, value.toString());
      case BINARY:
      case VARBINARY:
        org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.BitString bitString =
            (org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.BitString) value;
        return createBinaryPredicate(
            kind, columnName, Binary.fromConstantByteArray(bitString.getAsByteArray()));
      case DECIMAL:
        BigDecimal bigDecimalValue = (BigDecimal) value;
        return createBinaryPredicate(
            kind,
            columnName,
            Binary.fromConstantByteArray(bigDecimalValue.unscaledValue().toByteArray()));
      case DATE:
      case TIME:
      case TIMESTAMP:
      case ARRAY:
      case MAP:
      case ROW:
      default:
        return null;
    }
  }

  private static String getColumnName(RexInputRef columnRef, Schema beamSchema) {
    return beamSchema.getField(columnRef.getIndex()).getName();
  }

  private static FilterPredicate createIntPredicate(SqlKind kind, String name, Integer value) {
    IntColumn col = FilterApi.intColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      case GREATER_THAN:
        return FilterApi.gt(col, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(col, value);
      case LESS_THAN:
        return FilterApi.lt(col, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for INT: " + kind);
    }
  }

  private static FilterPredicate createLongPredicate(SqlKind kind, String name, Long value) {
    LongColumn col = FilterApi.longColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      case GREATER_THAN:
        return FilterApi.gt(col, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(col, value);
      case LESS_THAN:
        return FilterApi.lt(col, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for LONG: " + kind);
    }
  }

  private static FilterPredicate createFloatPredicate(SqlKind kind, String name, Float value) {
    FloatColumn col = FilterApi.floatColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      case GREATER_THAN:
        return FilterApi.gt(col, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(col, value);
      case LESS_THAN:
        return FilterApi.lt(col, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for FLOAT: " + kind);
    }
  }

  private static FilterPredicate createDoublePredicate(SqlKind kind, String name, Double value) {
    DoubleColumn col = FilterApi.doubleColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      case GREATER_THAN:
        return FilterApi.gt(col, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(col, value);
      case LESS_THAN:
        return FilterApi.lt(col, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for DOUBLE: " + kind);
    }
  }

  private static FilterPredicate createBooleanPredicate(SqlKind kind, String name, Boolean value) {
    BooleanColumn col = FilterApi.booleanColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for BOOLEAN: " + kind);
    }
  }

  private static FilterPredicate createStringPredicate(SqlKind kind, String name, String value) {
    return createBinaryPredicate(kind, name, Binary.fromString(value));
  }

  private static FilterPredicate createBinaryPredicate(SqlKind kind, String name, Binary value) {
    BinaryColumn col = FilterApi.binaryColumn(name);
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(col, value);
      case NOT_EQUALS:
        return FilterApi.notEq(col, value);
      case GREATER_THAN:
        return FilterApi.gt(col, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(col, value);
      case LESS_THAN:
        return FilterApi.lt(col, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for BINARY: " + kind);
    }
  }

  @FunctionalInterface
  private interface FilterCombiner {
    FilterPredicate combine(FilterPredicate a, FilterPredicate b);
  }
}
