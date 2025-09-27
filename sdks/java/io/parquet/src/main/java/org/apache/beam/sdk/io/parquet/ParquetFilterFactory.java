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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.io.api.Binary;

/**
 * Factory class for creating Parquet filter predicates from Calcite RexNode expressions.
 *
 * <p>This class converts SQL filter expressions (represented as Calcite RexNodes) into Parquet
 * filter predicates that can be used for predicate pushdown during Parquet file reading. This
 * enables significant performance improvements by filtering data at the storage level rather than
 * after reading all data into memory.
 *
 * <p>Supported operations include:
 *
 * <ul>
 *   <li>Comparison operators: =, !=, &lt;, &lt;=, &gt;, &gt;=
 *   <li>Logical operators: AND, OR, NOT
 *   <li>Set operations: IN, NOT IN
 *   <li>Null checks: IS NULL, IS NOT NULL
 * </ul>
 *
 * <p>Supported data types include:
 *
 * <ul>
 *   <li>Integer types: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>Floating point: FLOAT, DOUBLE
 *   <li>Boolean: BOOLEAN
 *   <li>String types: CHAR, VARCHAR
 *   <li>Binary types: BINARY, VARBINARY
 *   <li>Decimal: DECIMAL
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a filter from SQL expressions
 * List<RexNode> expressions = ...; // from SQL WHERE clause
 * Schema beamSchema = ...; // Beam schema for the table
 * ParquetFilter filter = ParquetFilterFactory.create(expressions, beamSchema);
 *
 * // Use with ParquetIO
 * ParquetIO.read(schema)
 *   .from(filePattern)
 *   .withFilter(filter)
 *   .withBeamSchemas(true);
 * }</pre>
 */
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

  /**
   * Creates a ParquetFilter from a list of Calcite RexNode expressions.
   *
   * <p>This method converts SQL filter expressions into Parquet filter predicates. Only supported
   * expressions will be converted; unsupported expressions are silently ignored, allowing for
   * partial filter pushdown.
   *
   * @param expressions List of RexNode expressions representing SQL filter conditions. Can be null
   *     or empty, in which case no filtering is applied.
   * @param beamSchema The Beam schema for the table being filtered. Used to map column indices to
   *     field names and validate column references.
   * @return A ParquetFilter that can be used with ParquetIO for predicate pushdown.
   * @throws IllegalArgumentException if beamSchema is null
   */
  public static ParquetFilter create(List<RexNode> expressions, Schema beamSchema) {
    if (beamSchema == null) {
      throw new IllegalArgumentException("Beam schema cannot be null");
    }
    FilterPredicate internalPredicate = toFilterPredicate(expressions, beamSchema);
    return new ParquetFilterImpl(internalPredicate);
  }

  /**
   * Creates a ParquetFilter from an existing Parquet FilterPredicate.
   *
   * <p>This method is useful when you already have a Parquet FilterPredicate and want to wrap it in
   * the Beam ParquetFilter interface.
   *
   * @param predicate The Parquet FilterPredicate to wrap. Can be null.
   * @return A ParquetFilter wrapping the provided predicate.
   */
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

    // Pre-allocate list with expected size for better performance
    List<FilterPredicate> predicates = new ArrayList<>(expressions.size());
    for (RexNode expr : expressions) {
      if (expr == null) {
        continue; // Skip null expressions
      }
      try {
        FilterPredicate p = convert(expr, beamSchema);
        if (p != null) {
          predicates.add(p);
        }
      } catch (Exception e) {
        // Log the error but continue processing other expressions
        // This allows partial filter pushdown when some expressions fail
        System.err.println(
            "Failed to convert expression to Parquet filter: "
                + expr
                + ", error: "
                + e.getMessage());
      }
    }

    if (predicates.isEmpty()) {
      return null;
    }

    // Optimize: if only one predicate, return it directly
    if (predicates.size() == 1) {
      return predicates.get(0);
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

    // Pre-allocate list with expected size for better performance
    List<FilterPredicate> predicates = new ArrayList<>(call.getOperands().size());
    for (RexNode op : call.getOperands()) {
      FilterPredicate p = convert(op, beamSchema);
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;
    }

    // Optimize: if only one predicate, return it directly
    if (predicates.size() == 1) {
      return predicates.get(0);
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
    if (call.getOperands().size() != 2) {
      return null; // Binary predicates must have exactly 2 operands
    }

    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    // Handle CAST operations
    if (left.getKind() == SqlKind.CAST) {
      left = ((RexCall) left).getOperands().get(0);
    }
    if (right.getKind() == SqlKind.CAST) {
      right = ((RexCall) right).getOperands().get(0);
    }

    // Only support column = literal comparisons
    if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) {
      return null;
    }

    RexInputRef columnRef = (RexInputRef) left;
    RexLiteral literal = (RexLiteral) right;

    // Validate column index is within schema bounds
    if (columnRef.getIndex() < 0 || columnRef.getIndex() >= beamSchema.getFieldCount()) {
      return null;
    }

    return createSingleComparison(call.getKind(), columnRef, literal, beamSchema);
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
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.BitString bitString =
            (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.BitString) value;
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
    int fieldIndex = columnRef.getIndex();
    if (fieldIndex < 0 || fieldIndex >= beamSchema.getFieldCount()) {
      throw new IllegalArgumentException(
          "Column index "
              + fieldIndex
              + " is out of bounds for schema with "
              + beamSchema.getFieldCount()
              + " fields");
    }
    return beamSchema.getField(fieldIndex).getName();
  }

  /** Creates a filter predicate for integer values. */
  private static FilterPredicate createIntPredicate(SqlKind kind, String name, Integer value) {
    return createIntComparison(kind, FilterApi.intColumn(name), value);
  }

  /** Creates a filter predicate for long values. */
  private static FilterPredicate createLongPredicate(SqlKind kind, String name, Long value) {
    return createLongComparison(kind, FilterApi.longColumn(name), value);
  }

  /** Creates a filter predicate for float values. */
  private static FilterPredicate createFloatPredicate(SqlKind kind, String name, Float value) {
    return createFloatComparison(kind, FilterApi.floatColumn(name), value);
  }

  /** Creates a filter predicate for double values. */
  private static FilterPredicate createDoublePredicate(SqlKind kind, String name, Double value) {
    return createDoubleComparison(kind, FilterApi.doubleColumn(name), value);
  }

  /** Creates a filter predicate for boolean values. */
  private static FilterPredicate createBooleanPredicate(SqlKind kind, String name, Boolean value) {
    return createBooleanComparison(kind, FilterApi.booleanColumn(name), value);
  }

  /** Creates a filter predicate for string values. */
  private static FilterPredicate createStringPredicate(SqlKind kind, String name, String value) {
    return createBinaryPredicate(kind, name, Binary.fromString(value));
  }

  /** Creates a filter predicate for binary values. */
  private static FilterPredicate createBinaryPredicate(SqlKind kind, String name, Binary value) {
    return createBinaryComparison(kind, FilterApi.binaryColumn(name), value);
  }

  /** Helper method to create comparison predicates for integer columns. */
  private static FilterPredicate createIntComparison(
      SqlKind kind,
      org.apache.parquet.filter2.predicate.Operators.IntColumn column,
      Integer value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      case GREATER_THAN:
        return FilterApi.gt(column, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(column, value);
      case LESS_THAN:
        return FilterApi.lt(column, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for INT: " + kind);
    }
  }

  /** Helper method to create comparison predicates for long columns. */
  private static FilterPredicate createLongComparison(
      SqlKind kind, org.apache.parquet.filter2.predicate.Operators.LongColumn column, Long value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      case GREATER_THAN:
        return FilterApi.gt(column, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(column, value);
      case LESS_THAN:
        return FilterApi.lt(column, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for LONG: " + kind);
    }
  }

  /** Helper method to create comparison predicates for float columns. */
  private static FilterPredicate createFloatComparison(
      SqlKind kind,
      org.apache.parquet.filter2.predicate.Operators.FloatColumn column,
      Float value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      case GREATER_THAN:
        return FilterApi.gt(column, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(column, value);
      case LESS_THAN:
        return FilterApi.lt(column, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for FLOAT: " + kind);
    }
  }

  /** Helper method to create comparison predicates for double columns. */
  private static FilterPredicate createDoubleComparison(
      SqlKind kind,
      org.apache.parquet.filter2.predicate.Operators.DoubleColumn column,
      Double value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      case GREATER_THAN:
        return FilterApi.gt(column, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(column, value);
      case LESS_THAN:
        return FilterApi.lt(column, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for DOUBLE: " + kind);
    }
  }

  /** Helper method to create comparison predicates for boolean columns. */
  private static FilterPredicate createBooleanComparison(
      SqlKind kind,
      org.apache.parquet.filter2.predicate.Operators.BooleanColumn column,
      Boolean value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for BOOLEAN: " + kind);
    }
  }

  /** Helper method to create comparison predicates for binary columns. */
  private static FilterPredicate createBinaryComparison(
      SqlKind kind, BinaryColumn column, Binary value) {
    switch (kind) {
      case EQUALS:
        return FilterApi.eq(column, value);
      case NOT_EQUALS:
        return FilterApi.notEq(column, value);
      case GREATER_THAN:
        return FilterApi.gt(column, value);
      case GREATER_THAN_OR_EQUAL:
        return FilterApi.gtEq(column, value);
      case LESS_THAN:
        return FilterApi.lt(column, value);
      case LESS_THAN_OR_EQUAL:
        return FilterApi.ltEq(column, value);
      default:
        throw new UnsupportedOperationException("Unsupported operator for BINARY: " + kind);
    }
  }

  @FunctionalInterface
  private interface FilterCombiner {
    FilterPredicate combine(FilterPredicate a, FilterPredicate b);
  }
}
