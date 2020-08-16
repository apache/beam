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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.functions.ZetaSQLDateTime.DateTimestampPart;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamBigQuerySqlDialect;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.SqlOperators;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.TimeUnit;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.DateString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.TimeString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.TimestampString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Utility methods for ZetaSQL <=> Calcite translation.
 *
 * <p>Unsupported ZetaSQL types: INT32, UINT32, UINT64, FLOAT, ENUM (internal), PROTO, GEOGRAPHY
 */
@Internal
public final class ZetaSqlCalciteTranslationUtils {

  private ZetaSqlCalciteTranslationUtils() {}

  // TODO[BEAM-9178]: support DateTimestampPart.WEEK and "WEEK with weekday"s
  private static final ImmutableMap<Integer, TimeUnit> TIME_UNIT_CASTING_MAP =
      ImmutableMap.<Integer, TimeUnit>builder()
          .put(DateTimestampPart.YEAR.getNumber(), TimeUnit.YEAR)
          .put(DateTimestampPart.MONTH.getNumber(), TimeUnit.MONTH)
          .put(DateTimestampPart.DAY.getNumber(), TimeUnit.DAY)
          .put(DateTimestampPart.DAYOFWEEK.getNumber(), TimeUnit.DOW)
          .put(DateTimestampPart.DAYOFYEAR.getNumber(), TimeUnit.DOY)
          .put(DateTimestampPart.QUARTER.getNumber(), TimeUnit.QUARTER)
          .put(DateTimestampPart.HOUR.getNumber(), TimeUnit.HOUR)
          .put(DateTimestampPart.MINUTE.getNumber(), TimeUnit.MINUTE)
          .put(DateTimestampPart.SECOND.getNumber(), TimeUnit.SECOND)
          .put(DateTimestampPart.MILLISECOND.getNumber(), TimeUnit.MILLISECOND)
          .put(DateTimestampPart.MICROSECOND.getNumber(), TimeUnit.MICROSECOND)
          .put(DateTimestampPart.ISOYEAR.getNumber(), TimeUnit.ISOYEAR)
          .put(DateTimestampPart.ISOWEEK.getNumber(), TimeUnit.WEEK)
          .build();

  // Type conversion: Calcite => ZetaSQL
  public static Type toZetaSqlType(RelDataType calciteType) {
    switch (calciteType.getSqlTypeName()) {
      case BIGINT:
        return TypeFactory.createSimpleType(TypeKind.TYPE_INT64);
      case DOUBLE:
        return TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE);
      case BOOLEAN:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BOOL);
      case VARCHAR:
        return TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
      case VARBINARY:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BYTES);
      case DECIMAL:
        return TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC);
      case DATE:
        return TypeFactory.createSimpleType(TypeKind.TYPE_DATE);
      case TIME:
        return TypeFactory.createSimpleType(TypeKind.TYPE_TIME);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME);
      case TIMESTAMP:
        return TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP);
      case ARRAY:
        return TypeFactory.createArrayType(toZetaSqlType(calciteType.getComponentType()));
      case ROW:
        return TypeFactory.createStructType(
            calciteType.getFieldList().stream()
                .map(f -> new StructField(f.getName(), toZetaSqlType(f.getType())))
                .collect(Collectors.toList()));
      default:
        throw new UnsupportedOperationException(
            "Unknown Calcite type: " + calciteType.getSqlTypeName().getName());
    }
  }

  // Type conversion: ZetaSQL => Calcite
  public static RelDataType toCalciteType(Type type, boolean nullable, RexBuilder rexBuilder) {
    RelDataType nonNullType;
    switch (type.getKind()) {
      case TYPE_INT64:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        break;
      case TYPE_DOUBLE:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        break;
      case TYPE_BOOL:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
        break;
      case TYPE_STRING:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        break;
      case TYPE_BYTES:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARBINARY);
        break;
      case TYPE_NUMERIC:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL);
        break;
      case TYPE_DATE:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE);
        break;
      case TYPE_TIME:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIME);
        break;
      case TYPE_DATETIME:
        nonNullType =
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        break;
      case TYPE_TIMESTAMP:
        nonNullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
        break;
      case TYPE_ARRAY:
        // TODO: Should element type has the same nullability as the array type?
        nonNullType = toCalciteArrayType(type.asArray().getElementType(), nullable, rexBuilder);
        break;
      case TYPE_STRUCT:
        // TODO: Should field type has the same nullability as the struct type?
        nonNullType = toCalciteStructType(type.asStruct(), nullable, rexBuilder);
        break;
      default:
        throw new UnsupportedOperationException("Unknown ZetaSQL type: " + type.getKind().name());
    }
    return rexBuilder.getTypeFactory().createTypeWithNullability(nonNullType, nullable);
  }

  private static RelDataType toCalciteArrayType(
      Type elementType, boolean nullable, RexBuilder rexBuilder) {
    return rexBuilder
        .getTypeFactory()
        // -1 cardinality means unlimited array size
        .createArrayType(toCalciteType(elementType, nullable, rexBuilder), -1);
  }

  private static RelDataType toCalciteStructType(
      StructType structType, boolean nullable, RexBuilder rexBuilder) {
    List<StructField> fields = structType.getFieldList();
    List<String> fieldNames = getFieldNameList(fields);
    List<RelDataType> fieldTypes =
        fields.stream()
            .map(f -> toCalciteType(f.getType(), nullable, rexBuilder))
            .collect(Collectors.toList());
    return rexBuilder.getTypeFactory().createStructType(fieldTypes, fieldNames);
  }

  private static List<String> getFieldNameList(List<StructField> fields) {
    ImmutableList.Builder<String> b = ImmutableList.builder();
    for (int i = 0; i < fields.size(); i++) {
      String name = fields.get(i).getName();
      if ("".equals(name)) {
        name = "$col" + i; // avoid empty field names because Beam does not allow duplicate names
      }
      b.add(name);
    }
    return b.build();
  }

  // Value conversion: ZetaSQL => Calcite
  public static RexNode toRexNode(Value value, RexBuilder rexBuilder) {
    Type type = value.getType();
    if (value.isNull()) {
      return rexBuilder.makeNullLiteral(toCalciteType(type, true, rexBuilder));
    }

    switch (type.getKind()) {
      case TYPE_INT64:
        return rexBuilder.makeExactLiteral(
            new BigDecimal(value.getInt64Value()), toCalciteType(type, false, rexBuilder));
      case TYPE_DOUBLE:
        // Cannot simply call makeApproxLiteral() for ZetaSQL DOUBLE type because positive infinity,
        // negative infinity and NaN cannot be directly converted to BigDecimal. So we create three
        // wrapper functions here for these three cases such that we can later recognize it and
        // customize its unparsing in BeamBigQuerySqlDialect.
        double val = value.getDoubleValue();
        String wrapperFun = null;
        if (val == Double.POSITIVE_INFINITY) {
          wrapperFun = BeamBigQuerySqlDialect.DOUBLE_POSITIVE_INF_FUNCTION;
        } else if (val == Double.NEGATIVE_INFINITY) {
          wrapperFun = BeamBigQuerySqlDialect.DOUBLE_NEGATIVE_INF_FUNCTION;
        } else if (Double.isNaN(val)) {
          wrapperFun = BeamBigQuerySqlDialect.DOUBLE_NAN_FUNCTION;
        }

        RelDataType returnType = toCalciteType(type, false, rexBuilder);
        if (wrapperFun == null) {
          return rexBuilder.makeApproxLiteral(new BigDecimal(val), returnType);
        } else if (BeamBigQuerySqlDialect.DOUBLE_NAN_FUNCTION.equals(wrapperFun)) {
          // TODO[BEAM-10550]: Update the temporary workaround below after vendored Calcite version.
          // Adding an additional random parameter for the wrapper function of NaN, to avoid
          // triggering Calcite operation simplification. (e.g. 'NaN == NaN' would be simplify to
          // 'null or NaN is not null' in Calcite. This would miscalculate the expression to be
          // true, which should be false.)
          return rexBuilder.makeCall(
              SqlOperators.createZetaSqlFunction(wrapperFun, returnType.getSqlTypeName()),
              rexBuilder.makeApproxLiteral(BigDecimal.valueOf(Math.random()), returnType));
        } else {
          return rexBuilder.makeCall(
              SqlOperators.createZetaSqlFunction(wrapperFun, returnType.getSqlTypeName()));
        }
      case TYPE_BOOL:
        return rexBuilder.makeLiteral(value.getBoolValue());
      case TYPE_STRING:
        // Has to allow CAST because Calcite create CHAR type first and does a CAST to VARCHAR.
        // If not allow cast, rexBuilder() will only build a literal with CHAR type.
        return rexBuilder.makeLiteral(
            value.getStringValue(), toCalciteType(type, false, rexBuilder), true);
      case TYPE_BYTES:
        return rexBuilder.makeBinaryLiteral(new ByteString(value.getBytesValue().toByteArray()));
      case TYPE_NUMERIC:
        // Cannot simply call makeExactLiteral() for ZetaSQL NUMERIC type because later it will be
        // unparsed to the string representation of the BigDecimal itself (e.g. "SELECT NUMERIC '0'"
        // will be unparsed to "SELECT 0E-9"), and Calcite does not allow customize unparsing of
        // SqlNumericLiteral. So we create a wrapper function here such that we can later recognize
        // it and customize its unparsing in BeamBigQuerySqlDialect.
        return rexBuilder.makeCall(
            SqlOperators.createZetaSqlFunction(
                BeamBigQuerySqlDialect.NUMERIC_LITERAL_FUNCTION,
                toCalciteType(type, false, rexBuilder).getSqlTypeName()),
            rexBuilder.makeExactLiteral(
                value.getNumericValue(), toCalciteType(type, false, rexBuilder)));
      case TYPE_DATE:
        return rexBuilder.makeDateLiteral(dateValueToDateString(value));
      case TYPE_TIME:
        return rexBuilder.makeTimeLiteral(
            timeValueToTimeString(value),
            rexBuilder.getTypeFactory().getTypeSystem().getMaxPrecision(SqlTypeName.TIME));
      case TYPE_DATETIME:
        return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
            datetimeValueToTimestampString(value),
            rexBuilder
                .getTypeFactory()
                .getTypeSystem()
                .getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
      case TYPE_TIMESTAMP:
        return rexBuilder.makeTimestampLiteral(
            timestampValueToTimestampString(value),
            rexBuilder.getTypeFactory().getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP));
      case TYPE_ARRAY:
        return arrayValueToRexNode(value, rexBuilder);
      case TYPE_STRUCT:
        return structValueToRexNode(value, rexBuilder);
      case TYPE_ENUM: // internal only, used for DateTimestampPart
        return enumValueToRexNode(value, rexBuilder);
      default:
        throw new UnsupportedOperationException("Unknown ZetaSQL type: " + type.getKind().name());
    }
  }

  private static RexNode arrayValueToRexNode(Value value, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
        toCalciteArrayType(value.getType().asArray().getElementType(), false, rexBuilder),
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        value.getElementList().stream()
            .map(v -> toRexNode(v, rexBuilder))
            .collect(Collectors.toList()));
  }

  private static RexNode structValueToRexNode(Value value, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
        toCalciteStructType(value.getType().asStruct(), false, rexBuilder),
        SqlStdOperatorTable.ROW,
        value.getFieldList().stream()
            .map(v -> toRexNode(v, rexBuilder))
            .collect(Collectors.toList()));
  }

  // internal only, used for DateTimestampPart
  // TODO: Fix Later
  @SuppressWarnings("nullness")
  private static RexNode enumValueToRexNode(Value value, RexBuilder rexBuilder) {
    String enumDescriptorName = value.getType().asEnum().getDescriptor().getFullName();
    if (!"zetasql.functions.DateTimestampPart".equals(enumDescriptorName)) {
      throw new UnsupportedOperationException("Unknown ZetaSQL Enum type: " + enumDescriptorName);
    }
    TimeUnit timeUnit = TIME_UNIT_CASTING_MAP.get(value.getEnumValue());
    if (timeUnit == null) {
      throw new UnsupportedOperationException("Unknown ZetaSQL Enum value: " + value.getEnumName());
    }
    return rexBuilder.makeFlag(TimeUnitRange.of(timeUnit, null));
  }

  private static DateString dateValueToDateString(Value value) {
    return DateString.fromDaysSinceEpoch(value.getDateValue());
  }

  private static TimeString timeValueToTimeString(Value value) {
    LocalTime localTime = value.getLocalTimeValue();
    return new TimeString(localTime.getHour(), localTime.getMinute(), localTime.getSecond())
        .withNanos(localTime.getNano());
  }

  private static TimestampString datetimeValueToTimestampString(Value value) {
    LocalDateTime dateTime = value.getLocalDateTimeValue();
    return new TimestampString(
            dateTime.getYear(),
            dateTime.getMonthValue(),
            dateTime.getDayOfMonth(),
            dateTime.getHour(),
            dateTime.getMinute(),
            dateTime.getSecond())
        .withNanos(dateTime.getNano());
  }

  private static TimestampString timestampValueToTimestampString(Value value) {
    long micros = value.getTimestampUnixMicros();
    if (micros % 1000L != 0) {
      throw new UnsupportedOperationException(
          String.format(
              "%s has sub-millisecond precision, which Beam ZetaSQL does not currently support.",
              micros));
    }
    return TimestampString.fromMillisSinceEpoch(micros / 1000L);
  }
}
