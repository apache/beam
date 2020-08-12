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

import com.google.protobuf.ByteString;
import com.google.zetasql.ArrayType;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.values.Row;

/**
 * Utility methods for ZetaSQL <=> Beam translation.
 *
 * <p>Unsupported ZetaSQL types: INT32, UINT32, UINT64, FLOAT, ENUM, PROTO, GEOGRAPHY
 */
@Internal
public final class ZetaSqlBeamTranslationUtils {

  private static final long MICROS_PER_SECOND = 1000000L;
  private static final long NANOS_PER_MICRO = 1000L;

  private ZetaSqlBeamTranslationUtils() {}

  // Type conversion: Beam => ZetaSQL
  public static Type toZetaSqlType(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT64:
        return TypeFactory.createSimpleType(TypeKind.TYPE_INT64);
      case DOUBLE:
        return TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE);
      case BOOLEAN:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BOOL);
      case STRING:
        return TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
      case BYTES:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BYTES);
      case DECIMAL:
        return TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC);
      case LOGICAL_TYPE:
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          return TypeFactory.createSimpleType(TypeKind.TYPE_DATE);
        } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
          return TypeFactory.createSimpleType(TypeKind.TYPE_TIME);
        } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
          return TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME);
        } else if (SqlTypes.TIMESTAMP.getIdentifier().equals(identifier)) {
          return TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP);
        } else {
          throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
        }
      case ARRAY:
        return toZetaSqlArrayType(fieldType.getCollectionElementType());
      case ROW:
        return toZetaSqlStructType(fieldType.getRowSchema());
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static ArrayType toZetaSqlArrayType(FieldType elementFieldType) {
    return TypeFactory.createArrayType(toZetaSqlType(elementFieldType));
  }

  public static StructType toZetaSqlStructType(Schema schema) {
    return TypeFactory.createStructType(
        schema.getFields().stream()
            .map(f -> new StructField(f.getName(), toZetaSqlType(f.getType())))
            .collect(Collectors.toList()));
  }

  // Value conversion: Beam => ZetaSQL
  public static Value toZetaSqlValue(Object object, FieldType fieldType) {
    if (object == null) {
      return Value.createNullValue(toZetaSqlType(fieldType));
    }
    switch (fieldType.getTypeName()) {
      case INT64:
        return Value.createInt64Value((Long) object);
      case DOUBLE:
        return Value.createDoubleValue((Double) object);
      case BOOLEAN:
        return Value.createBoolValue((Boolean) object);
      case STRING:
        return Value.createStringValue((String) object);
      case BYTES:
        return Value.createBytesValue(ByteString.copyFrom((byte[]) object));
      case DECIMAL:
        return Value.createNumericValue((BigDecimal) object);
      case LOGICAL_TYPE:
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          if (object instanceof Long) { // base type
            return Value.createDateValue(((Long) object).intValue());
          } else { // input type
            return Value.createDateValue((LocalDate) object);
          }
        } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
          LocalTime localTime;
          if (object instanceof Long) { // base type
            localTime = LocalTime.ofNanoOfDay((Long) object);
          } else { // input type
            localTime = (LocalTime) object;
          }
          return Value.createTimeValue(localTime);
        } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
          LocalDateTime datetime;
          if (object instanceof Row) { // base type
            datetime =
                LocalDateTime.of(
                    LocalDate.ofEpochDay(((Row) object).getInt64(DateTime.DATE_FIELD_NAME)),
                    LocalTime.ofNanoOfDay(((Row) object).getInt64(DateTime.TIME_FIELD_NAME)));
          } else { // input type
            datetime = (LocalDateTime) object;
          }
          return Value.createDatetimeValue(datetime);
        } else if (SqlTypes.TIMESTAMP.getIdentifier().equals(identifier)) {
          if (object instanceof Row) { // base type
            return Value.createTimestampValueFromUnixMicros(
                ((Row) object).getInt64(Timestamp.EPOCH_SECOND_FIELD_NAME) * MICROS_PER_SECOND
                    + ((Row) object).getInt32(Timestamp.NANO_FIELD_NAME) / NANOS_PER_MICRO);
          } else { // input type
            return Value.createTimestampValueFromUnixMicros(
                ((Instant) object).getEpochSecond() * MICROS_PER_SECOND
                    + ((Instant) object).getNano() / NANOS_PER_MICRO);
          }
        } else {
          throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
        }
      case ARRAY:
        return toZetaSqlArrayValue((List<Object>) object, fieldType.getCollectionElementType());
      case ROW:
        return toZetaSqlStructValue((Row) object, fieldType.getRowSchema());
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static Value toZetaSqlArrayValue(List<Object> elements, FieldType elementFieldType) {
    List<Value> values =
        elements.stream()
            .map(e -> toZetaSqlValue(e, elementFieldType))
            .collect(Collectors.toList());
    return Value.createArrayValue(toZetaSqlArrayType(elementFieldType), values);
  }

  public static Value toZetaSqlStructValue(Row row, Schema schema) {
    List<Value> values = new ArrayList<>(row.getFieldCount());

    for (int i = 0; i < row.getFieldCount(); i++) {
      values.add(toZetaSqlValue(row.getBaseValue(i, Object.class), schema.getField(i).getType()));
    }
    return Value.createStructValue(toZetaSqlStructType(schema), values);
  }

  // Type conversion: ZetaSQL => Beam
  public static FieldType toBeamType(Type type) {
    switch (type.getKind()) {
      case TYPE_INT64:
        return FieldType.INT64.withNullable(true);
      case TYPE_DOUBLE:
        return FieldType.DOUBLE.withNullable(true);
      case TYPE_BOOL:
        return FieldType.BOOLEAN.withNullable(true);
      case TYPE_STRING:
        return FieldType.STRING.withNullable(true);
      case TYPE_BYTES:
        return FieldType.BYTES.withNullable(true);
      case TYPE_NUMERIC:
        return FieldType.DECIMAL.withNullable(true);
      case TYPE_DATE:
        return FieldType.logicalType(SqlTypes.DATE).withNullable(true);
      case TYPE_TIME:
        return FieldType.logicalType(SqlTypes.TIME).withNullable(true);
      case TYPE_DATETIME:
        return FieldType.logicalType(SqlTypes.DATETIME).withNullable(true);
      case TYPE_TIMESTAMP:
        return FieldType.logicalType(SqlTypes.TIMESTAMP).withNullable(true);
      case TYPE_ARRAY:
        return FieldType.array(toBeamType(type.asArray().getElementType())).withNullable(true);
      case TYPE_STRUCT:
        return FieldType.row(
                type.asStruct().getFieldList().stream()
                    .map(f -> Field.of(f.getName(), toBeamType(f.getType())))
                    .collect(Schema.toSchema()))
            .withNullable(true);
      default:
        throw new UnsupportedOperationException("Unknown ZetaSQL type: " + type.getKind());
    }
  }

  // Value conversion: ZetaSQL => Beam (target Beam type unknown)
  public static Object toBeamObject(Value value, boolean verifyValues) {
    return toBeamObject(value, toBeamType(value.getType()), verifyValues);
  }

  // Value conversion: ZetaSQL => Beam (target Beam type known)
  public static Object toBeamObject(Value value, FieldType fieldType, boolean verifyValues) {
    if (value.isNull()) {
      return null;
    }
    switch (fieldType.getTypeName()) {
      case INT64:
        return value.getInt64Value();
      case DOUBLE:
        // Floats with a floating part equal to zero are treated as whole (INT64).
        // Cast to double when that happens.
        if (value.getType().getKind().equals(TypeKind.TYPE_INT64)) {
          return (double) value.getInt64Value();
        }
        return value.getDoubleValue();
      case BOOLEAN:
        return value.getBoolValue();
      case STRING:
        return value.getStringValue();
      case BYTES:
        return value.getBytesValue().toByteArray();
      case DECIMAL:
        return value.getNumericValue();
      case LOGICAL_TYPE:
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          return value.getLocalDateValue();
        } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
          return value.getLocalTimeValue();
        } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
          return value.getLocalDateTimeValue();
        } else if (SqlTypes.TIMESTAMP.getIdentifier().equals(identifier)) {
          return Instant.ofEpochSecond(
              value.getTimestampUnixMicros() / MICROS_PER_SECOND,
              (value.getTimestampUnixMicros() % MICROS_PER_SECOND) * NANOS_PER_MICRO);
        } else {
          throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
        }
      case ARRAY:
        return toBeamList(value, fieldType.getCollectionElementType(), verifyValues);
      case ROW:
        return toBeamRow(value, fieldType.getRowSchema(), verifyValues);
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static List<Object> toBeamList(
      Value arrayValue, FieldType elementType, boolean verifyValues) {
    return arrayValue.getElementList().stream()
        .map(e -> toBeamObject(e, elementType, verifyValues))
        .collect(Collectors.toList());
  }

  public static Row toBeamRow(Value structValue, Schema schema, boolean verifyValues) {
    List<Object> objects = new ArrayList<>(schema.getFieldCount());
    List<Value> values = structValue.getFieldList();
    for (int i = 0; i < values.size(); i++) {
      objects.add(toBeamObject(values.get(i), schema.getField(i).getType(), verifyValues));
    }
    Row row =
        verifyValues
            ? Row.withSchema(schema).addValues(objects).build()
            : Row.withSchema(schema).attachValues(objects);
    return row;
  }
}
