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
import com.google.zetasql.CivilTimeEncoder;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.LongMath;
import org.joda.time.Instant;

/**
 * Utility methods for ZetaSQL <=> Beam translation.
 *
 * <p>Unsupported ZetaSQL types: INT32, UINT32, UINT64, FLOAT, ENUM, PROTO, GEOGRAPHY
 * TODO[BEAM-10238]: support ZetaSQL types: TIME, DATETIME, NUMERIC
 */
@Internal
public final class ZetaSqlBeamTranslationUtils {

  private static final long MICROS_PER_MILLI = 1000L;

  private ZetaSqlBeamTranslationUtils() {}

  // Type conversion: Beam => ZetaSQL
  public static Type beamFieldTypeToZetaSqlType(FieldType fieldType) {
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
      case DATETIME:
        // TODO[BEAM-10238]: Mapping TIMESTAMP to a Beam LogicalType instead?
        return TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP);
      case ARRAY:
        return beamElementFieldTypeToZetaSqlArrayType(fieldType.getCollectionElementType());
      case ROW:
        return beamSchemaToZetaSqlStructType(fieldType.getRowSchema());
      case LOGICAL_TYPE:
        return beamLogicalTypeToZetaSqlType(fieldType.getLogicalType().getIdentifier());
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static ArrayType beamElementFieldTypeToZetaSqlArrayType(FieldType elementFieldType) {
    return TypeFactory.createArrayType(beamFieldTypeToZetaSqlType(elementFieldType));
  }

  public static StructType beamSchemaToZetaSqlStructType(Schema schema) {
    return TypeFactory.createStructType(
        schema.getFields().stream()
            .map(ZetaSqlBeamTranslationUtils::beamFieldToZetaSqlStructField)
            .collect(Collectors.toList()));
  }

  private static StructField beamFieldToZetaSqlStructField(Field field) {
    return new StructField(field.getName(), beamFieldTypeToZetaSqlType(field.getType()));
  }

  private static Type beamLogicalTypeToZetaSqlType(String identifier) {
    if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
      // Date type
      return TypeFactory.createSimpleType(TypeKind.TYPE_DATE);
    } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
      // Time type
      return TypeFactory.createSimpleType(TypeKind.TYPE_TIME);
    } else {
      throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
    }
  }

  // Value conversion: Beam => ZetaSQL
  public static Value javaObjectToZetaSqlValue(Object object, FieldType fieldType) {
    if (object == null) {
      return Value.createNullValue(beamFieldTypeToZetaSqlType(fieldType));
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
      case DATETIME:
        return jodaInstantToZetaSqlTimestampValue((Instant) object);
      case ARRAY:
        return javaListToZetaSqlArrayValue(
            (List<Object>) object, fieldType.getCollectionElementType());
      case ROW:
        return beamRowToZetaSqlStructValue((Row) object, fieldType.getRowSchema());
      case LOGICAL_TYPE:
        return beamLogicalObjectToZetaSqlValue(object, fieldType.getLogicalType().getIdentifier());
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static Value jodaInstantToZetaSqlTimestampValue(Instant instant) {
    return Value.createTimestampValueFromUnixMicros(
        LongMath.checkedMultiply(instant.getMillis(), MICROS_PER_MILLI));
  }

  private static Value javaListToZetaSqlArrayValue(List<Object> elements, FieldType elementType) {
    List<Value> values =
        elements.stream()
            .map(e -> javaObjectToZetaSqlValue(e, elementType))
            .collect(Collectors.toList());
    return Value.createArrayValue(beamElementFieldTypeToZetaSqlArrayType(elementType), values);
  }

  public static Value beamRowToZetaSqlStructValue(Row row, Schema schema) {
    List<Value> values = new ArrayList<>(row.getFieldCount());

    for (int i = 0; i < row.getFieldCount(); i++) {
      values.add(
          javaObjectToZetaSqlValue(
              row.getBaseValue(i, Object.class), schema.getField(i).getType()));
    }
    return Value.createStructValue(beamSchemaToZetaSqlStructType(schema), values);
  }

  private static Value beamLogicalObjectToZetaSqlValue(Object object, String identifier) {
    if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
      // Date value
      if (object instanceof Long) { // base type
        return Value.createDateValue(((Long) object).intValue());
      } else { // input type
        return Value.createDateValue((int) ((LocalDate) object).toEpochDay());
      }
    } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
      // Time value
      if (object instanceof Long) { // base type
        return Value.createTimeValue(
            CivilTimeEncoder.encodePacked64TimeNanos(LocalTime.ofNanoOfDay((Long) object)));
      } else { // input type
        return Value.createTimeValue(CivilTimeEncoder.encodePacked64TimeNanos((LocalTime) object));
      }
    } else {
      throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
    }
  }

  // Type conversion: ZetaSQL => Beam
  public static FieldType zetaSqlTypeToBeamFieldType(Type type) {
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
      case TYPE_TIMESTAMP:
        return FieldType.DATETIME.withNullable(true);
      case TYPE_ARRAY:
        return zetaSqlElementTypeToBeamArrayType(type.asArray().getElementType());
      case TYPE_STRUCT:
        return zetaSqlStructTypeToBeamRowType(type.asStruct());
      default:
        throw new UnsupportedOperationException("Unknown ZetaSQL type: " + type.getKind());
    }
  }

  private static FieldType zetaSqlElementTypeToBeamArrayType(Type elementType) {
    return FieldType.array(zetaSqlTypeToBeamFieldType(elementType)).withNullable(true);
  }

  private static FieldType zetaSqlStructTypeToBeamRowType(StructType structType) {
    return FieldType.row(
            structType.getFieldList().stream()
                .map(ZetaSqlBeamTranslationUtils::zetaSqlStructFieldToBeamField)
                .collect(Schema.toSchema()))
        .withNullable(true);
  }

  private static Field zetaSqlStructFieldToBeamField(StructField structField) {
    return Field.of(structField.getName(), zetaSqlTypeToBeamFieldType(structField.getType()));
  }

  // Value conversion: ZetaSQL => Beam (target Beam type unknown)
  public static Object zetaSqlValueToJavaObject(Value value, boolean verifyValues) {
    return zetaSqlValueToJavaObject(
        value, zetaSqlTypeToBeamFieldType(value.getType()), verifyValues);
  }

  // Value conversion: ZetaSQL => Beam (target Beam type known)
  public static Object zetaSqlValueToJavaObject(
      Value value, FieldType fieldType, boolean verifyValues) {
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
      case DATETIME:
        return zetaSqlTimestampValueToJodaInstant(value);
      case ARRAY:
        return zetaSqlArrayValueToJavaList(
            value, fieldType.getCollectionElementType(), verifyValues);
      case ROW:
        return zetaSqlStructValueToBeamRow(value, fieldType.getRowSchema(), verifyValues);
      case LOGICAL_TYPE:
        return zetaSqlValueToBeamLogicalObject(value, fieldType.getLogicalType().getIdentifier());
      default:
        throw new UnsupportedOperationException(
            "Unknown Beam fieldType: " + fieldType.getTypeName());
    }
  }

  private static Instant zetaSqlTimestampValueToJodaInstant(Value timestampValue) {
    long millis = timestampValue.getTimestampUnixMicros() / MICROS_PER_MILLI;
    return Instant.ofEpochMilli(millis);
  }

  private static List<Object> zetaSqlArrayValueToJavaList(
      Value arrayValue, FieldType elementType, boolean verifyValues) {
    return arrayValue.getElementList().stream()
        .map(e -> zetaSqlValueToJavaObject(e, elementType, verifyValues))
        .collect(Collectors.toList());
  }

  public static Row zetaSqlStructValueToBeamRow(
      Value structValue, Schema schema, boolean verifyValues) {
    List<Object> objects = new ArrayList<>(schema.getFieldCount());
    List<Value> values = structValue.getFieldList();
    for (int i = 0; i < values.size(); i++) {
      objects.add(
          zetaSqlValueToJavaObject(values.get(i), schema.getField(i).getType(), verifyValues));
    }
    Row row =
        verifyValues
            ? Row.withSchema(schema).addValues(objects).build()
            : Row.withSchema(schema).attachValues(objects);
    return row;
  }

  private static Object zetaSqlValueToBeamLogicalObject(Value value, String identifier) {
    if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
      // Date value
      return LocalDate.ofEpochDay(value.getDateValue());
    } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
      // Time value
      return CivilTimeEncoder.decodePacked64TimeNanosAsJavaTime(value.getTimeValue());
    } else {
      throw new UnsupportedOperationException("Unknown Beam logical type: " + identifier);
    }
  }
}
