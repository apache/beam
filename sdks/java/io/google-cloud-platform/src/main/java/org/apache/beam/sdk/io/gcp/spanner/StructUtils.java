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
package org.apache.beam.sdk.io.gcp.spanner;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.spanner.v1.StructType;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

final class StructUtils {

  private static final SpannerIO.Read.ToBeamRowFunction STRUCT_TO_BEAM_ROW_FUNCTION =
      schema -> (Struct struct) -> structToBeamRow(struct, schema);

  public static SpannerIO.Read.ToBeamRowFunction structToBeamRow() {
    return STRUCT_TO_BEAM_ROW_FUNCTION;
  }

  private static final SpannerIO.Read.FromBeamRowFunction STRUCT_FROM_BEAM_ROW_FUNCTION =
      ignored -> StructUtils::beamRowToStruct;

  public static SpannerIO.Read.FromBeamRowFunction structFromBeamRow() {
    return STRUCT_FROM_BEAM_ROW_FUNCTION;
  }

  // It's not possible to pass nulls as values even with a field is nullable
  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  public static Row structToBeamRow(Struct struct, Schema schema) {
    Map<String, @Nullable Object> structValues =
        schema.getFields().stream()
            .collect(
                HashMap::new,
                (map, field) -> map.put(field.getName(), getStructValue(struct, field)),
                Map::putAll);
    return Row.withSchema(schema).withFieldValues(structValues).build();
  }

  public static Schema structTypeToBeamRowSchema(StructType structType, boolean isRead) {
    Schema.Builder beamSchema = Schema.builder();
    structType
        .getFieldsList()
        .forEach(
            field -> {
              Schema.FieldType fieldType;
              try {
                fieldType = convertSpannerTypeToBeamFieldType(field.getType());
              } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Error processing struct to row: " + e.getMessage());
              }
              // Treat reads from Spanner as Nullable and leave Null handling to Spanner
              if (isRead) {
                beamSchema.addNullableField(field.getName(), fieldType);
              } else {
                beamSchema.addField(field.getName(), fieldType);
              }
            });
    return beamSchema.build();
  }

  public static Schema.FieldType convertSpannerTypeToBeamFieldType(
      com.google.spanner.v1.Type spannerType) {
    switch (spannerType.getCode()) {
      case BOOL:
        return Schema.FieldType.BOOLEAN;
      case BYTES:
        return Schema.FieldType.BYTES;
      case TIMESTAMP:
      case DATE:
        return Schema.FieldType.DATETIME;
      case INT64:
        return Schema.FieldType.INT64;
      case FLOAT32:
        return Schema.FieldType.FLOAT;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case NUMERIC:
        return Schema.FieldType.DECIMAL;
      case ARRAY:
        return Schema.FieldType.array(
            convertSpannerTypeToBeamFieldType(spannerType.getArrayElementType()));
      case STRUCT:
        throw new IllegalArgumentException(
            String.format("Unsupported type '%s'.", spannerType.getCode()));
      default:
        return Schema.FieldType.STRING;
    }
  }

  public static Struct beamRowToStruct(Row row) {
    Struct.Builder structBuilder = Struct.newBuilder();
    List<Schema.Field> fields = row.getSchema().getFields();
    fields.forEach(
        field -> {
          String column = field.getName();
          switch (field.getType().getTypeName()) {
            case ROW:
              @Nullable Row subRow = row.getRow(column);
              if (subRow == null) {
                structBuilder.set(column).to(beamTypeToSpannerType(field.getType()), null);
              } else {
                structBuilder
                    .set(column)
                    .to(beamTypeToSpannerType(field.getType()), beamRowToStruct(subRow));
              }
              break;
            case ARRAY:
              addIterableToStructBuilder(structBuilder, row.getArray(column), field);
              break;
            case ITERABLE:
              addIterableToStructBuilder(structBuilder, row.getIterable(column), field);
              break;
            case FLOAT:
              structBuilder.set(column).to(row.getFloat(column));
              break;
            case DOUBLE:
              structBuilder.set(column).to(row.getDouble(column));
              break;
            case INT16:
              @Nullable Short int16 = row.getInt16(column);
              if (int16 == null) {
                structBuilder.set(column).to((Long) null);
              } else {
                structBuilder.set(column).to(int16);
              }
              break;
            case INT32:
              @Nullable Integer int32 = row.getInt32(column);
              if (int32 == null) {
                structBuilder.set(column).to((Long) null);
              } else {
                structBuilder.set(column).to(int32);
              }
              break;
            case INT64:
              structBuilder.set(column).to(row.getInt64(column));
              break;
            case DECIMAL:
              @Nullable BigDecimal decimal = row.getDecimal(column);
              // BigDecimal is not nullable
              if (decimal == null) {
                checkNotNull(decimal, "Null decimal at column " + column);
              } else {
                structBuilder.set(column).to(decimal);
              }
              break;
              // TODO: implement logical type date and timestamp
            case DATETIME:
              @Nullable ReadableDateTime dateTime = row.getDateTime(column);
              if (dateTime == null) {
                structBuilder.set(column).to((Timestamp) null);
              } else {
                structBuilder.set(column).to(Timestamp.parseTimestamp(dateTime.toString()));
              }
              break;
            case STRING:
              structBuilder.set(column).to(row.getString(column));
              break;
            case BYTE:
              @Nullable Byte byteValue = row.getByte(column);
              if (byteValue == null) {
                structBuilder.set(column).to((Long) null);
              } else {
                structBuilder.set(column).to(byteValue);
              }
              break;
            case BYTES:
              byte @Nullable [] bytes = row.getBytes(column);
              if (bytes == null) {
                structBuilder.set(column).to((ByteArray) null);
              } else {
                structBuilder.set(column).to(ByteArray.copyFrom(bytes));
              }
              break;
            case BOOLEAN:
              structBuilder.set(column).to(row.getBoolean(column));
              break;
            default:
              throw new IllegalArgumentException(
                  String.format(
                      "Unsupported beam type '%s' while translating row to struct.",
                      field.getType().getTypeName()));
          }
        });
    return structBuilder.build();
  }

  public static Type beamTypeToSpannerType(Schema.FieldType beamType) {
    switch (beamType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        Schema.@Nullable FieldType elementType = beamType.getCollectionElementType();
        if (elementType == null) {
          throw new NullPointerException("Null element type");
        } else {
          return Type.array(simpleBeamTypeToSpannerType(elementType));
        }
      default:
        return simpleBeamTypeToSpannerType(beamType);
    }
  }

  private static Type simpleBeamTypeToSpannerType(Schema.FieldType beamType) {
    switch (beamType.getTypeName()) {
      case ROW:
        @Nullable Schema schema = beamType.getRowSchema();
        if (schema == null) {
          throw new NullPointerException("Null schema");
        } else {
          return Type.struct(translateRowFieldsToStructFields(schema.getFields()));
        }
      case BYTES:
        return Type.bytes();
      case BYTE:
      case INT64:
      case INT32:
      case INT16:
        return Type.int64();
      case DOUBLE:
        return Type.float64();
      case FLOAT:
        return Type.float32();
      case DECIMAL:
        return Type.numeric();
      case STRING:
        return Type.string();
      case BOOLEAN:
        return Type.bool();
        // TODO: implement logical type date and timestamp
      case DATETIME:
        return Type.timestamp();
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unable to translate beam type %s to Spanner type", beamType.getTypeName()));
    }
  }

  private static Iterable<Type.StructField> translateRowFieldsToStructFields(
      List<Schema.Field> rowFields) {
    return rowFields.stream()
        .map(field -> Type.StructField.of(field.getName(), beamTypeToSpannerType(field.getType())))
        .collect(toList());
  }

  @SuppressWarnings("unchecked")
  private static void addIterableToStructBuilder(
      Struct.Builder structBuilder, @Nullable Iterable<Object> iterable, Schema.Field field) {
    String column = field.getName();
    Schema.FieldType beamIterableType = field.getType().getCollectionElementType();
    if (beamIterableType == null) {
      throw new NullPointerException("Null collection element type at field " + field.getName());
    }
    Schema.TypeName beamIterableTypeName = beamIterableType.getTypeName();
    switch (beamIterableTypeName) {
      case ROW:
        if (iterable == null) {
          structBuilder.set(column).toStructArray(beamTypeToSpannerType(beamIterableType), null);
        } else {
          structBuilder
              .set(column)
              .toStructArray(
                  beamTypeToSpannerType(beamIterableType),
                  StreamSupport.stream(iterable.spliterator(), false)
                      .map(row -> beamRowToStruct((Row) row))
                      .collect(toList()));
        }
        break;
      case INT16:
      case INT32:
      case INT64:
      case BYTE:
        structBuilder.set(column).toInt64Array((Iterable<Long>) ((Object) iterable));
        break;
      case FLOAT:
        structBuilder.set(column).toFloat32Array((Iterable<Float>) ((Object) iterable));
        break;
      case DOUBLE:
        structBuilder.set(column).toFloat64Array((Iterable<Double>) ((Object) iterable));
        break;
      case DECIMAL:
        structBuilder.set(column).toNumericArray((Iterable<BigDecimal>) ((Object) iterable));
        break;
      case BOOLEAN:
        structBuilder.set(column).toBoolArray((Iterable<Boolean>) ((Object) iterable));
        break;
      case BYTES:
        if (iterable == null) {
          structBuilder.set(column).toBytesArray(null);
        } else {
          structBuilder
              .set(column)
              .toBytesArray(
                  StreamSupport.stream(iterable.spliterator(), false)
                      .map(bytes -> ByteArray.copyFrom((byte[]) bytes))
                      .collect(toList()));
        }
        break;
      case STRING:
        structBuilder.set(column).toStringArray((Iterable<String>) ((Object) iterable));
        break;
        // TODO: implement logical date and datetime
      case DATETIME:
        if (iterable == null) {
          structBuilder.set(column).toTimestampArray(null);
        } else {
          structBuilder
              .set(column)
              .toTimestampArray(
                  StreamSupport.stream(iterable.spliterator(), false)
                      .map(timestamp -> Timestamp.parseTimestamp(timestamp.toString()))
                      .collect(toList()));
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported iterable type '%s' while translating row to struct.",
                beamIterableType.getTypeName()));
    }
  }

  private static @Nullable Object getStructValue(Struct struct, Schema.Field field) {
    String column = field.getName();
    Type.Code typeCode = struct.getColumnType(column).getCode();
    if (struct.isNull(column)) {
      return null;
    }
    switch (typeCode) {
      case BOOL:
        return struct.getBoolean(column);
      case BYTES:
        return struct.getBytes(column).toByteArray();
        // TODO: implement logical datetime
      case TIMESTAMP:
        return Instant.ofEpochSecond(struct.getTimestamp(column).getSeconds()).toDateTime();
        // TODO: implement logical date
      case DATE:
        return DateTime.parse(struct.getDate(column).toString());
      case INT64:
        return struct.getLong(column);
      case FLOAT32:
        return struct.getFloat(column);
      case FLOAT64:
        return struct.getDouble(column);
      case NUMERIC:
        return struct.getBigDecimal(column);
      case STRING:
        return struct.getString(column);
      case ARRAY:
        return getStructArrayValue(
            struct, struct.getColumnType(column).getArrayElementType(), field);
      case STRUCT:
        @Nullable Schema schema = field.getType().getRowSchema();
        if (schema == null) {
          throw new NullPointerException("Null schema at field " + field.getName());
        } else {
          return structToBeamRow(struct.getStruct(column), schema);
        }
      default:
        throw new RuntimeException(
            String.format("Unsupported spanner type %s for column %s.", typeCode, column));
    }
  }

  private static @Nullable Object getStructArrayValue(
      Struct struct, Type arrayType, Schema.Field field) {
    Type.Code arrayCode = arrayType.getCode();
    String column = field.getName();
    if (struct.isNull(column)) {
      return null;
    }
    switch (arrayCode) {
      case BOOL:
        return struct.getBooleanList(column);
      case BYTES:
        return struct.getBytesList(column);
        // TODO: implement logical datetime
      case TIMESTAMP:
        return struct.getTimestampList(column).stream()
            .map(timestamp -> Instant.ofEpochSecond(timestamp.getSeconds()).toDateTime())
            .collect(toList());
        // TODO: implement logical date
      case DATE:
        return struct.getDateList(column).stream()
            .map(date -> DateTime.parse(date.toString()))
            .collect(toList());
      case INT64:
        return struct.getLongList(column);
      case FLOAT32:
        return struct.getFloatList(column);
      case FLOAT64:
        return struct.getDoubleList(column);
      case STRING:
        return struct.getStringList(column);
      case NUMERIC:
        return struct.getBigDecimalList(column);
      case ARRAY:
        throw new IllegalStateException(
            String.format("Column %s has array of arrays which is prohibited in Spanner.", column));
      case STRUCT:
        return struct.getStructList(column).stream()
            .map(
                structElem -> {
                  Schema.@Nullable FieldType fieldType = field.getType().getCollectionElementType();
                  if (fieldType == null) {
                    throw new NullPointerException(
                        "Null collection element type at field " + field.getName());
                  }

                  @Nullable Schema elementSchema = fieldType.getRowSchema();
                  if (elementSchema == null) {
                    throw new NullPointerException(
                        "Null schema element type at field " + field.getName());
                  }
                  return structToBeamRow(structElem, elementSchema);
                })
            .collect(toList());
      default:
        throw new RuntimeException(
            String.format("Unsupported spanner array type %s for column %s.", arrayCode, column));
    }
  }
}
