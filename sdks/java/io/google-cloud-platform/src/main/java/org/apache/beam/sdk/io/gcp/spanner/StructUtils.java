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

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
final class StructUtils {
  public static Row structToBeamRow(Struct struct, Schema schema) {
    Map<String, Object> structValues =
        schema.getFields().stream()
            .collect(
                HashMap::new,
                (map, field) -> map.put(field.getName(), getStructValue(struct, field)),
                Map::putAll);
    return Row.withSchema(schema).withFieldValues(structValues).build();
  }

  public static Struct beamRowToStruct(Row row) {
    Struct.Builder structBuilder = Struct.newBuilder();
    List<Schema.Field> fields = row.getSchema().getFields();
    fields.forEach(
        field -> {
          String column = field.getName();
          switch (field.getType().getTypeName()) {
            case ROW:
              structBuilder
                  .set(column)
                  .to(beamTypeToSpannerType(field.getType()), beamRowToStruct(row.getRow(column)));
              break;
            case ARRAY:
              addIterableToStructBuilder(structBuilder, row.getArray(column), field);
              break;
            case ITERABLE:
              addIterableToStructBuilder(structBuilder, row.getIterable(column), field);
              break;
            case FLOAT:
              structBuilder.set(column).to(row.getFloat(column).doubleValue());
              break;
            case DOUBLE:
              structBuilder.set(column).to(row.getDouble(column));
              break;
            case INT16:
              structBuilder.set(column).to(row.getInt16(column).longValue());
              break;
            case INT32:
              structBuilder.set(column).to(row.getInt32(column).longValue());
              break;
            case INT64:
              structBuilder.set(column).to(row.getInt64(column).longValue());
              break;
            case DECIMAL:
              structBuilder.set(column).to(row.getDecimal(column));
              break;
              // TODO: implement logical type date and timestamp
            case DATETIME:
              structBuilder
                  .set(column)
                  .to(Timestamp.parseTimestamp(row.getDateTime(column).toString()));
              break;
            case STRING:
              structBuilder.set(column).to(row.getString(column));
              break;
            case BYTE:
              structBuilder.set(column).to(row.getByte(column));
              break;
            case BYTES:
              structBuilder.set(column).to(ByteArray.copyFrom(row.getBytes(column)));
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
        return Type.array(simpleBeamTypeToSpannerType(beamType.getCollectionElementType()));
      default:
        return simpleBeamTypeToSpannerType(beamType);
    }
  }

  private static Type simpleBeamTypeToSpannerType(Schema.FieldType beamType) {
    switch (beamType.getTypeName()) {
      case ROW:
        return Type.struct(translateRowFieldsToStructFields(beamType.getRowSchema().getFields()));
      case BYTES:
        return Type.bytes();
      case BYTE:
      case INT64:
      case INT32:
      case INT16:
        return Type.int64();
      case DOUBLE:
      case FLOAT:
        return Type.float64();
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
      Struct.Builder structBuilder, Iterable<Object> iterable, Schema.Field field) {
    String column = field.getName();
    Schema.FieldType beamIterableType = field.getType().getCollectionElementType();
    Schema.TypeName beamIterableTypeName = beamIterableType.getTypeName();
    switch (beamIterableTypeName) {
      case ROW:
        structBuilder
            .set(column)
            .toStructArray(
                beamTypeToSpannerType(beamIterableType),
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(row -> beamRowToStruct((Row) row))
                    .collect(toList()));
        break;
      case INT16:
      case INT32:
      case INT64:
      case BYTE:
        structBuilder.set(column).toInt64Array((Iterable<Long>) ((Object) iterable));
        break;
      case FLOAT:
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
        structBuilder
            .set(column)
            .toBytesArray(
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(bytes -> ByteArray.copyFrom((byte[]) bytes))
                    .collect(toList()));
        break;
      case STRING:
        structBuilder.set(column).toStringArray((Iterable<String>) ((Object) iterable));
        break;
        // TODO: implement logical date and datetime
      case DATETIME:
        structBuilder
            .set(column)
            .toTimestampArray(
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(timestamp -> Timestamp.parseTimestamp(timestamp.toString()))
                    .collect(toList()));
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported iterable type while translating row to struct: %s",
                field.getType().getCollectionElementType().getTypeName()));
    }
  }

  private static Object getStructValue(Struct struct, Schema.Field field) {
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
        return structToBeamRow(struct.getStruct(column), field.getType().getRowSchema());
      default:
        throw new RuntimeException(
            String.format("Unsupported spanner type %s for column %s.", typeCode, column));
    }
  }

  private static Object getStructArrayValue(Struct struct, Type arrayType, Schema.Field field) {
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
      case FLOAT64:
        return struct.getDoubleList(column);
      case STRING:
        return struct.getStringList(column);
      case NUMERIC:
        return struct.getBigDecimal(column);
      case ARRAY:
        throw new IllegalStateException(
            String.format("Column %s has array of arrays which is prohibited in Spanner.", column));
      case STRUCT:
        return struct.getStructList(column).stream()
            .map(
                structElem ->
                    structToBeamRow(
                        structElem, field.getType().getCollectionElementType().getRowSchema()))
            .collect(toList());
      default:
        throw new RuntimeException(
            String.format("Unsupported spanner array type %s for column %s.", arrayCode, column));
    }
  }
}
