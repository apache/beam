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
import static org.apache.beam.sdk.io.gcp.spanner.StructUtils.beamRowToStruct;
import static org.apache.beam.sdk.io.gcp.spanner.StructUtils.beamTypeToSpannerType;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableDateTime;

final class MutationUtils {
  private MutationUtils() {}

  /**
   * Check if the mutation is a delete by a single primary key operation.
   *
   * @param m mutation
   * @return true if mutation is a point delete
   */
  public static boolean isPointDelete(Mutation m) {
    return m.getOperation() == Mutation.Op.DELETE
        && Iterables.isEmpty(m.getKeySet().getRanges())
        && Iterables.size(m.getKeySet().getKeys()) == 1;
  }

  /**
   * Utility function to convert row to mutation for cross-language usage.
   *
   * @return function that can convert row to mutation
   */
  public static SerializableFunction<Row, Mutation> beamRowToMutationFn(
      Mutation.Op operation, String table) {
    return (row -> {
      switch (operation) {
        case INSERT:
          return MutationUtils.createMutationFromBeamRows(Mutation.newInsertBuilder(table), row);
        case DELETE:
          return Mutation.delete(table, MutationUtils.createKeyFromBeamRow(row));
        case UPDATE:
          return MutationUtils.createMutationFromBeamRows(Mutation.newUpdateBuilder(table), row);
        case REPLACE:
          return MutationUtils.createMutationFromBeamRows(Mutation.newReplaceBuilder(table), row);
        case INSERT_OR_UPDATE:
          return MutationUtils.createMutationFromBeamRows(
              Mutation.newInsertOrUpdateBuilder(table), row);
        default:
          throw new IllegalArgumentException(
              String.format("Unknown mutation operation type: %s", operation));
      }
    });
  }

  private static Key createKeyFromBeamRow(Row row) {
    Key.Builder builder = Key.newBuilder();
    Schema schema = row.getSchema();
    List<String> columns = schema.getFieldNames();
    columns.forEach(
        columnName ->
            setBeamValueToKey(builder, schema.getField(columnName).getType(), columnName, row));
    return builder.build();
  }

  public static Mutation createMutationFromBeamRows(
      Mutation.WriteBuilder mutationBuilder, Row row) {
    Schema schema = row.getSchema();
    List<String> columns = schema.getFieldNames();
    columns.forEach(
        columnName ->
            setBeamValueToMutation(
                mutationBuilder, schema.getField(columnName).getType(), columnName, row));
    return mutationBuilder.build();
  }

  private static void setBeamValueToKey(
      Key.Builder keyBuilder, Schema.FieldType field, String columnName, Row row) {
    switch (field.getTypeName()) {
      case BYTE:
        @Nullable Byte byteValue = row.getByte(columnName);
        if (byteValue == null) {
          keyBuilder.append((Long) null);
        } else {
          keyBuilder.append(byteValue);
        }
        break;
      case INT16:
        @Nullable Short int16 = row.getInt16(columnName);
        if (int16 == null) {
          keyBuilder.append((Long) null);
        } else {
          keyBuilder.append(int16);
        }
        break;
      case INT32:
        @Nullable Integer int32 = row.getInt32(columnName);
        if (int32 == null) {
          keyBuilder.append((Long) null);
        } else {
          keyBuilder.append(int32);
        }
        break;
      case INT64:
        keyBuilder.append(row.getInt64(columnName));
        break;
      case FLOAT:
        @Nullable Float floatValue = row.getFloat(columnName);
        if (floatValue == null) {
          keyBuilder.append((Double) null);
        } else {
          keyBuilder.append(floatValue);
        }
        break;
      case DOUBLE:
        keyBuilder.append(row.getDouble(columnName));
        break;
      case DECIMAL:
        keyBuilder.append(row.getDecimal(columnName));
        break;
        // TODO: Implement logical date and datetime
      case DATETIME:
        @Nullable ReadableDateTime dateTime = row.getDateTime(columnName);
        if (dateTime == null) {
          keyBuilder.append((Timestamp) null);
        } else {
          keyBuilder.append(
              Timestamp.ofTimeMicroseconds(dateTime.toInstant().getMillis() * 1_000L));
        }
        break;
      case BOOLEAN:
        keyBuilder.append(row.getBoolean(columnName));
        break;
      case STRING:
        keyBuilder.append(row.getString(columnName));
        break;
      case BYTES:
        byte @Nullable [] bytes = row.getBytes(columnName);
        if (bytes == null) {
          keyBuilder.append((ByteArray) null);
        } else {
          keyBuilder.append(ByteArray.copyFrom(bytes));
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported field type: %s", field.getTypeName()));
    }
  }

  private static void setBeamValueToMutation(
      Mutation.WriteBuilder mutationBuilder,
      Schema.FieldType fieldType,
      String columnName,
      Row row) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        @Nullable Byte byteValue = row.getByte(columnName);
        if (byteValue == null) {
          mutationBuilder.set(columnName).to(((Long) null));
        } else {
          mutationBuilder.set(columnName).to(byteValue);
        }
        break;
      case INT16:
        @Nullable Short int16 = row.getInt16(columnName);
        if (int16 == null) {
          mutationBuilder.set(columnName).to(((Long) null));
        } else {
          mutationBuilder.set(columnName).to(int16);
        }
        break;
      case INT32:
        @Nullable Integer int32 = row.getInt32(columnName);
        if (int32 == null) {
          mutationBuilder.set(columnName).to(((Long) null));
        } else {
          mutationBuilder.set(columnName).to(int32);
        }
        break;
      case INT64:
        mutationBuilder.set(columnName).to(row.getInt64(columnName));
        break;
      case FLOAT:
        mutationBuilder.set(columnName).to(row.getFloat(columnName));
        break;
      case DOUBLE:
        mutationBuilder.set(columnName).to(row.getDouble(columnName));
        break;
      case DECIMAL:
        @Nullable BigDecimal decimal = row.getDecimal(columnName);
        // BigDecimal is not nullable
        if (decimal == null) {
          checkNotNull(decimal, "Null decimal at column " + columnName);
        } else {
          mutationBuilder.set(columnName).to(decimal);
        }
        break;
        // TODO: Implement logical date and datetime
      case DATETIME:
        @Nullable ReadableDateTime dateTime = row.getDateTime(columnName);
        if (dateTime == null) {
          mutationBuilder.set(columnName).to(((Timestamp) null));
        } else {
          mutationBuilder
              .set(columnName)
              .to(Timestamp.ofTimeMicroseconds(dateTime.toInstant().getMillis() * 1000L));
        }
        break;
      case BOOLEAN:
        mutationBuilder.set(columnName).to(row.getBoolean(columnName));
        break;
      case STRING:
        mutationBuilder.set(columnName).to(row.getString(columnName));
        break;
      case BYTES:
        byte @Nullable [] bytes = row.getBytes(columnName);
        if (bytes == null) {
          mutationBuilder.set(columnName).to(((ByteArray) null));
        } else {
          mutationBuilder.set(columnName).to(ByteArray.copyFrom(bytes));
        }
        break;
      case ROW:
        @Nullable Row subRow = row.getRow(columnName);
        if (subRow == null) {
          mutationBuilder
              .set(columnName)
              .to(beamTypeToSpannerType(row.getSchema().getField(columnName).getType()), null);
        } else {
          mutationBuilder
              .set(columnName)
              .to(
                  beamTypeToSpannerType(row.getSchema().getField(columnName).getType()),
                  beamRowToStruct(subRow));
        }
        break;
      case ARRAY:
        addIterableToMutationBuilder(
            mutationBuilder, row.getArray(columnName), row.getSchema().getField(columnName));
        break;
      case ITERABLE:
        addIterableToMutationBuilder(
            mutationBuilder, row.getIterable(columnName), row.getSchema().getField(columnName));
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported field type: %s", fieldType.getTypeName()));
    }
  }

  @SuppressWarnings("unchecked")
  private static void addIterableToMutationBuilder(
      Mutation.WriteBuilder mutationBuilder,
      @Nullable Iterable<Object> iterable,
      Schema.Field field) {
    String column = field.getName();
    Schema.FieldType beamIterableType = field.getType().getCollectionElementType();
    if (beamIterableType == null) {
      throw new NullPointerException("Null collection element type at field " + field.getName());
    }
    Schema.TypeName beamIterableTypeName = beamIterableType.getTypeName();
    switch (beamIterableTypeName) {
      case ROW:
        if (iterable == null) {
          mutationBuilder.set(column).toStructArray(beamTypeToSpannerType(beamIterableType), null);
        } else {
          mutationBuilder
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
        mutationBuilder.set(column).toInt64Array((Iterable<Long>) ((Object) iterable));
        break;
      case FLOAT:
        mutationBuilder.set(column).toFloat32Array((Iterable<Float>) ((Object) iterable));
        break;
      case DOUBLE:
        mutationBuilder.set(column).toFloat64Array((Iterable<Double>) ((Object) iterable));
        break;
      case DECIMAL:
        mutationBuilder.set(column).toNumericArray((Iterable<BigDecimal>) ((Object) iterable));
        break;
      case BOOLEAN:
        mutationBuilder.set(column).toBoolArray((Iterable<Boolean>) ((Object) iterable));
        break;
      case BYTES:
        if (iterable == null) {
          mutationBuilder.set(column).toBytesArray(null);
        } else {
          mutationBuilder
              .set(column)
              .toBytesArray(
                  StreamSupport.stream(iterable.spliterator(), false)
                      .map(object -> ByteArray.copyFrom((byte[]) object))
                      .collect(toList()));
        }
        break;
      case STRING:
        mutationBuilder.set(column).toStringArray((Iterable<String>) ((Object) iterable));
        break;
      case DATETIME:
        if (iterable == null) {
          mutationBuilder.set(column).toDateArray(null);
        } else {
          mutationBuilder
              .set(column)
              .toTimestampArray(
                  StreamSupport.stream(iterable.spliterator(), false)
                      .map(datetime -> Timestamp.parseTimestamp(datetime.toString()))
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

  public static Row createRowFromMutation(Schema schema, Mutation mutation) {
    Map<String, Object> mutationHashMap = new HashMap<>();
    mutation
        .asMap()
        .forEach(
            (column, value) -> mutationHashMap.put(column, convertValueToBeamFieldType(value)));
    return Row.withSchema(schema).withFieldValues(mutationHashMap).build();
  }

  public static Object convertValueToBeamFieldType(Value value) {
    switch (value.getType().getCode()) {
      case BOOL:
        return value.getBool();
      case BYTES:
        return value.getBytes();
      case DATE:
        return value.getDate();
      case INT64:
        return value.getInt64();
      case FLOAT64:
        return value.getFloat64();
      case NUMERIC:
        return value.getNumeric();
      case TIMESTAMP:
        return value.getTimestamp();
      case STRING:
        return value.getString();
      case JSON:
        return value.getJson();
      case ARRAY:
        switch (value.getType().getArrayElementType().getCode()) {
          case BOOL:
            return value.getBoolArray();
          case BYTES:
            return value.getBytesArray();
          case DATE:
            return value.getDateArray();
          case INT64:
            return value.getInt64Array();
          case FLOAT64:
            return value.getFloat64Array();
          case NUMERIC:
            return value.getNumericArray();
          case TIMESTAMP:
            return value.getTimestampArray();
          case STRING:
            return value.getStringArray();
          case JSON:
            return value.getJsonArray();
          default:
            return value.toString();
        }
      default:
        return value.toString();
    }
  }
}
