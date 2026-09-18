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
package org.apache.beam.sdk.io.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.types.Binary;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Utility methods for MongoDB IO. */
public class MongoDbUtils {

  /** Converts a Beam {@link Row} to a BSON {@link Document}. */
  public static Document toDocument(Row row) {
    Object converted = convertToBsonValue(row);
    if (converted instanceof Document) {
      return (Document) converted;
    }
    throw new IllegalArgumentException(
        "Expected Document but got "
            + (converted != null ? converted.getClass().getName() : "null"));
  }

  private static @Nullable Object convertToBsonValue(@Nullable Object value) {
    if (value == null) {
      return new BsonNull();
    }
    if (value instanceof Row) {
      Row row = (Row) value;
      Document doc = new Document();
      for (Field field : row.getSchema().getFields()) {
        Object fieldValue = row.getValue(field.getName());
        Object converted = convertToBsonValue(fieldValue);
        doc.append(field.getName(), converted != null ? converted : new BsonNull());
      }
      return doc;
    } else if (value instanceof Iterable) {
      List<Object> bsonList = new ArrayList<>();
      for (Object item : (Iterable<?>) value) {
        Object converted = convertToBsonValue(item);
        bsonList.add(converted != null ? converted : new BsonNull());
      }
      return bsonList;
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Document doc = new Document();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object converted = convertToBsonValue(entry.getValue());
        doc.append(String.valueOf(entry.getKey()), converted != null ? converted : new BsonNull());
      }
      return doc;
    }
    return value;
  }

  /**
   * Converts a BSON {@link Document} (or any Map representing fields) to a Beam {@link Row}
   * matching the given {@link Schema}.
   */
  public static Row toRow(Map<?, ?> doc, Schema schema) {
    Row.Builder rowBuilder = Row.withSchema(schema);
    for (Field field : schema.getFields()) {
      Object value = doc.get(field.getName());
      rowBuilder.addValue(convertFromBsonValue(value, field.getType()));
    }
    return rowBuilder.build();
  }

  @SuppressWarnings("JavaUtilDate")
  private static @Nullable Object convertFromBsonValue(
      @Nullable Object value, FieldType fieldType) {
    if (value == null || value instanceof BsonNull) {
      return null;
    }

    switch (fieldType.getTypeName()) {
      case BYTE:
        return (value instanceof Number)
            ? ((Number) value).byteValue()
            : Byte.parseByte(value.toString());
      case INT16:
        return (value instanceof Number)
            ? ((Number) value).shortValue()
            : Short.parseShort(value.toString());
      case INT32:
        return (value instanceof Number)
            ? ((Number) value).intValue()
            : Integer.parseInt(value.toString());
      case INT64:
        return (value instanceof Number)
            ? ((Number) value).longValue()
            : Long.parseLong(value.toString());
      case FLOAT:
        return (value instanceof Number)
            ? ((Number) value).floatValue()
            : Float.parseFloat(value.toString());
      case DOUBLE:
        return (value instanceof Number)
            ? ((Number) value).doubleValue()
            : Double.parseDouble(value.toString());
      case DECIMAL:
        return (value instanceof Number)
            ? java.math.BigDecimal.valueOf(((Number) value).doubleValue())
            : new java.math.BigDecimal(value.toString());
      case STRING:
        return value.toString();
      case BOOLEAN:
        return (value instanceof Boolean)
            ? (Boolean) value
            : Boolean.parseBoolean(value.toString());
      case DATETIME:
        if (value instanceof java.util.Date) {
          return new Instant(((java.util.Date) value).getTime());
        } else if (value instanceof Number) {
          return new Instant(((Number) value).longValue());
        } else {
          return Instant.parse(value.toString());
        }
      case BYTES:
        if (value instanceof Binary) {
          return ((Binary) value).getData();
        } else if (value instanceof byte[]) {
          return (byte[]) value;
        } else {
          return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
      case ARRAY:
      case ITERABLE:
        if (!(value instanceof Iterable)) {
          throw new IllegalArgumentException(
              "Expected Iterable for type "
                  + fieldType
                  + ", but got: "
                  + value.getClass().getName());
        }
        Iterable<?> iterable = (Iterable<?>) value;
        List<@Nullable Object> rowList = new ArrayList<>();
        FieldType elementType = fieldType.getCollectionElementType();
        if (elementType == null) {
          throw new IllegalArgumentException(
              "Collection element type cannot be null for type: " + fieldType);
        }
        for (Object item : iterable) {
          rowList.add(convertFromBsonValue(item, elementType));
        }
        return rowList;
      case MAP:
        if (!(value instanceof Map)) {
          throw new IllegalArgumentException(
              "Expected Map for type " + fieldType + ", but got: " + value.getClass().getName());
        }
        Map<?, ?> map = (Map<?, ?>) value;
        Map<String, @Nullable Object> rowMap = new HashMap<>();
        FieldType valueType = fieldType.getMapValueType();
        if (valueType == null) {
          throw new IllegalArgumentException(
              "Map value type cannot be null for type: " + fieldType);
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          rowMap.put(
              String.valueOf(entry.getKey()), convertFromBsonValue(entry.getValue(), valueType));
        }
        return rowMap;
      case ROW:
        Schema rowSchema = fieldType.getRowSchema();
        if (rowSchema == null) {
          throw new IllegalArgumentException("Row schema cannot be null for type: " + fieldType);
        }
        if (value instanceof Map) {
          return toRow((Map<?, ?>) value, rowSchema);
        } else {
          throw new IllegalArgumentException(
              "Cannot convert value of type "
                  + (value != null ? value.getClass().getName() : "null")
                  + " to Row");
        }
      default:
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }
  }
}
