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
package org.apache.beam.sdk.io.gcp.firestore;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Utility methods for Firestore SchemaTransform providers. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
final class FirestoreUtils {

  private FirestoreUtils() {}

  static String documentsRoot(String projectId, String databaseId) {
    return String.format("projects/%s/databases/%s/documents", projectId, databaseId);
  }

  static String documentPath(
      String projectId, String databaseId, String collectionId, String documentId) {
    return String.format(
        "%s/%s/%s", documentsRoot(projectId, databaseId), collectionId, documentId);
  }

  static String documentIdFromName(String documentName) {
    int lastSlash = documentName.lastIndexOf('/');
    if (lastSlash < 0 || lastSlash == documentName.length() - 1) {
      throw new IllegalArgumentException("Invalid Firestore document name: " + documentName);
    }
    return documentName.substring(lastSlash + 1);
  }

  static Row documentToRow(Document document, Schema schema, @Nullable String documentIdField) {
    Map<String, Object> values = new HashMap<>();
    for (Map.Entry<String, Value> entry : document.getFieldsMap().entrySet()) {
      values.put(entry.getKey(), valueToJava(entry.getValue()));
    }
    if (documentIdField != null && schema.hasField(documentIdField)) {
      values.put(documentIdField, documentIdFromName(document.getName()));
    }
    return toRow(values, schema);
  }

  static Document rowToDocument(
      Row row,
      Schema schema,
      String projectId,
      String databaseId,
      String collectionId,
      String documentIdField) {
    String documentId = row.getString(documentIdField);
    if (documentId == null || documentId.isEmpty()) {
      throw new IllegalArgumentException(
          "Document id field '" + documentIdField + "' must be set on input rows.");
    }

    Document.Builder builder =
        Document.newBuilder()
            .setName(documentPath(projectId, databaseId, collectionId, documentId));
    for (Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(documentIdField)) {
        continue;
      }
      Object fieldValue = row.getValue(fieldName);
      if (fieldValue != null) {
        builder.putFields(fieldName, javaToValue(fieldValue, field.getType()));
      }
    }
    return builder.build();
  }

  static Row toRow(Map<String, ?> values, Schema schema) {
    Row.Builder rowBuilder = Row.withSchema(schema);
    for (Field field : schema.getFields()) {
      rowBuilder.addValue(convertFromJava(values.get(field.getName()), field.getType()));
    }
    return rowBuilder.build();
  }

  private static Map<String, ?> castToStringKeyMap(Map<?, ?> map) {
    Map<String, Object> converted = new HashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      converted.put(String.valueOf(entry.getKey()), entry.getValue());
    }
    return converted;
  }

  private static @Nullable Object valueToJava(Value value) {
    switch (value.getValueTypeCase()) {
      case STRING_VALUE:
        return value.getStringValue();
      case INTEGER_VALUE:
        return value.getIntegerValue();
      case DOUBLE_VALUE:
        return value.getDoubleValue();
      case BOOLEAN_VALUE:
        return value.getBooleanValue();
      case TIMESTAMP_VALUE:
        return new Instant(Timestamps.toMillis(value.getTimestampValue()));
      case BYTES_VALUE:
        return value.getBytesValue().toByteArray();
      case NULL_VALUE:
        return null;
      case ARRAY_VALUE:
        List<@Nullable Object> values = new ArrayList<>();
        for (Value element : value.getArrayValue().getValuesList()) {
          values.add(valueToJava(element));
        }
        return values;
      case MAP_VALUE:
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, Value> entry : value.getMapValue().getFieldsMap().entrySet()) {
          map.put(entry.getKey(), valueToJava(entry.getValue()));
        }
        return map;
      case VALUETYPE_NOT_SET:
        return null;
      default:
        throw new IllegalArgumentException(
            "Unsupported Firestore value type: " + value.getValueTypeCase());
    }
  }

  private static Value javaToValue(Object value, FieldType fieldType) {
    if (value == null) {
      return Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
    }
    switch (fieldType.getTypeName()) {
      case STRING:
        return Value.newBuilder().setStringValue(value.toString()).build();
      case INT64:
        return Value.newBuilder().setIntegerValue(((Number) value).longValue()).build();
      case DOUBLE:
        return Value.newBuilder().setDoubleValue(((Number) value).doubleValue()).build();
      case BOOLEAN:
        return Value.newBuilder().setBooleanValue((Boolean) value).build();
      case DATETIME:
        Instant instant = (Instant) value;
        return Value.newBuilder()
            .setTimestampValue(Timestamps.fromMillis(instant.getMillis()))
            .build();
      case BYTES:
        return Value.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) value)).build();
      case ARRAY:
      case ITERABLE:
        ArrayValue.Builder arrayBuilder = ArrayValue.newBuilder();
        FieldType elementType = fieldType.getCollectionElementType();
        if (elementType == null) {
          throw new IllegalArgumentException("Collection element type cannot be null.");
        }
        for (Object item : (Iterable<?>) value) {
          arrayBuilder.addValues(
              item == null
                  ? Value.newBuilder()
                      .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
                      .build()
                  : javaToValue(item, elementType));
        }
        return Value.newBuilder().setArrayValue(arrayBuilder.build()).build();
      case MAP:
        MapValue.Builder mapBuilder = MapValue.newBuilder();
        FieldType valueType = fieldType.getMapValueType();
        if (valueType == null) {
          throw new IllegalArgumentException("Map value type cannot be null.");
        }
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          Object mapValue = entry.getValue();
          mapBuilder.putFields(
              String.valueOf(entry.getKey()),
              mapValue == null
                  ? Value.newBuilder()
                      .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
                      .build()
                  : javaToValue(mapValue, valueType));
        }
        return Value.newBuilder().setMapValue(mapBuilder.build()).build();
      case ROW:
        Schema rowSchema = fieldType.getRowSchema();
        if (rowSchema == null) {
          throw new IllegalArgumentException("Row schema cannot be null.");
        }
        if (!(value instanceof Row)) {
          throw new IllegalArgumentException("Expected Row for nested field.");
        }
        MapValue.Builder nestedMapBuilder = MapValue.newBuilder();
        Row nestedRow = (Row) value;
        for (Field nestedField : rowSchema.getFields()) {
          Object nestedValue = nestedRow.getValue(nestedField.getName());
          if (nestedValue != null) {
            nestedMapBuilder.putFields(
                nestedField.getName(), javaToValue(nestedValue, nestedField.getType()));
          }
        }
        return Value.newBuilder().setMapValue(nestedMapBuilder.build()).build();
      default:
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }
  }

  private static @Nullable Object convertFromJava(@Nullable Object value, FieldType fieldType) {
    if (value == null) {
      return null;
    }
    switch (fieldType.getTypeName()) {
      case BYTE:
        return ((Number) value).byteValue();
      case INT16:
        return ((Number) value).shortValue();
      case INT32:
        return ((Number) value).intValue();
      case INT64:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      case DECIMAL:
        return value instanceof java.math.BigDecimal
            ? value
            : java.math.BigDecimal.valueOf(((Number) value).doubleValue());
      case STRING:
        return value.toString();
      case BOOLEAN:
        return value;
      case DATETIME:
        if (value instanceof Instant) {
          return value;
        }
        if (value instanceof Number) {
          return new Instant(((Number) value).longValue());
        }
        return Instant.parse(value.toString());
      case BYTES:
        if (value instanceof byte[]) {
          return value;
        }
        return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      case ARRAY:
      case ITERABLE:
        if (!(value instanceof Iterable)) {
          throw new IllegalArgumentException("Expected Iterable for array field.");
        }
        FieldType elementType = fieldType.getCollectionElementType();
        if (elementType == null) {
          throw new IllegalArgumentException("Collection element type cannot be null.");
        }
        List<@Nullable Object> rowList = new ArrayList<>();
        for (Object item : (Iterable<?>) value) {
          rowList.add(convertFromJava(item, elementType));
        }
        return rowList;
      case MAP:
        if (!(value instanceof Map)) {
          throw new IllegalArgumentException("Expected Map for map field.");
        }
        FieldType valueType = fieldType.getMapValueType();
        if (valueType == null) {
          throw new IllegalArgumentException("Map value type cannot be null.");
        }
        Map<String, @Nullable Object> rowMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          rowMap.put(String.valueOf(entry.getKey()), convertFromJava(entry.getValue(), valueType));
        }
        return rowMap;
      case ROW:
        Schema rowSchema = fieldType.getRowSchema();
        if (rowSchema == null) {
          throw new IllegalArgumentException("Row schema cannot be null.");
        }
        if (value instanceof Map) {
          return toRow(castToStringKeyMap((Map<?, ?>) value), rowSchema);
        }
        if (value instanceof Row) {
          return value;
        }
        throw new IllegalArgumentException("Cannot convert value to Row.");
      default:
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }
  }
}
