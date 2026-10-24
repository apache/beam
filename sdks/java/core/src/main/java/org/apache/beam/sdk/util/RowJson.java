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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BYTE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DECIMAL;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.MAP;
import static org.apache.beam.sdk.schemas.Schema.TypeName.STRING;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.booleanValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.byteValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.dateValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.datetimeValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.decimalValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.doubleValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.floatValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.intValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.localDatetimeValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.longValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.shortValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.stringValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.timeValueExtractor;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.util.RowJsonValueExtractors.ValueExtractor;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableInstant;

/**
 * Jackson serializer and deserializer for {@link Row Rows}.
 *
 * <p>Supports converting between JSON primitive types and:
 *
 * <ul>
 *   <li>{@link Schema.TypeName#BYTE}
 *   <li>{@link Schema.TypeName#INT16}
 *   <li>{@link Schema.TypeName#INT32}
 *   <li>{@link Schema.TypeName#INT64}
 *   <li>{@link Schema.TypeName#FLOAT}
 *   <li>{@link Schema.TypeName#DOUBLE}
 *   <li>{@link Schema.TypeName#BOOLEAN}
 *   <li>{@link Schema.TypeName#STRING}
 *   <li>{@link Schema.TypeName#DECIMAL}
 *   <li>{@link Schema.TypeName#DATETIME}
 * </ul>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RowJson {
  private static final ImmutableSet<TypeName> SUPPORTED_TYPES =
      ImmutableSet.of(
          BYTE, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, DECIMAL, DATETIME, MAP);
  private static final ImmutableSet<String> KNOWN_LOGICAL_TYPE_IDENTIFIERS =
      ImmutableSet.of(
          SqlTypes.DATE.getIdentifier(),
          SqlTypes.TIME.getIdentifier(),
          SqlTypes.DATETIME.getIdentifier());

  /**
   * Throws {@link UnsupportedRowJsonException} if {@code schema} contains an unsupported field
   * type.
   */
  public static void verifySchemaSupported(Schema schema) {
    ImmutableList<UnsupportedField> unsupportedFields = findUnsupportedFields(schema);
    if (!unsupportedFields.isEmpty()) {
      throw new UnsupportedRowJsonException(
          String.format(
              "Field type%s %s not supported when converting between JSON and Rows. Supported types are: %s",
              unsupportedFields.size() > 1 ? "s" : "",
              unsupportedFields.toString(),
              SUPPORTED_TYPES.toString()));
    }
  }

  private static class UnsupportedField {
    final String descriptor;
    final TypeName typeName;

    UnsupportedField(String descriptor, TypeName typeName) {
      this.descriptor = descriptor;
      this.typeName = typeName;
    }

    @Override
    public String toString() {
      return this.descriptor + "=" + this.typeName;
    }
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(Schema schema) {
    return schema.getFields().stream()
        .flatMap((field) -> findUnsupportedFields(field).stream())
        .collect(toImmutableList());
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(Field field) {
    return findUnsupportedFields(field.getType(), field.getName());
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(
      FieldType fieldType, String fieldName) {
    TypeName fieldTypeName = fieldType.getTypeName();

    if (fieldTypeName.isCompositeType()) {
      return fieldType.getRowSchema().getFields().stream()
          .flatMap(
              (field) ->
                  findUnsupportedFields(field.getType(), fieldName + "." + field.getName())
                      .stream())
          .collect(toImmutableList());
    }

    if (fieldTypeName.isCollectionType()) {
      return findUnsupportedFields(fieldType.getCollectionElementType(), fieldName + "[]");
    }

    if (fieldTypeName.isMapType()) {
      if (!STRING.equals(fieldType.getMapKeyType().getTypeName())) {
        return ImmutableList.of(
            new UnsupportedField(fieldName + ".key", fieldType.getMapKeyType().getTypeName()));
      }
      return findUnsupportedFields(fieldType.getMapValueType(), fieldName + "{}");
    }

    if (fieldTypeName.isLogicalType()) {
      if (KNOWN_LOGICAL_TYPE_IDENTIFIERS.contains(fieldType.getLogicalType().getIdentifier())) {
        return ImmutableList.of();
      } else {
        return findUnsupportedFields(fieldType.getLogicalType().getBaseType(), fieldName);
      }
    }

    if (!SUPPORTED_TYPES.contains(fieldTypeName)) {
      return ImmutableList.of(new UnsupportedField(fieldName, fieldTypeName));
    }

    return ImmutableList.of();
  }

  /** Jackson deserializer for parsing JSON into {@link Row Rows}. */
  public static class RowJsonDeserializer extends StdDeserializer<Row> {

    /**
     * An enumeration type for specifying how {@link RowJsonDeserializer} should expect null values
     * to be represented.
     *
     * <p>For example, when parsing JSON for the Schema {@code (str: REQUIRED STRING, int: NULLABLE
     * INT64)}:
     *
     * <ul>
     *   <li>If configured with {@code REQUIRE_NULL}, {@code {"str": "foo", "int": null}} would be
     *       accepted.
     *   <li>If configured with {@code REQUIRE_MISSING}, {@code {"str": "bar"}} would be accepted,
     *       and would yield a {@link Row} with {@code null} for the {@code int} field.
     *   <li>If configured with {@code ALLOW_MISSING_OR_NULL}, either JSON string would be accepted.
     * </ul>
     */
    public enum NullBehavior {
      /**
       * Specifies that a null value may be represented as either a missing field or a null value in
       * the input JSON.
       */
      ACCEPT_MISSING_OR_NULL,
      /**
       * Specifies that a null value must be represented with a null value in JSON. If the field is
       * missing an {@link UnsupportedRowJsonException} will be thrown.
       */
      REQUIRE_NULL,
      /**
       * Specifies that a null value must be represented with a missing field in JSON. If the field
       * has a null value an {@link UnsupportedRowJsonException} will be thrown.
       */
      REQUIRE_MISSING,
    }

    private static final ImmutableMap<TypeName, ValueExtractor<?>> JSON_VALUE_GETTERS =
        Maps.immutableEnumMap(
            ImmutableMap.<TypeName, ValueExtractor<?>>builder()
                .put(BYTE, byteValueExtractor())
                .put(INT16, shortValueExtractor())
                .put(INT32, intValueExtractor())
                .put(INT64, longValueExtractor())
                .put(FLOAT, floatValueExtractor())
                .put(DOUBLE, doubleValueExtractor())
                .put(BOOLEAN, booleanValueExtractor())
                .put(STRING, stringValueExtractor())
                .put(DECIMAL, decimalValueExtractor())
                .put(DATETIME, datetimeValueExtractor())
                .build());

    private static final ValueExtractor<LocalDate> DATE_EXTRACTOR = dateValueExtractor();
    private static final ValueExtractor<LocalTime> TIME_EXTRACTOR = timeValueExtractor();
    private static final ValueExtractor<LocalDateTime> LOCAL_DATETIME_EXTRACTOR =
        localDatetimeValueExtractor();

    private final Schema schema;
    private final FieldType rootRowType;
    private NullBehavior nullBehavior = NullBehavior.ACCEPT_MISSING_OR_NULL;

    /** Creates a deserializer for a {@link Row} {@link Schema}. */
    public static RowJsonDeserializer forSchema(Schema schema) {
      verifySchemaSupported(schema);
      return new RowJsonDeserializer(schema);
    }

    private RowJsonDeserializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
      this.rootRowType = FieldType.row(schema);
    }

    /**
     * Sets the behaviour of the deserializer when retrieving null values in the input JSON. See
     * {@link NullBehavior} for a description of the options. Default value is {@code
     * ACCEPT_MISSING_OR_NULL}.
     */
    public RowJsonDeserializer withNullBehavior(NullBehavior behavior) {
      this.nullBehavior = behavior;
      return this;
    }

    @Override
    public Row deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {

      // Parse and convert the root object to Row as if it's a nested field with name 'root'
      return (Row) extractJsonNodeValue("root", rootRowType, jsonParser.readValueAsTree());
    }

    private @Nullable Object extractJsonNodeValue(
        String fieldName, FieldType fieldType, @Nullable JsonNode jsonValue) {
      if (fieldType.getNullable()) {
        if (jsonValue == null) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_MISSING:
              return null;
            case REQUIRE_NULL:
              throw new UnsupportedRowJsonException(
                  "Field '" + fieldName + "' is not present in the JSON object.");
          }
        }

        if (jsonValue.isNull()) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_NULL:
              return null;
            case REQUIRE_MISSING:
              throw new UnsupportedRowJsonException(
                  "Field '" + fieldName + "' has a null value in the JSON object.");
          }
        }
      } else {
        // field is not nullable
        if (jsonValue == null) {
          throw new UnsupportedRowJsonException(
              "Non-nullable field '" + fieldName + "' is not present in the JSON object.");
        } else if (jsonValue.isNull()) {
          throw new UnsupportedRowJsonException(
              "Non-nullable field '" + fieldName + "' has value null in the JSON object.");
        }
      }

      TypeName typeName = fieldType.getTypeName();
      switch (typeName) {
        case ROW:
          return jsonObjectToRow(fieldName, fieldType.getRowSchema(), jsonValue);
        case ARRAY:
        case ITERABLE:
          return jsonArrayToList(fieldName, fieldType.getCollectionElementType(), jsonValue);
        case MAP:
          return jsonObjectToMap(fieldName, fieldType.getMapValueType(), jsonValue);
        case LOGICAL_TYPE:
          Schema.LogicalType<Object, Object> logicalType =
              (Schema.LogicalType<Object, Object>) fieldType.getLogicalType();
          String identifier = logicalType.getIdentifier();
          if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
            return extractWithExtractor(fieldName, typeName, jsonValue, DATE_EXTRACTOR);
          } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
            return extractWithExtractor(fieldName, typeName, jsonValue, TIME_EXTRACTOR);
          } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
            return extractWithExtractor(fieldName, typeName, jsonValue, LOCAL_DATETIME_EXTRACTOR);
          } else {
            return logicalType.toInputType(
                extractJsonNodeValue(fieldName, logicalType.getBaseType(), jsonValue));
          }
        default:
          return extractJsonPrimitiveValue(fieldName, typeName, jsonValue);
      }
    }

    private Row jsonObjectToRow(String fieldName, Schema rowSchema, JsonNode jsonValue) {
      if (!jsonValue.isObject()) {
        throw new UnsupportedRowJsonException(
            "Expected JSON object for field '"
                + fieldName
                + "'. Unable to convert '"
                + jsonValue.asText()
                + "' to Beam Row, it is not a JSON object. Currently only JSON objects can be parsed to Beam Rows");
      }

      int fieldCount = rowSchema.getFieldCount();
      List<Object> values = new ArrayList<>(fieldCount);
      for (int i = 0; i < fieldCount; i++) {
        Field schemaField = rowSchema.getField(i);
        String schemaFieldName = schemaField.getName();
        values.add(
            extractJsonNodeValue(
                schemaFieldName, schemaField.getType(), jsonValue.get(schemaFieldName)));
      }
      return Row.withSchema(rowSchema).addValues(values).build();
    }

    private Object jsonArrayToList(
        String fieldName, FieldType elementType, JsonNode jsonArrayNode) {
      if (!jsonArrayNode.isArray()) {
        throw new UnsupportedRowJsonException(
            "Expected JSON array for field '"
                + fieldName
                + "'. Instead got "
                + jsonArrayNode.getNodeType().name());
      }

      int size = jsonArrayNode.size();
      if (size == 0) {
        return ImmutableList.of();
      }
      String elementFieldName = fieldName + "[]";
      ImmutableList.Builder<Object> builder = ImmutableList.builderWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        builder.add(extractJsonNodeValue(elementFieldName, elementType, jsonArrayNode.get(i)));
      }
      return builder.build();
    }

    private Map<String, Object> jsonObjectToMap(
        String fieldName, FieldType mapValueType, JsonNode jsonObjectNode) {
      if (!jsonObjectNode.isObject()) {
        throw new UnsupportedRowJsonException(
            "Expected JSON object for field '"
                + fieldName
                + "'. Instead got "
                + jsonObjectNode.getNodeType().name());
      }

      Map<String, Object> result = Maps.newHashMapWithExpectedSize(jsonObjectNode.size());
      Iterator<Map.Entry<String, JsonNode>> fields = jsonObjectNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String key = field.getKey();
        JsonNode value = field.getValue();

        Object extractedValue =
            extractJsonNodeValue(fieldName + "['" + key + "']", mapValueType, value);

        result.put(key, extractedValue);
      }
      return result;
    }

    private static Object extractJsonPrimitiveValue(
        String fieldName, TypeName typeName, JsonNode jsonValue) {
      return extractWithExtractor(
          fieldName, typeName, jsonValue, JSON_VALUE_GETTERS.get(typeName));
    }

    private static Object extractWithExtractor(
        String fieldName, TypeName typeName, JsonNode jsonValue, ValueExtractor<?> extractor) {
      try {
        return extractor.extractValue(jsonValue);
      } catch (RuntimeException e) {
        throw new UnsupportedRowJsonException(
            "Unable to get value from field '"
                + fieldName
                + "'. Schema type '"
                + typeName
                + "'. JSON node type "
                + jsonValue.getNodeType().name(),
            e);
      }
    }
  }

  /** Jackson serializer for converting {@link Row Rows} to JSON. */
  public static class RowJsonSerializer extends StdSerializer<Row> {

    private final Schema schema;
    private boolean dropNullsOnWrite = false;

    /** Creates a serializer for a {@link Row} {@link Schema}. */
    public static RowJsonSerializer forSchema(Schema schema) {
      verifySchemaSupported(schema);
      return new RowJsonSerializer(schema);
    }

    private RowJsonSerializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
    }

    /** Serializer drops nulls on write if set to true instead of writing fieldName: null. */
    public RowJsonSerializer withDropNullsOnWrite(Boolean dropNullsOnWrite) {
      this.dropNullsOnWrite = Boolean.TRUE.equals(dropNullsOnWrite);
      return this;
    }

    @Override
    public void serialize(Row value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      writeRow(value, this.schema, gen);
    }

    // TODO: ByteBuddy generate based on schema?
    private void writeRow(Row row, Schema schema, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        Field field = schema.getField(i);
        Object value = row.getValue(i);
        if (dropNullsOnWrite && value == null && field.getType().getNullable()) {
          continue;
        }
        gen.writeFieldName(field.getName());
        if (field.getType().getNullable() && value == null) {
          gen.writeNull();
          continue;
        }
        writeValue(gen, field.getType(), value);
      }
      gen.writeEndObject();
    }

    private void writeValue(JsonGenerator gen, FieldType type, Object value) throws IOException {
      switch (type.getTypeName()) {
        case BOOLEAN:
          gen.writeBoolean((boolean) value);
          break;
        case STRING:
          gen.writeString((String) value);
          break;
        case BYTE:
          gen.writeNumber((byte) value);
          break;
        case DOUBLE:
          gen.writeNumber((double) value);
          break;
        case FLOAT:
          gen.writeNumber((float) value);
          break;
        case INT16:
          gen.writeNumber((short) value);
          break;
        case INT32:
          gen.writeNumber((int) value);
          break;
        case INT64:
          gen.writeNumber((long) value);
          break;
        case DECIMAL:
          gen.writeNumber((BigDecimal) value);
          break;
        case DATETIME:
          // ReadableInstant.toString produces ISO8601 format string
          gen.writeString(((ReadableInstant) value).toString());
          break;
        case ARRAY:
        case ITERABLE:
          gen.writeStartArray();
          for (Object element : (Iterable<Object>) value) {
            writeValue(gen, type.getCollectionElementType(), element);
          }
          gen.writeEndArray();
          break;
        case ROW:
          writeRow((Row) value, type.getRowSchema(), gen);
          break;
        case MAP:
          gen.writeStartObject();
          for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
            gen.writeFieldName(entry.getKey().toString());
            writeValue(gen, type.getMapValueType(), entry.getValue());
          }
          gen.writeEndObject();
          break;
        case LOGICAL_TYPE:
          Schema.LogicalType<Object, Object> logicalType =
              (Schema.LogicalType<Object, Object>) type.getLogicalType();
          String identifier = logicalType.getIdentifier();
          if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
            gen.writeString(((LocalDate) value).toString()); // ISO 8601 format
          } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
            gen.writeString(((LocalTime) value).toString()); // ISO 8601 format
          } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
            gen.writeString(((LocalDateTime) value).toString()); // ISO 8601 format
          } else {
            writeValue(gen, logicalType.getBaseType(), logicalType.toBaseType(value));
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported field type: " + type);
      }
    }
  }

  /** Gets thrown when Row parsing or serialization fails for any reason. */
  public static class UnsupportedRowJsonException extends RuntimeException {

    UnsupportedRowJsonException(String message, Throwable reason) {
      super(message, reason);
    }

    UnsupportedRowJsonException(String message) {
      super(message);
    }
  }
}
