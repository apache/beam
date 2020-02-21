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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BYTE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DECIMAL;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.STRING;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.booleanValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.byteValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.decimalValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.doubleValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.floatValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.intValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.longValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.shortValueExtractor;
import static org.apache.beam.sdk.util.RowJsonValueExtractors.stringValueExtractor;
import static org.apache.beam.sdk.values.Row.toRow;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJsonValueExtractors.ValueExtractor;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

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
 * </ul>
 */
@Experimental(Kind.SCHEMAS)
public class RowJson {
  /** Jackson deserializer for parsing JSON into {@link Row Rows}. */
  public static class RowJsonDeserializer extends StdDeserializer<Row> {

    private static final boolean SEQUENTIAL = false;

    private static final ImmutableMap<TypeName, ValueExtractor<?>> JSON_VALUE_GETTERS =
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
            .build();

    private final Schema schema;

    /** Creates a deserializer for a {@link Row} {@link Schema}. */
    public static RowJsonDeserializer forSchema(Schema schema) {
      schema.getFields().forEach(RowJsonValidation::verifyFieldTypeSupported);
      return new RowJsonDeserializer(schema);
    }

    private RowJsonDeserializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
    }

    @Override
    public Row deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {

      // Parse and convert the root object to Row as if it's a nested field with name 'root'
      return (Row)
          extractJsonNodeValue(
              FieldValue.of("root", FieldType.row(schema), jsonParser.readValueAsTree()));
    }

    private static Object extractJsonNodeValue(FieldValue fieldValue) {
      if (!fieldValue.isJsonValuePresent()) {
        throw new UnsupportedRowJsonException(
            "Field '" + fieldValue.name() + "' is not present in the JSON object");
      }

      if (fieldValue.isJsonNull()) {
        return null;
      }

      if (fieldValue.isRowType()) {
        return jsonObjectToRow(fieldValue);
      }

      if (fieldValue.isArrayType()) {
        return jsonArrayToList(fieldValue);
      }

      if (fieldValue.typeName().isLogicalType()) {
        return extractJsonNodeValue(
            FieldValue.of(
                fieldValue.name(),
                fieldValue.type().getLogicalType().getBaseType(),
                fieldValue.jsonValue()));
      }

      return extractJsonPrimitiveValue(fieldValue);
    }

    private static Row jsonObjectToRow(FieldValue rowFieldValue) {
      if (!rowFieldValue.isJsonObject()) {
        throw new UnsupportedRowJsonException(
            "Expected JSON object for field '"
                + rowFieldValue.name()
                + "'. Unable to convert '"
                + rowFieldValue.jsonValue().asText()
                + "' to Beam Row, it is not a JSON object. Currently only JSON objects can be parsed to Beam Rows");
      }

      return rowFieldValue.rowSchema().getFields().stream()
          .map(
              schemaField ->
                  extractJsonNodeValue(
                      FieldValue.of(
                          schemaField.getName(),
                          schemaField.getType(),
                          rowFieldValue.jsonFieldValue(schemaField.getName()))))
          .collect(toRow(rowFieldValue.rowSchema()));
    }

    private static Object jsonArrayToList(FieldValue arrayFieldValue) {
      if (!arrayFieldValue.isJsonArray()) {
        throw new UnsupportedRowJsonException(
            "Expected JSON array for field '"
                + arrayFieldValue.name()
                + "'. Instead got "
                + arrayFieldValue.jsonNodeType().name());
      }

      return arrayFieldValue
          .jsonArrayElements()
          .map(
              jsonArrayElement ->
                  extractJsonNodeValue(
                      FieldValue.of(
                          arrayFieldValue.name() + "[]",
                          arrayFieldValue.arrayElementType(),
                          jsonArrayElement)))
          .collect(toList());
    }

    private static Object extractJsonPrimitiveValue(FieldValue fieldValue) {
      try {
        return JSON_VALUE_GETTERS.get(fieldValue.typeName()).extractValue(fieldValue.jsonValue());
      } catch (RuntimeException e) {
        throw new UnsupportedRowJsonException(
            "Unable to get value from field '"
                + fieldValue.name()
                + "'. Schema type '"
                + fieldValue.typeName()
                + "'. JSON node type "
                + fieldValue.jsonNodeType().name(),
            e);
      }
    }

    /**
     * Helper class to keep track of schema field type, name, and actual json value for the field.
     */
    @AutoValue
    abstract static class FieldValue {
      abstract String name();

      abstract FieldType type();

      abstract @Nullable JsonNode jsonValue();

      TypeName typeName() {
        return type().getTypeName();
      }

      boolean isJsonValuePresent() {
        return jsonValue() != null;
      }

      boolean isJsonNull() {
        return jsonValue().isNull();
      }

      JsonNodeType jsonNodeType() {
        return jsonValue().getNodeType();
      }

      boolean isJsonArray() {
        return jsonValue().isArray();
      }

      Stream<JsonNode> jsonArrayElements() {
        return StreamSupport.stream(jsonValue().spliterator(), SEQUENTIAL);
      }

      boolean isArrayType() {
        return TypeName.ARRAY.equals(type().getTypeName())
            || TypeName.ITERABLE.equals(type().getTypeName());
      }

      FieldType arrayElementType() {
        return type().getCollectionElementType();
      }

      boolean isJsonObject() {
        return jsonValue().isObject();
      }

      JsonNode jsonFieldValue(String fieldName) {
        return jsonValue().get(fieldName);
      }

      boolean isRowType() {
        return TypeName.ROW.equals(type().getTypeName());
      }

      Schema rowSchema() {
        return type().getRowSchema();
      }

      static FieldValue of(String name, FieldType type, JsonNode jsonValue) {
        return new AutoValue_RowJson_RowJsonDeserializer_FieldValue(name, type, jsonValue);
      }
    }

    /** Gets thrown when Row parsing fails for any reason. */
    public static class UnsupportedRowJsonException extends RuntimeException {

      UnsupportedRowJsonException(String message, Throwable reason) {
        super(message, reason);
      }

      UnsupportedRowJsonException(String message) {
        super(message);
      }
    }
  }

  /** Jackson serializer for converting {@link Row Rows} to JSON. */
  public static class RowJsonSerializer extends StdSerializer<Row> {

    private final Schema schema;

    /** Creates a serializer for a {@link Row} {@link Schema}. */
    public static RowJsonSerializer forSchema(Schema schema) {
      schema.getFields().forEach(RowJsonValidation::verifyFieldTypeSupported);
      return new RowJsonSerializer(schema);
    }

    private RowJsonSerializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
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
        case LOGICAL_TYPE:
          writeValue(gen, type.getLogicalType().getBaseType(), value);
          break;
        default:
          throw new IllegalArgumentException("Unsupported field type: " + type);
      }
    }
  }
}
