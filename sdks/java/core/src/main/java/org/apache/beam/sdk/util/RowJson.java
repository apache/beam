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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList.toImmutableList;

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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJsonValueExtractors.ValueExtractor;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

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
  private static final ImmutableSet<TypeName> SUPPORTED_TYPES =
      ImmutableSet.of(BYTE, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, DECIMAL);

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

    if (fieldTypeName.isLogicalType()) {
      return findUnsupportedFields(fieldType.getLogicalType().getBaseType(), fieldName);
    }

    if (!SUPPORTED_TYPES.contains(fieldTypeName)) {
      return ImmutableList.of(new UnsupportedField(fieldName, fieldTypeName));
    }

    return ImmutableList.of();
  }

  /** Jackson deserializer for parsing JSON into {@link Row Rows}. */
  public static class RowJsonDeserializer extends StdDeserializer<Row> {

    private static final boolean SEQUENTIAL = false;

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
    private NullBehavior nullBehavior = NullBehavior.ACCEPT_MISSING_OR_NULL;

    /** Creates a deserializer for a {@link Row} {@link Schema}. */
    public static RowJsonDeserializer forSchema(Schema schema) {
      verifySchemaSupported(schema);
      return new RowJsonDeserializer(schema);
    }

    private RowJsonDeserializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
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
      return (Row)
          extractJsonNodeValue(
              FieldValue.of("root", FieldType.row(schema), jsonParser.readValueAsTree()));
    }

    private Object extractJsonNodeValue(FieldValue fieldValue) {
      if (fieldValue.type().getNullable()) {
        if (!fieldValue.isJsonValuePresent()) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_MISSING:
              return null;
            case REQUIRE_NULL:
              throw new UnsupportedRowJsonException(
                  "Field '" + fieldValue.name() + "' is not present in the JSON object.");
          }
        }

        if (fieldValue.isJsonNull()) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_NULL:
              return null;
            case REQUIRE_MISSING:
              throw new UnsupportedRowJsonException(
                  "Field '" + fieldValue.name() + "' has a null value in the JSON object.");
          }
        }
      } else {
        // field is not nullable
        if (!fieldValue.isJsonValuePresent()) {
          throw new UnsupportedRowJsonException(
              "Non-nullable field '" + fieldValue.name() + "' is not present in the JSON object.");
        } else if (fieldValue.isJsonNull()) {
          throw new UnsupportedRowJsonException(
              "Non-nullable field '" + fieldValue.name() + "' has value null in the JSON object.");
        }
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

    private Row jsonObjectToRow(FieldValue rowFieldValue) {
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

    private Object jsonArrayToList(FieldValue arrayFieldValue) {
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
          .collect(toImmutableList());
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
  }

  /** Jackson serializer for converting {@link Row Rows} to JSON. */
  public static class RowJsonSerializer extends StdSerializer<Row> {

    private final Schema schema;
    private Boolean dropNullsOnWrite = false;

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
      this.dropNullsOnWrite = dropNullsOnWrite;
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
