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
package org.apache.beam.sdk.schemas;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonSchemaConversionTest {
  @Test
  public void testBeamSchemaToJsonSchemaValidatesPrimitives() {
    Schema schema =
        Schema.builder()
            .addBooleanField("booleanProp")
            .addInt32Field("integerProp")
            .addFloatField("floatProp")
            .addStringField("stringProp")
            .addByteField("byteProp")
            .build();

    JSONObject obj =
        new JSONObject()
            .put("booleanProp", true)
            .put("integerProp", 1)
            .put("floatProp", 1.1)
            .put("stringProp", "a")
            .put("byteProp", (byte) 1);

    JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidatesNullablePrimitives() {
    Schema schema =
        Schema.builder()
            .addNullableBooleanField("booleanProp")
            .addNullableInt32Field("integerProp")
            .addNullableFloatField("floatProp")
            .addNullableStringField("stringProp")
            .addNullableByteField("byteProp")
            .build();

    JSONObject obj =
        new JSONObject()
            .put("booleanProp", JSONObject.NULL)
            .put("integerProp", JSONObject.NULL)
            .put("floatProp", JSONObject.NULL)
            .put("stringProp", JSONObject.NULL)
            .put("byteProp", JSONObject.NULL);

    JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidatesArrayPrimitives() {
    Schema schema =
        Schema.builder()
            .addArrayField("booleanArray", FieldType.BOOLEAN)
            .addArrayField("integerArray", FieldType.INT32)
            .addArrayField("floatArray", FieldType.FLOAT)
            .addArrayField("stringArray", FieldType.STRING)
            .addArrayField("byteArray", FieldType.BYTE)
            .build();

    JSONObject obj =
        new JSONObject()
            .put("booleanArray", new JSONArray().put(true).put(false))
            .put("integerArray", new JSONArray().put(1).put(2).put(3))
            .put("floatArray", new JSONArray().put(1.1).put(2.2).put(3.3))
            .put("stringArray", new JSONArray().put("a").put("b").put("c"))
            .put("byteArray", new JSONArray().put((byte) 1).put((byte) 2).put((byte) 3));

    JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidatesNestedPrimitives() {
    Schema nestedSchema =
        Schema.builder()
            .addBooleanField("booleanProp")
            .addInt32Field("integerProp")
            .addFloatField("floatProp")
            .addStringField("stringProp")
            .addByteField("byteProp")
            .build();
    Schema schema =
        Schema.builder()
            .addRowField("nestedObj", nestedSchema)
            .addArrayField("nestedRepeated", FieldType.row(nestedSchema))
            .build();

    JSONObject nestedObj =
        new JSONObject()
            .put("booleanProp", true)
            .put("integerProp", 1)
            .put("floatProp", 1.1)
            .put("stringProp", "a")
            .put("byteProp", (byte) 1);
    JSONObject obj =
        new JSONObject()
            .put("nestedObj", nestedObj)
            .put("nestedRepeated", new JSONArray().put(nestedObj).put(nestedObj));

    JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidationFailsWithMissingProperties() {
    Schema schema =
        Schema.builder().addInt32Field("integerProp").addStringField("stringProp").build();

    JSONObject obj = new JSONObject().put("integerProp", 1);

    try {
      JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
      throw new RuntimeException("Did not throw validation exception for missing properties");
    } catch (ValidationException e) {
      assertEquals("#: required key [stringProp] not found", e.getMessage());
    }
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidationFailsWithExtraProperties() {
    Schema schema =
        Schema.builder().addInt32Field("integerProp").addStringField("stringProp").build();

    JSONObject obj =
        new JSONObject().put("integerProp", 1).put("stringProp", "a").put("extraProp", "extra");

    try {
      JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
      throw new RuntimeException("Did not throw validation exception for extra properties");
    } catch (ValidationException e) {
      assertEquals("#: extraneous key [extraProp] is not permitted", e.getMessage());
    }
  }

  @Test
  public void testBeamSchemaToJsonSchemaValidationFailsWithNonNullProperty() {
    Schema schema =
        Schema.builder().addInt32Field("integerProp").addStringField("stringProp").build();

    JSONObject obj = new JSONObject().put("integerProp", JSONObject.NULL).put("stringProp", "a");

    try {
      JsonUtils.jsonSchemaFromBeamSchema(schema).validate(obj);
      throw new RuntimeException(
          "Did not throw validation exception for null value in non-null property");
    } catch (ValidationException e) {
      assertEquals("#/integerProp: expected type: Integer, found: Null", e.getMessage());
    }
  }

  @Test
  public void testBasicJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/basic_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(
          parsedSchema.getFieldNames(),
          containsInAnyOrder("booleanProp", "integerProp", "numberProp", "stringProp"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.BOOLEAN.withNullable(true),
              Schema.FieldType.INT64.withNullable(true),
              Schema.FieldType.DOUBLE.withNullable(true),
              Schema.FieldType.STRING.withNullable(true)));
    }
  }

  @Test
  public void testNestedStructsJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/nested_arrays_objects_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(parsedSchema.getFieldNames(), containsInAnyOrder("fruits", "vegetables"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true),
              Schema.FieldType.array(
                      Schema.FieldType.row(
                          Schema.of(
                              Schema.Field.of("veggieName", Schema.FieldType.STRING),
                              Schema.Field.of("veggieLike", Schema.FieldType.BOOLEAN))))
                  .withNullable(true)));
    }
  }

  @Test
  public void testArrayNestedArrayObjectJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/array_nested_array_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(parsedSchema.getFieldNames(), containsInAnyOrder("complexMatrix"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.array(
                      Schema.FieldType.array(
                          Schema.FieldType.row(
                              Schema.of(
                                  Schema.Field.nullable("imaginary", Schema.FieldType.DOUBLE),
                                  Schema.Field.nullable("real", Schema.FieldType.DOUBLE)))))
                  .withNullable(true)));
    }
  }

  @Test
  public void testObjectNestedObjectArrayJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass()
            .getResourceAsStream("/json-schema/object_nested_object_and_array_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(parsedSchema.getFieldNames(), containsInAnyOrder("classroom"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.row(
                      Schema.of(
                          Schema.Field.nullable(
                              "classroom",
                              Schema.FieldType.row(
                                  Schema.of(
                                      Schema.Field.nullable(
                                          "students",
                                          Schema.FieldType.array(
                                                  Schema.FieldType.row(
                                                      Schema.of(
                                                          Schema.Field.nullable(
                                                              "name", Schema.FieldType.STRING),
                                                          Schema.Field.nullable(
                                                              "age", Schema.FieldType.INT64))))
                                              .withNullable(true)),
                                      Schema.Field.nullable("building", Schema.FieldType.STRING)))),
                          Schema.Field.nullable("teacher", Schema.FieldType.STRING)))
                  .withNullable(true)));
    }
  }

  @Test
  public void testArrayWithNestedRefsBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/ref_with_ref_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(parsedSchema.getFieldNames(), containsInAnyOrder("vegetables"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.array(
                      Schema.FieldType.row(
                          Schema.of(
                              Schema.Field.of("veggieName", Schema.FieldType.STRING),
                              Schema.Field.of("veggieLike", Schema.FieldType.BOOLEAN),
                              Schema.Field.nullable(
                                  "origin",
                                  Schema.FieldType.row(
                                      Schema.of(
                                          Schema.Field.nullable("country", Schema.FieldType.STRING),
                                          Schema.Field.nullable("town", Schema.FieldType.STRING),
                                          Schema.Field.nullable(
                                              "region", Schema.FieldType.STRING)))))))
                  .withNullable(true)));
    }
  }

  @Test
  public void testUnsupportedTupleArrays() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/unsupported_tuple_arrays.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");

      IllegalArgumentException thrownException =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);
              });

      assertThat(
          thrownException.getMessage(),
          containsString(
              "Tuple-like arrays are unsupported. Expected a single item type for field tupleArray"));
    }
  }

  @Test
  public void testUnsupportedNestedTupleArrays() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/json-schema/unsupported_nested_tuple_array.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");

      IllegalArgumentException thrownException =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);
              });

      assertThat(
          thrownException.getCause().getMessage(),
          containsString(
              "Tuple-like arrays are unsupported. Expected a single item type for field tupleArray"));
    }
  }
}
