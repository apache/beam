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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class YamlUtilsTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  public String makeNested(String input) {
    return Arrays.stream(input.split("\n"))
        .map(str -> "  " + str)
        .collect(Collectors.joining("\n"));
  }

  @Test
  public void testEmptyYamlString() {
    Schema schema = Schema.builder().build();

    assertEquals(Row.nullRow(schema), YamlUtils.toBeamRow("", schema));
  }

  @Test
  public void testInvalidEmptyYamlWithNonEmptySchema() {
    Schema schema = Schema.builder().addStringField("dummy").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Received an empty YAML string, but output schema contains required fields");
    thrown.expectMessage("dummy");

    YamlUtils.toBeamRow("", schema);
  }

  @Test
  public void testNullableValues() {
    String yamlString = "nullable_string:\n" + "nullable_integer:\n" + "nullable_boolean:\n";
    Schema schema =
        Schema.builder()
            .addNullableStringField("nullable_string")
            .addNullableInt32Field("nullable_integer")
            .addNullableBooleanField("nullable_boolean")
            .build();

    assertEquals(Row.nullRow(schema), YamlUtils.toBeamRow(yamlString, schema));
  }

  @Test
  public void testMissingNullableValues() {
    String yamlString = "nullable_string:";
    Schema schema =
        Schema.builder()
            .addNullableStringField("nullable_string")
            .addNullableInt32Field("nullable_integer")
            .addNullableBooleanField("nullable_boolean")
            .build();

    assertEquals(Row.nullRow(schema), YamlUtils.toBeamRow(yamlString, schema));
  }

  @Test
  public void testInvalidNullableValues() {
    String yamlString = "nullable_string:\n" + "integer:";
    Schema schema =
        Schema.builder().addNullableStringField("nullable_string").addInt32Field("integer").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Received null value for non-nullable field \"integer\"");
    YamlUtils.toBeamRow(yamlString, schema);
  }

  @Test
  public void testInvalidMissingRequiredValues() {
    String yamlString = "nullable_string:";
    Schema schema =
        Schema.builder().addNullableStringField("nullable_string").addInt32Field("integer").build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Received null value for non-nullable field \"integer\"");

    YamlUtils.toBeamRow(yamlString, schema);
  }

  @Test
  public void testExtraFieldsAreIgnored() {
    String yamlString = "field1: val1\n" + "field2: val2";
    Schema schema = Schema.builder().addStringField("field1").build();
    Row expectedRow = Row.withSchema(schema).withFieldValue("field1", "val1").build();

    assertEquals(expectedRow, YamlUtils.toBeamRow(yamlString, schema));
  }

  @Test
  public void testInvalidTopLevelArray() {
    String invalidYaml = "- top_level_list" + "- another_list";
    Schema schema = Schema.builder().build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected a YAML mapping");
    YamlUtils.toBeamRow(invalidYaml, schema);
  }

  private static final Schema FLAT_SCHEMA =
      Schema.builder()
          .addByteField("byte_field")
          .addInt16Field("int16_field")
          .addInt32Field("int32_field")
          .addInt64Field("int64_field")
          .addFloatField("float_field")
          .addDoubleField("double_field")
          .addDecimalField("decimal_field")
          .addBooleanField("boolean_field")
          .addStringField("string_field")
          .addByteArrayField("bytes_field")
          .build();

  private static final Row FLAT_ROW =
      Row.withSchema(FLAT_SCHEMA)
          .withFieldValue("byte_field", Byte.valueOf("123"))
          .withFieldValue("int16_field", Short.valueOf("16"))
          .withFieldValue("int32_field", 32)
          .withFieldValue("int64_field", 64L)
          .withFieldValue("float_field", 123.456F)
          .withFieldValue("double_field", 456.789)
          .withFieldValue("decimal_field", BigDecimal.valueOf(789.123))
          .withFieldValue("boolean_field", true)
          .withFieldValue("string_field", "some string")
          .withFieldValue("bytes_field", BaseEncoding.base64().decode("abc"))
          .build();

  private static final String FLAT_YAML =
      "byte_field: 123\n"
          + "int16_field: 16\n"
          + "int32_field: 32\n"
          + "int64_field: 64\n"
          + "float_field: 123.456\n"
          + "double_field: 456.789\n"
          + "decimal_field: 789.123\n"
          + "boolean_field: true\n"
          + "string_field: some string\n"
          + "bytes_field: abc";

  @Test
  public void testAllTypesFlat() {
    assertEquals(FLAT_ROW, YamlUtils.toBeamRow(FLAT_YAML, FLAT_SCHEMA));
  }

  @Test
  public void testAllTypesNested() {
    String nestedFlatTypes = makeNested(FLAT_YAML);
    String topLevelYaml = "top_string: abc\n" + "nested: \n" + nestedFlatTypes;

    Schema schema =
        Schema.builder().addStringField("top_string").addRowField("nested", FLAT_SCHEMA).build();
    Row expectedRow =
        Row.withSchema(schema)
            .withFieldValue("top_string", "abc")
            .withFieldValue("nested", FLAT_ROW)
            .build();

    assertEquals(expectedRow, YamlUtils.toBeamRow(topLevelYaml, schema));
  }

  private static final String INT_ARRAY_YAML =
      "arr:\n" + "  - 1\n" + "  - 2\n" + "  - 3\n" + "  - 4\n" + "  - 5\n";

  private static final Schema INT_ARRAY_SCHEMA =
      Schema.builder().addArrayField("arr", Schema.FieldType.INT32).build();

  private static final Row INT_ARRAY_ROW =
      Row.withSchema(INT_ARRAY_SCHEMA)
          .withFieldValue("arr", IntStream.range(1, 6).boxed().collect(Collectors.toList()))
          .build();

  @Test
  public void testArray() {
    assertEquals(INT_ARRAY_ROW, YamlUtils.toBeamRow(INT_ARRAY_YAML, INT_ARRAY_SCHEMA));
  }

  @Test
  public void testNestedArray() {
    String nestedArray = makeNested(INT_ARRAY_YAML);
    String yamlString = "str_field: some string\n" + "nested: \n" + nestedArray;

    Schema schema =
        Schema.builder()
            .addStringField("str_field")
            .addRowField("nested", INT_ARRAY_SCHEMA)
            .build();

    Row expectedRow =
        Row.withSchema(schema)
            .withFieldValue("str_field", "some string")
            .withFieldValue("nested", INT_ARRAY_ROW)
            .build();

    assertEquals(expectedRow, YamlUtils.toBeamRow(yamlString, schema));
  }

  private static final Schema FLAT_SCHEMA_CAMEL_CASE =
      Schema.builder()
          .addFields(
              FLAT_SCHEMA.getFields().stream()
                  .map(
                      field ->
                          field.withName(
                              CaseFormat.LOWER_UNDERSCORE.to(
                                  CaseFormat.LOWER_CAMEL, field.getName())))
                  .collect(Collectors.toList()))
          .build();

  private static final Map<String, Object> FLAT_MAP =
      FLAT_SCHEMA.getFields().stream()
          .collect(
              Collectors.toMap(
                  Schema.Field::getName,
                  field -> Preconditions.checkArgumentNotNull(FLAT_ROW.getValue(field.getName()))));

  @Test
  public void testSnakeCaseMapToCamelCaseRow() {
    Row expectedRow =
        FLAT_SCHEMA.getFields().stream()
            .map(field -> Preconditions.checkStateNotNull(FLAT_ROW.getValue(field.getName())))
            .collect(Row.toRow(FLAT_SCHEMA_CAMEL_CASE));

    Row convertedRow = YamlUtils.toBeamRow(FLAT_MAP, FLAT_SCHEMA_CAMEL_CASE, true);

    assertEquals(expectedRow, convertedRow);
  }
}
