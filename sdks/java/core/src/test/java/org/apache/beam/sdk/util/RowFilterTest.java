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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link RowFilter}. */
public class RowFilterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Schema DOUBLY_NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("doubly_nested_str")
          .addInt32Field("doubly_nested_int")
          .build();

  private static final Schema NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("nested_str")
          .addInt32Field("nested_int")
          .addFloatField("nested_float")
          .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
          .build();
  private static final Schema ROW_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addBooleanField("bool")
          .addNullableInt32Field("nullable_int")
          .addArrayField("arr_int", Schema.FieldType.INT32)
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .build();

  @Test
  public void testSchemaValidation() {
    List<List<String>> goodFields =
        Arrays.asList(
            Arrays.asList("str", "bool", "nullable_row"),
            Arrays.asList("nullable_int", "arr_int"),
            Arrays.asList("row.nested_str", "row.nested_row.doubly_nested_str"),
            Arrays.asList("nullable_row.nested_row.doubly_nested_int"));

    for (List<String> fields : goodFields) {
      RowFilter.validateSchemaContainsFields(ROW_SCHEMA, fields, "test-operation");
    }
  }

  @Test
  public void testSchemaValidationFailsWithHelpfulErrorForMissingFields() {
    List<KV<List<String>, List<String>>> nonExistentFields =
        Arrays.asList(
            KV.of(
                Arrays.asList("nonexistent_1", "nonexistent_2", "nonexistent_3"),
                Arrays.asList("nonexistent_1", "nonexistent_2", "nonexistent_3")),
            KV.of(
                Arrays.asList("nullable_int", "arr_int", "nonexistent"),
                Collections.singletonList("nonexistent")),
            KV.of(
                Arrays.asList(
                    "nullable_row.nested_row.nonexistent", "row.nonexistent", "row.nested_float"),
                Arrays.asList("nullable_row.nested_row.nonexistent", "row.nonexistent")));

    for (KV<List<String>, List<String>> fields : nonExistentFields) {
      List<String> allFields = fields.getKey();
      List<String> badFields = fields.getValue();

      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  RowFilter.validateSchemaContainsFields(ROW_SCHEMA, allFields, "test-operation"));

      assertThat(e.getMessage(), containsString("Validation failed for 'test-operation'"));
      assertThat(
          e.getMessage(),
          containsString("Row Schema does not contain the following specified fields"));
      for (String badField : badFields) {
        assertThat(e.getMessage(), containsString(badField));
      }
    }
  }

  @Test
  public void testSchemaValidationFailsWithHelpfulErrorForInvalidNestedFields() {
    List<KV<List<String>, List<String>>> nonNestedFields =
        Arrays.asList(
            KV.of(
                Arrays.asList(
                    "row.nested_row", "row.nested_int", "row.nested_str.unexpected_nested"),
                Collections.singletonList("row.nested_str")),
            KV.of(
                Arrays.asList(
                    "nullable_row.nested_str",
                    "nullable_row.nested_str.unexpected",
                    "row.nested_int.unexpected_2"),
                Arrays.asList("nullable_row.nested_str", "row.nested_int")));

    for (KV<List<String>, List<String>> fields : nonNestedFields) {
      List<String> allFields = fields.getKey();
      List<String> badFields = fields.getValue();

      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  RowFilter.validateSchemaContainsFields(ROW_SCHEMA, allFields, "test-operation"));

      assertThat(e.getMessage(), containsString("Validation failed for 'test-operation'"));
      assertThat(
          e.getMessage(),
          containsString(
              "The following specified fields are not of type Row. Their nested fields could not be reached"));
      for (String badField : badFields) {
        assertThat(e.getMessage(), containsString(badField));
      }
    }
  }

  @Test
  public void testGetFieldTree() {
    List<String> fields =
        Arrays.asList(
            "top-level",
            "top-level-2",
            "top-level.nested-level",
            "top-level.nested-level-2",
            "top-level.nested-level.doubly-nested-level",
            "top-level.nested-level.doubly-nested-level-2");
    List<String> nestedLayer =
        Arrays.asList(
            "nested-level",
            "nested-level-2",
            "nested-level.doubly-nested-level",
            "nested-level.doubly-nested-level-2");

    Map<String, List<String>> expectedTree =
        ImmutableMap.<String, List<String>>builder()
            .put("top-level-2", Collections.emptyList())
            .put("top-level", nestedLayer)
            .build();

    assertEquals(expectedTree, RowFilter.getFieldTree(fields));

    List<String> doublyNestedLayer = Arrays.asList("doubly-nested-level", "doubly-nested-level-2");

    Map<String, List<String>> expectedNestedTree =
        ImmutableMap.<String, List<String>>builder()
            .put("nested-level-2", Collections.emptyList())
            .put("nested-level", doublyNestedLayer)
            .build();

    assertEquals(expectedNestedTree, RowFilter.getFieldTree(nestedLayer));
  }

  @Test
  public void testDropSchemaFields() {
    List<String> fieldsToDrop =
        Arrays.asList(
            "str",
            "arr_int",
            "nullable_int",
            "row.nested_int",
            "row.nested_float",
            "row.nested_row.doubly_nested_int",
            "nullable_row.nested_str",
            "nullable_row.nested_row");

    Schema expectedDroppedSchema =
        Schema.builder()
            .addBooleanField("bool")
            .addRowField(
                "row",
                Schema.builder()
                    .addStringField("nested_str")
                    .addRowField(
                        "nested_row", Schema.builder().addStringField("doubly_nested_str").build())
                    .build())
            .addNullableRowField(
                "nullable_row",
                Schema.builder().addInt32Field("nested_int").addFloatField("nested_float").build())
            .build();

    assertTrue(expectedDroppedSchema.equivalent(RowFilter.dropFields(ROW_SCHEMA, fieldsToDrop)));
  }

  @Test
  public void testKeepSchemaFields() {
    List<String> fieldsToKeep =
        Arrays.asList(
            "str",
            "arr_int",
            "nullable_int",
            "row.nested_int",
            "row.nested_float",
            "row.nested_row.doubly_nested_int",
            "nullable_row.nested_str",
            "nullable_row.nested_row");

    Schema expectedKeptSchema =
        Schema.builder()
            .addStringField("str")
            .addArrayField("arr_int", Schema.FieldType.INT32)
            .addNullableInt32Field("nullable_int")
            .addRowField(
                "row",
                Schema.builder()
                    .addInt32Field("nested_int")
                    .addFloatField("nested_float")
                    .addRowField(
                        "nested_row", Schema.builder().addInt32Field("doubly_nested_int").build())
                    .build())
            .addNullableRowField(
                "nullable_row",
                Schema.builder()
                    .addStringField("nested_str")
                    .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
                    .build())
            .build();

    assertTrue(expectedKeptSchema.equivalent(RowFilter.keepFields(ROW_SCHEMA, fieldsToKeep)));
  }

  @Test
  public void testDropNestedFieldsFails() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("'drop' does not support nested fields");

    new RowFilter(ROW_SCHEMA)
        .dropping(
            Arrays.asList(
                "bool",
                "nullable_int",
                "row.nested_int",
                "row.nested_float",
                "row.nested_row.doubly_nested_int",
                "nullable_row"));
  }

  @Test
  public void testKeepNestedFieldsFails() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("'keep' does not support nested fields");

    new RowFilter(ROW_SCHEMA)
        .keeping(
            Arrays.asList("str", "arr_int", "row.nested_str", "row.nested_row.doubly_nested_str"));
  }

  @Test
  public void testOnlyFailsWhenSpecifyingNonRowField() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Expected type 'ROW' for field 'nullable_int', but instead got type 'INT32'");

    new RowFilter(ROW_SCHEMA).only("nullable_int");
  }

  private static final Row ORIGINAL_ROW =
      Row.withSchema(ROW_SCHEMA)
          .addValue("str_value")
          .addValue(true)
          .addValue(123)
          .addValue(Arrays.asList(1, 2, 3, 4, 5))
          .addValue(
              Row.withSchema(NESTED_ROW_SCHEMA)
                  .addValue("nested_str_value")
                  .addValue(456)
                  .addValue(1.234f)
                  .addValue(
                      Row.withSchema(DOUBLY_NESTED_ROW_SCHEMA)
                          .addValue("doubly_nested_str_value")
                          .addValue(789)
                          .build())
                  .build())
          .addValue(null)
          .build();

  private static final Schema FILTERED_DOUBLY_NESTED_SCHEMA =
      Schema.builder().addStringField("doubly_nested_str").build();
  private static final Schema FILTERED_NESTED_SCHEMA =
      Schema.builder()
          .addStringField("nested_str")
          .addRowField("nested_row", FILTERED_DOUBLY_NESTED_SCHEMA)
          .build();
  private static final Schema FILTERED_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addArrayField("arr_int", Schema.FieldType.INT32)
          .addRowField("row", FILTERED_NESTED_SCHEMA)
          .build();

  private static final Row FILTERED_ROW =
      Row.withSchema(FILTERED_SCHEMA)
          .addValue("str_value")
          .addValue(Arrays.asList(1, 2, 3, 4, 5))
          .addValue(
              Row.withSchema(FILTERED_NESTED_SCHEMA)
                  .addValue("nested_str_value")
                  .addValue(
                      Row.withSchema(FILTERED_DOUBLY_NESTED_SCHEMA)
                          .addValue("doubly_nested_str_value")
                          .build())
                  .build())
          .build();

  @Test
  public void testCopyRowWithNewSchema() {
    assertEquals(FILTERED_ROW, RowFilter.copyWithNewSchema(ORIGINAL_ROW, FILTERED_SCHEMA));
  }

  @Test
  public void testOnlyRowField() {
    RowFilter rowFilter = new RowFilter(ROW_SCHEMA).only("row");

    Row expecedRow =
        Row.withSchema(rowFilter.outputSchema())
            .addValues(ORIGINAL_ROW.getRow("row").getValues())
            .build();

    assertEquals(expecedRow, rowFilter.filter(ORIGINAL_ROW));
  }
}
