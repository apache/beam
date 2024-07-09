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
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
  public void testSchemaValidationFailsWithHelpfulError() {
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
      List<String> specifiedFields = fields.getKey();
      List<String> badFields = fields.getValue();

      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  RowFilter.validateSchemaContainsFields(
                      ROW_SCHEMA, specifiedFields, "test-operation"));

      assertThat(e.getMessage(), containsString("Validation failed for test-operation"));
      assertThat(
          e.getMessage(),
          containsString("Row Schema does not contain the following specified fields"));
      for (String badField : badFields) {
        assertThat(e.getMessage(), containsString(badField));
      }
    }

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
      List<String> specifiedFields = fields.getKey();
      List<String> badFields = fields.getValue();

      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  RowFilter.validateSchemaContainsFields(
                      ROW_SCHEMA, specifiedFields, "test-operation"));

      assertThat(e.getMessage(), containsString("Validation failed for test-operation"));
      assertThat(
          e.getMessage(),
          containsString(
              "The following specified fields are not of type Row. Their nested fields could not be reached"));
      for (String badField : badFields) {
        assertThat(e.getMessage(), containsString(badField));
      }
    }
  }
}
