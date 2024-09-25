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

import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test class for {@link RowStringInterpolator}. */
public class RowStringInterpolatorTest {
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
          .addInt32Field("int")
          .addNullableInt32Field("nullable_int")
          .addArrayField("arr_int", Schema.FieldType.INT32)
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .build();

  @Test
  public void testInvalidRowThrowsHelpfulError() {
    String template = "foo {str}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    Row invalidRow = Row.nullRow(Schema.builder().addNullableStringField("xyz").build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid row does not contain field 'str'.");

    interpolator.interpolate(invalidRow, null, null, null);
  }

  @Test
  public void testInvalidRowThrowsHelpfulErrorForNestedFields() {
    String template = "foo {row.nested_int}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    Schema nestedSchema = Schema.builder().addNullableStringField("xyz").build();
    Row invalidRow =
        Row.withSchema(Schema.builder().addNullableRowField("row", nestedSchema).build())
            .addValue(Row.nullRow(nestedSchema))
            .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid row does not contain field 'nested_int'.");

    interpolator.interpolate(invalidRow, null, null, null);
  }

  @Test
  public void testInvalidRowThrowsHelpfulErrorForDoublyNestedFields() {
    String template = "foo {row.nested_row.doubly_nested_int}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    Schema doublyNestedSchema = Schema.builder().addNullableStringField("xyz").build();
    Schema nestedSchema =
        Schema.builder().addNullableRowField("nested_row", doublyNestedSchema).build();
    Row invalidRow =
        Row.withSchema(Schema.builder().addNullableRowField("row", doublyNestedSchema).build())
            .addValue(
                Row.withSchema(nestedSchema).addValue(Row.nullRow(doublyNestedSchema)).build())
            .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid row does not contain field 'doubly_nested_int'.");

    interpolator.interpolate(invalidRow, null, null, null);
  }

  private static final Row ROW =
      Row.withSchema(ROW_SCHEMA)
          .addValue("str_value")
          .addValue(true)
          .addValue(123)
          .addValue(null)
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

  @Test
  public void testTopLevelInterpolation() {
    String template = "foo {str}, bar {bool}, baz {int}, xyz {nullable_int}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    String output = interpolator.interpolate(ROW, null, null, null);

    assertEquals("foo str_value, bar true, baz 123, xyz ", output);
  }

  @Test
  public void testNestedLevelInterpolation() {
    String template = "foo {str}, bar {row.nested_str}, baz {row.nested_float}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    String output = interpolator.interpolate(ROW, null, null, null);

    assertEquals("foo str_value, bar nested_str_value, baz 1.234", output);
  }

  @Test
  public void testDoublyNestedInterpolation() {
    String template =
        "foo {str}, bar {row.nested_row.doubly_nested_str}, baz {row.nested_row.doubly_nested_int}";
    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    String output = interpolator.interpolate(ROW, null, null, null);

    assertEquals("foo str_value, bar doubly_nested_str_value, baz 789", output);
  }

  @Test
  public void testInterpolateWindowingInformation() {
    String template =
        String.format(
            "str: {str}, window: {%s}, pane: {%s}, year: {%s}, month: {%s}, day: {%s}",
            RowStringInterpolator.WINDOW,
            RowStringInterpolator.PANE_INDEX,
            RowStringInterpolator.YYYY,
            RowStringInterpolator.MM,
            RowStringInterpolator.DD);

    RowStringInterpolator interpolator = new RowStringInterpolator(template, ROW_SCHEMA);

    Instant instant = new DateTime(2024, 8, 28, 12, 0).toInstant();

    String output =
        interpolator.interpolate(
            ROW,
            GlobalWindow.INSTANCE,
            PaneInfo.createPane(false, false, PaneInfo.Timing.ON_TIME, 2, 0),
            instant);
    String expected =
        String.format(
            "str: str_value, window: %s, pane: 2, year: 2024, month: 8, day: 28",
            GlobalWindow.INSTANCE);

    assertEquals(expected, output);
  }
}
