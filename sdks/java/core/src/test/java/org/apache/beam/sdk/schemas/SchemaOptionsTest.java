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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link Schema.Options}. */
public class SchemaOptionsTest {

  private static final String OPTION_NAME = "beam:test:field_i1";
  private static final String FIELD_NAME = "f_field";
  private static final Field FIELD = Field.of(FIELD_NAME, FieldType.STRING);
  private static final Row TEST_ROW =
      Row.withSchema(
              Schema.builder()
                  .addField("field_one", FieldType.STRING)
                  .addField("field_two", FieldType.INT32)
                  .build())
          .addValue("value")
          .addValue(42)
          .build();

  private static final Map<Integer, String> TEST_MAP = new HashMap<>();

  static {
    TEST_MAP.put(1, "one");
    TEST_MAP.put(2, "two");
  }

  private static final List<String> TEST_LIST = new ArrayList<>();

  static {
    TEST_LIST.add("one");
    TEST_LIST.add("two");
    TEST_LIST.add("three");
  }

  private static final byte[] TEST_BYTES = new byte[] {0x42, 0x69, 0x00};

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBooleanOption() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.BOOLEAN, true).build();
    assertEquals(true, options.getValue(OPTION_NAME));
    assertEquals(FieldType.BOOLEAN, options.getType(OPTION_NAME));
  }

  @Test
  public void testInt16Option() {
    Schema.Options options =
        Schema.Options.setOption(OPTION_NAME, FieldType.INT16, (short) 12).build();
    assertEquals(Short.valueOf((short) 12), options.getValue(OPTION_NAME));
    assertEquals(FieldType.INT16, options.getType(OPTION_NAME));
  }

  @Test
  public void testByteOption() {
    Schema.Options options =
        Schema.Options.setOption(OPTION_NAME, FieldType.BYTE, (byte) 12).build();
    assertEquals(Byte.valueOf((byte) 12), options.getValue(OPTION_NAME));
    assertEquals(FieldType.BYTE, options.getType(OPTION_NAME));
  }

  @Test
  public void testInt32Option() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.INT32, 12).build();
    assertEquals(Integer.valueOf(12), options.getValue(OPTION_NAME));
    assertEquals(FieldType.INT32, options.getType(OPTION_NAME));
  }

  @Test
  public void testInt64Option() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.INT64, 12L).build();
    assertEquals(Long.valueOf(12), options.getValue(OPTION_NAME));
    assertEquals(FieldType.INT64, options.getType(OPTION_NAME));
  }

  @Test
  public void testFloatOption() {
    Schema.Options options =
        Schema.Options.setOption(OPTION_NAME, FieldType.FLOAT, (float) 12.0).build();
    assertEquals(Float.valueOf((float) 12.0), options.getValue(OPTION_NAME));
    assertEquals(FieldType.FLOAT, options.getType(OPTION_NAME));
  }

  @Test
  public void testDoubleOption() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.DOUBLE, 12.0).build();
    assertEquals(Double.valueOf(12.0), options.getValue(OPTION_NAME));
    assertEquals(FieldType.DOUBLE, options.getType(OPTION_NAME));
  }

  @Test
  public void testStringOption() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.STRING, "foo").build();
    assertEquals("foo", options.getValue(OPTION_NAME));
    assertEquals(FieldType.STRING, options.getType(OPTION_NAME));
  }

  @Test
  public void testBytesOption() {
    byte[] bytes = new byte[] {0x42, 0x69, 0x00};
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.BYTES, bytes).build();
    assertEquals(bytes, options.getValue(OPTION_NAME));
    assertEquals(FieldType.BYTES, options.getType(OPTION_NAME));
  }

  @Test
  public void testDateTimeOption() {
    DateTime now = DateTime.now().withZone(DateTimeZone.UTC);
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.DATETIME, now).build();
    assertEquals(now, options.getValue(OPTION_NAME));
    assertEquals(FieldType.DATETIME, options.getType(OPTION_NAME));
  }

  @Test
  public void testArrayOfIntegerOption() {
    Schema.Options options =
        Schema.Options.setOption(OPTION_NAME, FieldType.array(FieldType.STRING), TEST_LIST).build();
    assertEquals(TEST_LIST, options.getValue(OPTION_NAME));
    assertEquals(FieldType.array(FieldType.STRING), options.getType(OPTION_NAME));
  }

  @Test
  public void testMapOfIntegerStringOption() {
    Schema.Options options =
        Schema.Options.setOption(
                OPTION_NAME, FieldType.map(FieldType.INT32, FieldType.STRING), TEST_MAP)
            .build();
    assertEquals(TEST_MAP, options.getValue(OPTION_NAME));
    assertEquals(FieldType.map(FieldType.INT32, FieldType.STRING), options.getType(OPTION_NAME));
  }

  @Test
  public void testRowOption() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, TEST_ROW).build();
    assertEquals(TEST_ROW, options.getValue(OPTION_NAME));
    assertEquals(FieldType.row(TEST_ROW.getSchema()), options.getType(OPTION_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotNullableOptionSetNull() {
    Schema.Options options = Schema.Options.setOption(OPTION_NAME, FieldType.STRING, null).build();
  }

  @Test
  public void testNullableOptionSetNull() {
    Schema.Options options =
        Schema.Options.setOption(OPTION_NAME, FieldType.STRING.withNullable(true), null).build();
    assertNull(options.getValue(OPTION_NAME));
    assertEquals(FieldType.STRING.withNullable(true), options.getType(OPTION_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetValueNoOption() {
    Schema.Options options = Schema.Options.none();
    options.getValue("foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeNoOption() {
    Schema.Options options = Schema.Options.none();
    options.getType("foo");
  }

  @Test
  public void testGetValueOrDefault() {
    Schema.Options options = Schema.Options.none();
    assertNull(options.getValueOrDefault("foo", null));
  }

  private Schema.Options.Builder setOptionsSet1() {
    return setOptionsSet1(Schema.Options.builder());
  }

  private Schema.Options.Builder setOptionsSet1(Schema.Options.Builder builder) {
    return builder
        .setOption("field_option_boolean", FieldType.BOOLEAN, true)
        .setOption("field_option_byte", FieldType.BYTE, (byte) 12)
        .setOption("field_option_int16", FieldType.INT16, (short) 12)
        .setOption("field_option_int32", FieldType.INT32, 12)
        .setOption("field_option_int64", FieldType.INT64, 12L)
        .setOption("field_option_string", FieldType.STRING, "foo");
  }

  private Schema.Options.Builder setOptionsSet2() {
    return setOptionsSet2(Schema.Options.builder());
  }

  private void assertOptionSet1(Schema.Options options) {
    assertEquals(true, options.getValue("field_option_boolean"));
    assertEquals(12, (byte) options.getValue("field_option_byte"));
    assertEquals(12, (short) options.getValue("field_option_int16"));
    assertEquals(12, (int) options.getValue("field_option_int32"));
    assertEquals(12L, (long) options.getValue("field_option_int64"));
    assertEquals("foo", options.getValue("field_option_string"));
  }

  private Schema.Options.Builder setOptionsSet2(Schema.Options.Builder builder) {
    return builder
        .setOption("field_option_bytes", FieldType.BYTES, new byte[] {0x42, 0x69, 0x00})
        .setOption("field_option_float", FieldType.FLOAT, (float) 12.0)
        .setOption("field_option_double", FieldType.DOUBLE, 12.0)
        .setOption("field_option_map", FieldType.map(FieldType.INT32, FieldType.STRING), TEST_MAP)
        .setOption("field_option_array", FieldType.array(FieldType.STRING), TEST_LIST)
        .setOption("field_option_row", TEST_ROW)
        .setOption("field_option_value", FieldType.STRING, "other");
  }

  private void assertOptionSet2(Schema.Options options) {
    assertArrayEquals(TEST_BYTES, options.getValue("field_option_bytes"));
    assertEquals((float) 12.0, (float) options.getValue("field_option_float"), 0.1);
    assertEquals(12.0, (double) options.getValue("field_option_double"), 0.1);
    assertEquals(TEST_MAP, options.getValue("field_option_map"));
    assertEquals(TEST_LIST, options.getValue("field_option_array"));
    assertEquals(TEST_ROW, options.getValue("field_option_row"));
    assertEquals("other", options.getValue("field_option_value"));
  }

  @Test
  public void testSchemaSetOptionWithBuilder() {
    Schema schema =
        Schema.builder()
            .setOptions(setOptionsSet1(Schema.Options.builder()))
            .addField(FIELD)
            .build();
    assertOptionSet1(schema.getOptions());
  }

  @Test
  public void testSchemaSetOption() {
    Schema schema =
        Schema.builder()
            .setOptions(setOptionsSet1(Schema.Options.builder()).build())
            .addField(FIELD)
            .build();
    assertOptionSet1(schema.getOptions());
  }

  @Test
  public void testFieldWithOptionsBuilder() {
    Schema schema = Schema.builder().addField(FIELD.withOptions(setOptionsSet1())).build();
    assertOptionSet1(schema.getField(FIELD_NAME).getOptions());
  }

  @Test
  public void testFieldWithOptions() {
    Schema schema = Schema.builder().addField(FIELD.withOptions(setOptionsSet1().build())).build();
    assertOptionSet1(schema.getField(FIELD_NAME).getOptions());
  }

  @Test
  public void testFieldHasOptions() {
    Schema schema = Schema.builder().addField(FIELD.withOptions(setOptionsSet1().build())).build();
    assertTrue(schema.getField(FIELD_NAME).getOptions().hasOptions());
  }

  @Test
  public void testFieldHasNonOptions() {
    Schema schema = Schema.builder().addField(FIELD).build();
    assertFalse(schema.getField(FIELD_NAME).getOptions().hasOptions());
  }

  @Test
  public void testFieldHasOption() {
    Schema schema = Schema.builder().addField(FIELD.withOptions(setOptionsSet1().build())).build();
    assertTrue(schema.getField(FIELD_NAME).getOptions().hasOption("field_option_byte"));
    assertFalse(schema.getField(FIELD_NAME).getOptions().hasOption("foo_bar"));
  }

  @Test
  public void testFieldOptionNames() {
    Schema schema = Schema.builder().addField(FIELD.withOptions(setOptionsSet1().build())).build();
    Set<String> optionNames = schema.getField(FIELD_NAME).getOptions().getOptionNames();
    assertThat(
        optionNames,
        containsInAnyOrder(
            "field_option_boolean",
            "field_option_byte",
            "field_option_int16",
            "field_option_int32",
            "field_option_int64",
            "field_option_string"));
  }

  @Test
  public void testFieldBuilderSetOptions() {
    Schema schema =
        Schema.builder()
            .addField(FIELD.toBuilder().setOptions(setOptionsSet1().build()).build())
            .build();
    assertOptionSet1(schema.getField(FIELD_NAME).getOptions());
  }

  @Test
  public void testFieldBuilderSetOptionsBuilder() {
    Schema schema =
        Schema.builder().addField(FIELD.toBuilder().setOptions(setOptionsSet1()).build()).build();
    assertOptionSet1(schema.getField(FIELD_NAME).getOptions());
  }

  @Test
  public void testAddOptions() {
    Schema.Options options =
        setOptionsSet1(Schema.Options.builder())
            .addOptions(setOptionsSet2(Schema.Options.builder()).build())
            .build();
    assertOptionSet1(options);
    assertOptionSet2(options);
  }
}
