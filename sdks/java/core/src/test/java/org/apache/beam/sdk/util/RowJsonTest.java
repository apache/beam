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

import static org.apache.beam.sdk.testing.JsonMatcher.jsonStringLike;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.stringContainsInOrder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.NullBehavior;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link RowJson.RowJsonDeserializer} and {@link RowJson.RowJsonSerializer}. */
@RunWith(Enclosed.class)
public class RowJsonTest {
  @RunWith(Parameterized.class)
  public static class ValueTests {
    @Parameter(0)
    public String name;

    @Parameter(1)
    public Schema schema;

    @Parameter(2)
    public String serializedString;

    @Parameter(3)
    public Row row;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return ImmutableList.of(
          makeFlatRowTestCase(),
          makeLogicalTypeTestCase(),
          makeArrayFieldTestCase(),
          makeArrayOfArraysTestCase(),
          makeNestedRowTestCase(),
          makeDoublyNestedRowTestCase(),
          makeNullsTestCase());
    }

    private static Object[] makeFlatRowTestCase() {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addInt16Field("f_int16")
              .addInt32Field("f_int32")
              .addInt64Field("f_int64")
              .addFloatField("f_float")
              .addDoubleField("f_double")
              .addBooleanField("f_boolean")
              .addStringField("f_string")
              .addDecimalField("f_decimal")
              .build();

      String rowString =
          "{\n"
              + "\"f_byte\" : 12,\n"
              + "\"f_int16\" : 22,\n"
              + "\"f_int32\" : 32,\n"
              + "\"f_int64\" : 42,\n"
              + "\"f_float\" : 1.02E5,\n"
              + "\"f_double\" : 62.2,\n"
              + "\"f_boolean\" : true,\n"
              + "\"f_string\" : \"hello\",\n"
              + "\"f_decimal\" : 123.12\n"
              + "}";

      Row expectedRow =
          Row.withSchema(schema)
              .addValues(
                  (byte) 12,
                  (short) 22,
                  32,
                  (long) 42,
                  1.02E5f,
                  62.2d,
                  true,
                  "hello",
                  new BigDecimal("123.12"))
              .build();

      return new Object[] {"Flat row", schema, rowString, expectedRow};
    }

    private static Object[] makeLogicalTypeTestCase() {
      Schema schema =
          Schema.builder()
              .addLogicalTypeField(
                  "f_passThroughString",
                  new PassThroughLogicalType<String>(
                      "SqlCharType", FieldType.STRING, "", FieldType.STRING) {})
              .build();

      String rowString = "{\n" + "\"f_passThroughString\" : \"hello\"\n" + "}";

      Row expectedRow = Row.withSchema(schema).addValues("hello").build();

      return new Object[] {"Logical Types", schema, rowString, expectedRow};
    }

    private static Object[] makeArrayFieldTestCase() {

      Schema schema =
          Schema.builder()
              .addInt32Field("f_int32")
              .addArrayField("f_intArray", FieldType.INT32)
              .build();

      String rowString =
          "{\n" + "\"f_int32\" : 32,\n" + "\"f_intArray\" : [ 1, 2, 3, 4, 5]\n" + "}";

      Row expectedRow = Row.withSchema(schema).addValues(32, Arrays.asList(1, 2, 3, 4, 5)).build();

      return new Object[] {"Array field", schema, rowString, expectedRow};
    }

    private static Object[] makeArrayOfArraysTestCase() {
      Schema schema =
          Schema.builder()
              .addArrayField("f_arrayOfIntArrays", FieldType.array(FieldType.INT32))
              .build();

      String rowString = "{\n" + "\"f_arrayOfIntArrays\" : [ [1, 2], [3, 4], [5]]\n" + "}";

      Row expectedRow =
          Row.withSchema(schema)
              .addArray(Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5))
              .build();

      return new Object[] {"Array of arrays", schema, rowString, expectedRow};
    }

    private static Object[] makeNestedRowTestCase() {
      Schema nestedRowSchema =
          Schema.builder().addInt32Field("f_nestedInt32").addStringField("f_nestedString").build();

      Schema schema =
          Schema.builder().addInt32Field("f_int32").addRowField("f_row", nestedRowSchema).build();

      String rowString =
          "{\n"
              + "\"f_int32\" : 32,\n"
              + "\"f_row\" : {\n"
              + "             \"f_nestedInt32\" : 54,\n"
              + "             \"f_nestedString\" : \"foo\"\n"
              + "            }\n"
              + "}";

      Row expectedRow =
          Row.withSchema(schema)
              .addValues(32, Row.withSchema(nestedRowSchema).addValues(54, "foo").build())
              .build();

      return new Object[] {"Nested row", schema, rowString, expectedRow};
    }

    private static Object[] makeDoublyNestedRowTestCase() {

      Schema doubleNestedRowSchema =
          Schema.builder().addStringField("f_doubleNestedString").build();

      Schema nestedRowSchema =
          Schema.builder().addRowField("f_nestedRow", doubleNestedRowSchema).build();

      Schema schema = Schema.builder().addRowField("f_row", nestedRowSchema).build();

      String rowString =
          "{\n"
              + "\"f_row\" : {\n"
              + "             \"f_nestedRow\" : {\n"
              + "                                \"f_doubleNestedString\":\"foo\"\n"
              + "                             }\n"
              + "            }\n"
              + "}";

      Row expectedRow =
          Row.withSchema(schema)
              .addValues(
                  Row.withSchema(nestedRowSchema)
                      .addValues(Row.withSchema(doubleNestedRowSchema).addValues("foo").build())
                      .build())
              .build();

      return new Object[] {"Doubly nested row", schema, rowString, expectedRow};
    }

    private static Object[] makeNullsTestCase() {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addNullableField("f_string", FieldType.STRING)
              .build();

      String rowString = "{\n" + "\"f_byte\" : 12,\n" + "\"f_string\" : null\n" + "}";

      Row expectedRow = Row.withSchema(schema).addValues((byte) 12, null).build();

      return new Object[] {"Nulls", schema, rowString, expectedRow};
    }

    @Test
    public void testDeserialize() throws IOException {
      Row parsedRow =
          newObjectMapperWith(
                  RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema))
              .readValue(serializedString, Row.class);

      assertThat(row, equalTo(parsedRow));
    }

    @Test
    public void testSerialize() throws IOException {
      String str =
          newObjectMapperWith(
                  RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema))
              .writeValueAsString(row);

      assertThat(str, jsonStringLike(serializedString));
    }

    @Test
    public void testRoundTrip_KeepNulls_AcceptMissingOrNull() throws IOException {
      ObjectMapper objectMapper =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema).withDropNullsOnWrite(false),
              RowJsonDeserializer.forSchema(schema)
                  .withNullBehavior(NullBehavior.ACCEPT_MISSING_OR_NULL));
      Row parsedRow = objectMapper.readValue(objectMapper.writeValueAsString(row), Row.class);

      assertThat(row, equalTo(parsedRow));
    }

    @Test
    public void testRoundTrip_DropNulls_AcceptMissingOrNull() throws IOException {
      ObjectMapper objectMapper =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema).withDropNullsOnWrite(true),
              RowJsonDeserializer.forSchema(schema)
                  .withNullBehavior(NullBehavior.ACCEPT_MISSING_OR_NULL));
      Row parsedRow = objectMapper.readValue(objectMapper.writeValueAsString(row), Row.class);

      assertThat(row, equalTo(parsedRow));
    }

    @Test
    public void testRoundTrip_DropNulls_RequireMissing() throws IOException {
      ObjectMapper objectMapper =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema).withDropNullsOnWrite(true),
              RowJsonDeserializer.forSchema(schema).withNullBehavior(NullBehavior.REQUIRE_MISSING));
      Row parsedRow = objectMapper.readValue(objectMapper.writeValueAsString(row), Row.class);

      assertThat(row, equalTo(parsedRow));
    }

    @Test
    public void testRoundTrip_KeepNulls_RequireNull() throws IOException {
      ObjectMapper objectMapper =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema).withDropNullsOnWrite(false),
              RowJsonDeserializer.forSchema(schema).withNullBehavior(NullBehavior.REQUIRE_NULL));
      Row parsedRow = objectMapper.readValue(objectMapper.writeValueAsString(row), Row.class);

      assertThat(row, equalTo(parsedRow));
    }
  }

  @RunWith(JUnit4.class)
  public static class DeserializerTests {
    private static final Boolean BOOLEAN_TRUE_VALUE = true;
    private static final String BOOLEAN_TRUE_STRING = "true";
    private static final Byte BYTE_VALUE = 126;
    private static final String BYTE_STRING = "126";
    private static final Short SHORT_VALUE = 32766;
    private static final String SHORT_STRING = "32766";
    private static final Integer INT_VALUE = 2147483646;
    private static final String INT_STRING = "2147483646";
    private static final Long LONG_VALUE = 9223372036854775806L;
    private static final String LONG_STRING = "9223372036854775806";
    private static final Float FLOAT_VALUE = 1.02e5f;
    private static final String FLOAT_STRING = "1.02e5";
    private static final Double DOUBLE_VALUE = 1.02d;
    private static final String DOUBLE_STRING = "1.02";

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testThrowsForMismatchedArrayField() throws Exception {

      Schema schema =
          Schema.builder()
              .addArrayField("f_arrayOfIntArrays", FieldType.array(FieldType.INT32))
              .build();

      String rowString =
          "{\n"
              + "\"f_arrayOfIntArrays\" : { }\n" // expect array, get object
              + "}";

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("Expected JSON array");

      newObjectMapperWith(
              RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema))
          .readValue(rowString, Row.class);
    }

    @Test
    public void testThrowsForMismatchedRowField() throws Exception {
      Schema nestedRowSchema =
          Schema.builder().addInt32Field("f_nestedInt32").addStringField("f_nestedString").build();

      Schema schema =
          Schema.builder().addInt32Field("f_int32").addRowField("f_row", nestedRowSchema).build();

      String rowString =
          "{\n"
              + "\"f_int32\" : 32,\n"
              + "\"f_row\" : []\n" // expect object, get array
              + "}";

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("Expected JSON object");

      newObjectMapperWith(
              RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema))
          .readValue(rowString, Row.class);
    }

    @Test
    public void testThrowsForMissingNotNullableField() throws Exception {
      Schema schema = Schema.builder().addByteField("f_byte").addStringField("f_string").build();

      String rowString = "{\n" + "\"f_byte\" : 12\n" + "}";

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("'f_string' is not present");

      newObjectMapperWith(RowJsonDeserializer.forSchema(schema)).readValue(rowString, Row.class);
    }

    @Test
    public void testRequireNullThrowsForMissingNullableField() throws Exception {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addNullableField("f_string", FieldType.STRING)
              .build();

      String rowString = "{\n" + "\"f_byte\" : 12\n" + "}";

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("'f_string'");

      newObjectMapperWith(
              RowJsonDeserializer.forSchema(schema).withNullBehavior(NullBehavior.REQUIRE_NULL))
          .readValue(rowString, Row.class);
    }

    @Test
    public void testRequireNullAcceptsNullValue() throws Exception {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addNullableField("f_string", FieldType.STRING)
              .build();

      String rowString = "{\"f_byte\": 12, \"f_string\": null}";

      assertThat(
          newObjectMapperWith(
                  RowJsonDeserializer.forSchema(schema).withNullBehavior(NullBehavior.REQUIRE_NULL))
              .readValue(rowString, Row.class),
          equalTo(
              Row.withSchema(schema)
                  .withFieldValue("f_byte", (byte) 12)
                  .withFieldValue("f_string", null)
                  .build()));
    }

    @Test
    public void testRequireMissingThrowsForNullValue() throws Exception {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addNullableField("f_string", FieldType.STRING)
              .build();

      String rowString = "{\"f_byte\": 12, \"f_string\": null}";

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("'f_string'");

      newObjectMapperWith(
              RowJsonDeserializer.forSchema(schema).withNullBehavior(NullBehavior.REQUIRE_MISSING))
          .readValue(rowString, Row.class);
    }

    @Test
    public void testRequireMissingAcceptsMissingField() throws Exception {
      Schema schema =
          Schema.builder()
              .addByteField("f_byte")
              .addNullableField("f_string", FieldType.STRING)
              .build();

      String rowString = "{\"f_byte\": 12}";

      assertThat(
          newObjectMapperWith(
                  RowJsonDeserializer.forSchema(schema)
                      .withNullBehavior(NullBehavior.REQUIRE_MISSING))
              .readValue(rowString, Row.class),
          equalTo(
              Row.withSchema(schema)
                  .withFieldValue("f_byte", (byte) 12)
                  .withFieldValue("f_string", null)
                  .build()));
    }

    @Test
    public void testDeserializerThrowsForUnsupportedType() throws Exception {
      Schema schema = Schema.builder().addDateTimeField("f_dateTime").build();

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("DATETIME");
      thrown.expectMessage("f_dateTime");
      thrown.expectMessage("not supported");

      RowJson.RowJsonDeserializer.forSchema(schema);
    }

    @Test
    public void testDeserializerThrowsForUnsupportedArrayElementType() throws Exception {
      Schema schema = Schema.builder().addArrayField("f_dateTimeArray", FieldType.DATETIME).build();

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("DATETIME");
      thrown.expectMessage("f_dateTimeArray[]");
      thrown.expectMessage("not supported");

      RowJson.RowJsonDeserializer.forSchema(schema);
    }

    @Test
    public void testDeserializerThrowsForUnsupportedNestedFieldType() throws Exception {
      Schema nestedSchema =
          Schema.builder().addArrayField("f_dateTimeArray", FieldType.DATETIME).build();

      Schema schema = Schema.builder().addRowField("f_nestedRow", nestedSchema).build();

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("DATETIME");
      thrown.expectMessage("f_nestedRow.f_dateTimeArray[]");
      thrown.expectMessage("not supported");

      RowJson.RowJsonDeserializer.forSchema(schema);
    }

    @Test
    public void testDeserializerThrowsForMultipleUnsupportedFieldTypes() throws Exception {
      Schema schema =
          Schema.builder()
              .addInt32Field("f_int32")
              .addDateTimeField("f_dateTime")
              .addArrayField("f_dateTimeArray", FieldType.DATETIME)
              .build();

      thrown.expect(UnsupportedRowJsonException.class);
      thrown.expectMessage("f_dateTime=DATETIME");
      thrown.expectMessage("f_dateTimeArray[]=DATETIME");
      thrown.expectMessage("not supported");

      RowJson.RowJsonDeserializer.forSchema(schema);
    }

    @Test
    public void testSupportedBooleanConversions() throws Exception {
      testSupportedConversion(FieldType.BOOLEAN, BOOLEAN_TRUE_STRING, BOOLEAN_TRUE_VALUE);
    }

    @Test
    public void testSupportedStringConversions() throws Exception {
      testSupportedConversion(FieldType.STRING, quoted(FLOAT_STRING), FLOAT_STRING);
    }

    @Test
    public void testSupportedByteConversions() throws Exception {
      testSupportedConversion(FieldType.BYTE, BYTE_STRING, BYTE_VALUE);
    }

    @Test
    public void testSupportedShortConversions() throws Exception {
      testSupportedConversion(FieldType.INT16, BYTE_STRING, (short) BYTE_VALUE);
      testSupportedConversion(FieldType.INT16, SHORT_STRING, SHORT_VALUE);
    }

    @Test
    public void testSupportedIntConversions() throws Exception {
      testSupportedConversion(FieldType.INT32, BYTE_STRING, (int) BYTE_VALUE);
      testSupportedConversion(FieldType.INT32, SHORT_STRING, (int) SHORT_VALUE);
      testSupportedConversion(FieldType.INT32, INT_STRING, INT_VALUE);
    }

    @Test
    public void testSupportedLongConversions() throws Exception {
      testSupportedConversion(FieldType.INT64, BYTE_STRING, (long) BYTE_VALUE);
      testSupportedConversion(FieldType.INT64, SHORT_STRING, (long) SHORT_VALUE);
      testSupportedConversion(FieldType.INT64, INT_STRING, (long) INT_VALUE);
      testSupportedConversion(FieldType.INT64, LONG_STRING, LONG_VALUE);
    }

    @Test
    public void testSupportedFloatConversions() throws Exception {
      testSupportedConversion(FieldType.FLOAT, FLOAT_STRING, FLOAT_VALUE);
      testSupportedConversion(FieldType.FLOAT, SHORT_STRING, (float) SHORT_VALUE);
    }

    @Test
    public void testSupportedDoubleConversions() throws Exception {
      testSupportedConversion(FieldType.DOUBLE, DOUBLE_STRING, DOUBLE_VALUE);
      testSupportedConversion(FieldType.DOUBLE, FLOAT_STRING, (double) FLOAT_VALUE);
      testSupportedConversion(FieldType.DOUBLE, INT_STRING, (double) INT_VALUE);
    }

    private void testSupportedConversion(
        FieldType fieldType, String jsonFieldValue, Object expectedRowFieldValue) throws Exception {

      String fieldName = "f_" + fieldType.getTypeName().name().toLowerCase();
      Schema schema = schemaWithField(fieldName, fieldType);
      Row expectedRow = Row.withSchema(schema).addValues(expectedRowFieldValue).build();
      ObjectMapper jsonParser =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema));

      Row parsedRow = jsonParser.readValue(jsonObjectWith(fieldName, jsonFieldValue), Row.class);

      assertThat(expectedRow, equalTo(parsedRow));
    }

    @Test
    public void testUnsupportedBooleanConversions() throws Exception {
      testUnsupportedConversion(FieldType.BOOLEAN, quoted(BOOLEAN_TRUE_STRING));
      testUnsupportedConversion(FieldType.BOOLEAN, BYTE_STRING);
      testUnsupportedConversion(FieldType.BOOLEAN, SHORT_STRING);
      testUnsupportedConversion(FieldType.BOOLEAN, INT_STRING);
      testUnsupportedConversion(FieldType.BOOLEAN, LONG_STRING);
      testUnsupportedConversion(FieldType.BOOLEAN, FLOAT_STRING);
      testUnsupportedConversion(FieldType.BOOLEAN, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedStringConversions() throws Exception {
      testUnsupportedConversion(FieldType.STRING, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.STRING, BYTE_STRING);
      testUnsupportedConversion(FieldType.STRING, SHORT_STRING);
      testUnsupportedConversion(FieldType.STRING, INT_STRING);
      testUnsupportedConversion(FieldType.STRING, LONG_STRING);
      testUnsupportedConversion(FieldType.STRING, FLOAT_STRING);
      testUnsupportedConversion(FieldType.STRING, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedByteConversions() throws Exception {
      testUnsupportedConversion(FieldType.BYTE, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.BYTE, quoted(BYTE_STRING));
      testUnsupportedConversion(FieldType.BYTE, SHORT_STRING);
      testUnsupportedConversion(FieldType.BYTE, INT_STRING);
      testUnsupportedConversion(FieldType.BYTE, LONG_STRING);
      testUnsupportedConversion(FieldType.BYTE, FLOAT_STRING);
      testUnsupportedConversion(FieldType.BYTE, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedShortConversions() throws Exception {
      testUnsupportedConversion(FieldType.INT16, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.INT16, quoted(SHORT_STRING));
      testUnsupportedConversion(FieldType.INT16, INT_STRING);
      testUnsupportedConversion(FieldType.INT16, LONG_STRING);
      testUnsupportedConversion(FieldType.INT16, FLOAT_STRING);
      testUnsupportedConversion(FieldType.INT16, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedIntConversions() throws Exception {
      testUnsupportedConversion(FieldType.INT32, quoted(INT_STRING));
      testUnsupportedConversion(FieldType.INT32, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.INT32, LONG_STRING);
      testUnsupportedConversion(FieldType.INT32, FLOAT_STRING);
      testUnsupportedConversion(FieldType.INT32, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedLongConversions() throws Exception {
      testUnsupportedConversion(FieldType.INT64, quoted(LONG_STRING));
      testUnsupportedConversion(FieldType.INT64, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.INT64, FLOAT_STRING);
      testUnsupportedConversion(FieldType.INT64, DOUBLE_STRING);
    }

    @Test
    public void testUnsupportedFloatConversions() throws Exception {
      testUnsupportedConversion(FieldType.FLOAT, quoted(FLOAT_STRING));
      testUnsupportedConversion(FieldType.FLOAT, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.FLOAT, DOUBLE_STRING);
      testUnsupportedConversion(FieldType.FLOAT, INT_STRING); // too large to fit
    }

    @Test
    public void testUnsupportedDoubleConversions() throws Exception {
      testUnsupportedConversion(FieldType.DOUBLE, quoted(DOUBLE_STRING));
      testUnsupportedConversion(FieldType.DOUBLE, BOOLEAN_TRUE_STRING);
      testUnsupportedConversion(FieldType.DOUBLE, LONG_STRING); // too large to fit
    }

    private void testUnsupportedConversion(FieldType fieldType, String jsonFieldValue)
        throws Exception {

      String fieldName = "f_" + fieldType.getTypeName().name().toLowerCase();

      Schema schema = schemaWithField(fieldName, fieldType);
      ObjectMapper jsonParser =
          newObjectMapperWith(
              RowJsonSerializer.forSchema(schema), RowJsonDeserializer.forSchema(schema));

      thrown.expectMessage(fieldName);
      thrown.expectCause(unsupportedWithMessage(jsonFieldValue, "out of range"));

      jsonParser.readValue(jsonObjectWith(fieldName, jsonFieldValue), Row.class);
    }
  }

  private static String quoted(String string) {
    return "\"" + string + "\"";
  }

  private static Schema schemaWithField(String fieldName, FieldType fieldType) {
    return Schema.builder().addField(fieldName, fieldType).build();
  }

  private static String jsonObjectWith(String fieldName, String fieldValue) {
    return "{\n" + "\"" + fieldName + "\" : " + fieldValue + "\n" + "}";
  }

  private static Matcher<UnsupportedRowJsonException> unsupportedWithMessage(String... message) {
    return allOf(
        Matchers.isA(UnsupportedRowJsonException.class),
        hasProperty("message", stringContainsInOrder(Arrays.asList(message))));
  }

  private static ObjectMapper newObjectMapperWith(
      RowJsonSerializer ser, RowJsonDeserializer deser) {
    SimpleModule simpleModule = new SimpleModule("rowSerializationTesModule");
    simpleModule.addSerializer(Row.class, ser);
    simpleModule.addDeserializer(Row.class, deser);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(simpleModule);
    return objectMapper;
  }

  private static ObjectMapper newObjectMapperWith(RowJsonDeserializer deser) {
    SimpleModule simpleModule = new SimpleModule("rowSerializationTesModule");
    simpleModule.addDeserializer(Row.class, deser);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(simpleModule);
    return objectMapper;
  }
}
