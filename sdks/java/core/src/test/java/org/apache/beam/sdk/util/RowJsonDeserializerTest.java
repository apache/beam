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

import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BYTE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.STRING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJsonDeserializer.UnsupportedRowJsonException;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RowJsonDeserializer}.
 */
public class RowJsonDeserializerTest {
  private static final boolean NOT_NULLABLE = false;
  private static final boolean NULLABLE = true;

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

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testParsesFlatRow() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addByteField("f_byte", NOT_NULLABLE)
            .addInt16Field("f_int16", NOT_NULLABLE)
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addInt64Field("f_int64", NOT_NULLABLE)
            .addFloatField("f_float", NOT_NULLABLE)
            .addDoubleField("f_double", NOT_NULLABLE)
            .addBooleanField("f_boolean", NOT_NULLABLE)
            .addStringField("f_string", NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_byte\" : 12,\n"
                       + "\"f_int16\" : 22,\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_int64\" : 42,\n"
                       + "\"f_float\" : 1.02E5,\n"
                       + "\"f_double\" : 62.2,\n"
                       + "\"f_boolean\" : true,\n"
                       + "\"f_string\" : \"hello\"\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues((byte) 12, (short) 22, 32, (long) 42, 1.02E5f, 62.2d, true, "hello")
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesArrayField() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addArrayField("f_intArray", INT32.type())
            .build();

    String rowString = "{\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_intArray\" : [ 1, 2, 3, 4, 5]\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(32, Arrays.asList(1, 2, 3, 4, 5))
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesArrayOfArrays() throws Exception {

    Schema schema =
        Schema
            .builder()
            .addArrayField("f_arrayOfIntArrays",
                           FieldType.of(ARRAY).withComponentType(INT32.type()))
            .build();

    String rowString = "{\n"
                       + "\"f_arrayOfIntArrays\" : [ [1, 2], [3, 4], [5]]\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addArray(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5))
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testThrowsForMismatchedArrayField() throws Exception {

    Schema schema =
        Schema
            .builder()
            .addArrayField("f_arrayOfIntArrays",
                           FieldType.of(ARRAY).withComponentType(INT32.type()))
            .build();

    String rowString = "{\n"
                       + "\"f_arrayOfIntArrays\" : { }\n" // expect array, get object
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("Expected JSON array");

    newObjectMapperWith(deserializer).readValue(rowString, Row.class);
  }

  @Test
  public void testParsesRowField() throws Exception {
    Schema nestedRowSchema =
        Schema
            .builder()
            .addInt32Field("f_nestedInt32", NOT_NULLABLE)
            .addStringField("f_nestedString", NOT_NULLABLE)
            .build();

    Schema schema =
        Schema
            .builder()
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addRowField("f_row", nestedRowSchema, NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_row\" : {\n"
                       + "             \"f_nestedInt32\" : 54,\n"
                       + "             \"f_nestedString\" : \"foo\"\n"
                       + "            }\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(32, Row.withSchema(nestedRowSchema).addValues(54, "foo").build())
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testThrowsForMismatchedRowField() throws Exception {
    Schema nestedRowSchema =
        Schema
            .builder()
            .addInt32Field("f_nestedInt32", NOT_NULLABLE)
            .addStringField("f_nestedString", NOT_NULLABLE)
            .build();

    Schema schema =
        Schema
            .builder()
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addRowField("f_row", nestedRowSchema, NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_row\" : []\n" // expect object, get array
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("Expected JSON object");

    newObjectMapperWith(deserializer).readValue(rowString, Row.class);
  }

  @Test
  public void testParsesNestedRowField() throws Exception {

    Schema doubleNestedRowSchema =
        Schema
            .builder()
            .addStringField("f_doubleNestedString", NOT_NULLABLE)
            .build();

    Schema nestedRowSchema =
        Schema
            .builder()
            .addRowField("f_nestedRow", doubleNestedRowSchema, NOT_NULLABLE)
            .build();

    Schema schema =
        Schema
            .builder()
            .addRowField("f_row", nestedRowSchema, NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_row\" : {\n"
                       + "             \"f_nestedRow\" : {\n"
                       + "                                \"f_doubleNestedString\":\"foo\"\n"
                       + "                               }\n"
                       + "            }\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(
                Row
                    .withSchema(nestedRowSchema)
                    .addValues(
                        Row
                            .withSchema(doubleNestedRowSchema)
                            .addValues("foo")
                            .build())
                    .build())
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testThrowsForUnsupportedType() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addDateTimeField("f_dateTime", NOT_NULLABLE)
            .build();

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("DATETIME is not supported");

    RowJsonDeserializer.forSchema(schema);
  }

  @Test
  public void testThrowsForUnsupportedArrayElementType() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addArrayField("f_dateTimeArray", DATETIME.type())
            .build();

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("DATETIME is not supported");

    RowJsonDeserializer.forSchema(schema);
  }

  @Test
  public void testThrowsForUnsupportedNestedFieldType() throws Exception {
    Schema nestedSchema =
        Schema
            .builder()
            .addArrayField("f_dateTimeArray", DATETIME.type())
            .build();

    Schema schema =
        Schema
            .builder()
            .addRowField("f_nestedRow", nestedSchema, NOT_NULLABLE)
            .build();

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("DATETIME is not supported");

    RowJsonDeserializer.forSchema(schema);
  }

  @Test
  public void testParsesNulls() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addByteField("f_byte", NOT_NULLABLE)
            .addStringField("f_string", NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_byte\" : 12,\n"
                       + "\"f_string\" : null\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    Row parsedRow = newObjectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues((byte) 12, null)
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testThrowsForMissingNotNullableField() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addByteField("f_byte", NOT_NULLABLE)
            .addStringField("f_string", NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_byte\" : 12\n"
                       + "}";

    RowJsonDeserializer deserializer = RowJsonDeserializer.forSchema(schema);

    thrown.expect(UnsupportedRowJsonException.class);
    thrown.expectMessage("'f_string' is not present");

    newObjectMapperWith(deserializer).readValue(rowString, Row.class);
  }

  @Test
  public void testSupportedBooleanConversions() throws Exception {
    testSupportedConversion(BOOLEAN, BOOLEAN_TRUE_STRING, BOOLEAN_TRUE_VALUE);
  }

  @Test
  public void testSupportedStringConversions() throws Exception {
    testSupportedConversion(STRING, quoted(FLOAT_STRING), FLOAT_STRING);
  }

  @Test
  public void testSupportedByteConversions() throws Exception {
    testSupportedConversion(BYTE, BYTE_STRING, BYTE_VALUE);
  }

  @Test
  public void testSupportedShortConversions() throws Exception {
    testSupportedConversion(INT16, BYTE_STRING, (short) BYTE_VALUE);
    testSupportedConversion(INT16, SHORT_STRING, SHORT_VALUE);
  }

  @Test
  public void testSupportedIntConversions() throws Exception {
    testSupportedConversion(INT32, BYTE_STRING, (int) BYTE_VALUE);
    testSupportedConversion(INT32, SHORT_STRING, (int) SHORT_VALUE);
    testSupportedConversion(INT32, INT_STRING, INT_VALUE);
  }

  @Test
  public void testSupportedLongConversions() throws Exception {
    testSupportedConversion(INT64, BYTE_STRING, (long) BYTE_VALUE);
    testSupportedConversion(INT64, SHORT_STRING, (long) SHORT_VALUE);
    testSupportedConversion(INT64, INT_STRING, (long) INT_VALUE);
    testSupportedConversion(INT64, LONG_STRING, LONG_VALUE);
  }

  @Test
  public void testSupportedFloatConversions() throws Exception {
    testSupportedConversion(FLOAT, FLOAT_STRING, FLOAT_VALUE);
    testSupportedConversion(FLOAT, SHORT_STRING, (float) SHORT_VALUE);
  }

  @Test
  public void testSupportedDoubleConversions() throws Exception {
    testSupportedConversion(DOUBLE, DOUBLE_STRING, DOUBLE_VALUE);
    testSupportedConversion(DOUBLE, FLOAT_STRING, (double) FLOAT_VALUE);
    testSupportedConversion(DOUBLE, INT_STRING, (double) INT_VALUE);
  }

  private void testSupportedConversion(
      TypeName fieldType,
      String jsonFieldValue,
      Object expectedRowFieldValue) throws Exception {

    String fieldName = "f_" + fieldType.name().toLowerCase();
    Schema schema = schemaWithField(fieldName, fieldType);
    Row expectedRow = Row.withSchema(schema).addValues(expectedRowFieldValue).build();
    ObjectMapper jsonParser = newObjectMapperWith(RowJsonDeserializer.forSchema(schema));

    Row parsedRow = jsonParser.readValue(jsonObjectWith(fieldName, jsonFieldValue), Row.class);

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testUnsupportedBooleanConversions() throws Exception {
    testUnsupportedConversion(BOOLEAN, quoted(BOOLEAN_TRUE_STRING));
    testUnsupportedConversion(BOOLEAN, BYTE_STRING);
    testUnsupportedConversion(BOOLEAN, SHORT_STRING);
    testUnsupportedConversion(BOOLEAN, INT_STRING);
    testUnsupportedConversion(BOOLEAN, LONG_STRING);
    testUnsupportedConversion(BOOLEAN, FLOAT_STRING);
    testUnsupportedConversion(BOOLEAN, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedStringConversions() throws Exception {
    testUnsupportedConversion(STRING, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(STRING, BYTE_STRING);
    testUnsupportedConversion(STRING, SHORT_STRING);
    testUnsupportedConversion(STRING, INT_STRING);
    testUnsupportedConversion(STRING, LONG_STRING);
    testUnsupportedConversion(STRING, FLOAT_STRING);
    testUnsupportedConversion(STRING, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedByteConversions() throws Exception {
    testUnsupportedConversion(BYTE, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(BYTE, quoted(BYTE_STRING));
    testUnsupportedConversion(BYTE, SHORT_STRING);
    testUnsupportedConversion(BYTE, INT_STRING);
    testUnsupportedConversion(BYTE, LONG_STRING);
    testUnsupportedConversion(BYTE, FLOAT_STRING);
    testUnsupportedConversion(BYTE, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedShortConversions() throws Exception {
    testUnsupportedConversion(INT16, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(INT16, quoted(SHORT_STRING));
    testUnsupportedConversion(INT16, INT_STRING);
    testUnsupportedConversion(INT16, LONG_STRING);
    testUnsupportedConversion(INT16, FLOAT_STRING);
    testUnsupportedConversion(INT16, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedIntConversions() throws Exception {
    testUnsupportedConversion(INT32, quoted(INT_STRING));
    testUnsupportedConversion(INT32, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(INT32, LONG_STRING);
    testUnsupportedConversion(INT32, FLOAT_STRING);
    testUnsupportedConversion(INT32, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedLongConversions() throws Exception {
    testUnsupportedConversion(INT64, quoted(LONG_STRING));
    testUnsupportedConversion(INT64, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(INT64, FLOAT_STRING);
    testUnsupportedConversion(INT64, DOUBLE_STRING);
  }

  @Test
  public void testUnsupportedFloatConversions() throws Exception {
    testUnsupportedConversion(FLOAT, quoted(FLOAT_STRING));
    testUnsupportedConversion(FLOAT, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(FLOAT, DOUBLE_STRING);
    testUnsupportedConversion(FLOAT, INT_STRING); // too large to fit
  }

  @Test
  public void testUnsupportedDoubleConversions() throws Exception {
    testUnsupportedConversion(DOUBLE, quoted(DOUBLE_STRING));
    testUnsupportedConversion(DOUBLE, BOOLEAN_TRUE_STRING);
    testUnsupportedConversion(DOUBLE, LONG_STRING); // too large to fit
  }

  private void testUnsupportedConversion(
      TypeName fieldType,
      String jsonFieldValue) throws Exception {

    String fieldName = "f_" + fieldType.name().toLowerCase();

    ObjectMapper jsonParser =
        newObjectMapperWith(RowJsonDeserializer
                                .forSchema(
                                    schemaWithField(fieldName, fieldType)));

    thrown.expectMessage(fieldName);
    thrown.expectCause(unsupportedWithMessage(jsonFieldValue, "out of range"));

    jsonParser.readValue(jsonObjectWith(fieldName, jsonFieldValue), Row.class);
  }

  private String quoted(String string) {
    return "\"" + string + "\"";
  }

  private Schema schemaWithField(String fieldName, TypeName fieldType) {
    return
        Schema
            .builder()
            .addField(Schema.Field.of(fieldName, fieldType.type()))
            .build();
  }

  private String jsonObjectWith(String fieldName, String fieldValue) {
    return
        "{\n"
        + "\"" + fieldName + "\" : " + fieldValue + "\n"
        + "}";
  }

  private Matcher<UnsupportedRowJsonException> unsupportedWithMessage(String ... message) {
    return allOf(
        Matchers.isA(UnsupportedRowJsonException.class),
        hasProperty("message",
                    stringContainsInOrder(
                        Arrays.asList(message))));
  }

  private ObjectMapper newObjectMapperWith(RowJsonDeserializer deserializer) {
    SimpleModule simpleModule = new SimpleModule("rowSerializationTesModule");
    simpleModule.addDeserializer(Row.class, deserializer);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(simpleModule);
    return objectMapper;
  }
}
