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
package org.apache.beam.sdk.io.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvIOParseHelpers}. */
@RunWith(JUnit4.class)
public class CsvIOParseHelpersTest {

  /** Tests for {@link CsvIOParseHelpers#validateCsvFormat(CSVFormat)}. */
  @Test
  public void givenCSVFormatWithHeader_validates() {
    CSVFormat format = csvFormatWithHeader();
    CsvIOParseHelpers.validateCsvFormat(format);
  }

  @Test
  public void givenCSVFormatWithNullHeader_throwsException() {
    CSVFormat format = csvFormat();
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals("Illegal class org.apache.commons.csv.CSVFormat: header is required", gotMessage);
  }

  @Test
  public void givenCSVFormatWithEmptyHeader_throwsException() {
    CSVFormat format = csvFormat().withHeader();
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: header cannot be empty", gotMessage);
  }

  @Test
  public void givenCSVFormatWithHeaderContainingEmptyString_throwsException() {
    CSVFormat format = csvFormat().withHeader("", "bar");
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: column name is required", gotMessage);
  }

  @Test
  public void givenCSVFormatWithHeaderContainingNull_throwsException() {
    CSVFormat format = csvFormat().withHeader(null, "bar");
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: column name is required", gotMessage);
  }

  @Test
  public void givenCSVFormatThatAllowsMissingColumnNames_throwsException() {
    CSVFormat format = csvFormatWithHeader().withAllowMissingColumnNames(true);
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: cannot allow missing column names",
        gotMessage);
  }

  @Test
  public void givenCSVFormatThatIgnoresHeaderCase_throwsException() {
    CSVFormat format = csvFormatWithHeader().withIgnoreHeaderCase(true);
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: cannot ignore header case", gotMessage);
  }

  @Test
  public void givenCSVFormatThatAllowsDuplicateHeaderNames_throwsException() {
    CSVFormat format = csvFormatWithHeader().withAllowDuplicateHeaderNames(true);
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: cannot allow duplicate header names",
        gotMessage);
  }

  @Test
  public void givenCSVFormatThatSkipsHeaderRecord_throwsException() {
    CSVFormat format = csvFormatWithHeader().withSkipHeaderRecord(true);
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class, () -> CsvIOParseHelpers.validateCsvFormat(format))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: cannot skip header record because the header is already accounted for",
        gotMessage);
  }

  /** End of tests for {@link CsvIOParseHelpers#validateCsvFormat(CSVFormat)}. */
  //////////////////////////////////////////////////////////////////////////////////////////////

  /** Tests for {@link CsvIOParseHelpers#validateCsvFormatWithSchema(CSVFormat, Schema)}. */
  @Test
  public void givenNullableSchemaFieldNotPresentInHeader_validates() {
    CSVFormat format = csvFormat().withHeader("foo", "bar");
    Schema schema =
        Schema.of(
            Schema.Field.of("foo", Schema.FieldType.STRING),
            Schema.Field.of("bar", Schema.FieldType.STRING),
            Schema.Field.nullable("baz", Schema.FieldType.STRING));
    CsvIOParseHelpers.validateCsvFormatWithSchema(format, schema);
  }

  @Test
  public void givenRequiredSchemaFieldNotPresentInHeader_throwsException() {
    CSVFormat format = csvFormat().withHeader("foo", "bar");
    Schema schema =
        Schema.of(
            Schema.Field.of("foo", Schema.FieldType.STRING),
            Schema.Field.of("bar", Schema.FieldType.STRING),
            Schema.Field.of("baz", Schema.FieldType.STRING));
    String gotMessage =
        assertThrows(
                IllegalArgumentException.class,
                () -> CsvIOParseHelpers.validateCsvFormatWithSchema(format, schema))
            .getMessage();
    assertEquals(
        "Illegal class org.apache.commons.csv.CSVFormat: required org.apache.beam.sdk.schemas.Schema field 'baz' not found in header",
        gotMessage);
  }

  /** End of tests for {@link CsvIOParseHelpers#validateCsvFormatWithSchema(CSVFormat, Schema)}. */
  //////////////////////////////////////////////////////////////////////////////////////////////
  /** Tests for {@link CsvIOParseHelpers#mapFieldPositions(CSVFormat, Schema)}. */
  @Test
  public void testHeaderWithComments() {
    String[] comments = {"first line", "second line", "third line"};
    Schema schema =
        Schema.builder().addStringField("a_string").addStringField("another_string").build();
    ImmutableMap<Integer, Schema.Field> want =
        ImmutableMap.of(0, schema.getField("a_string"), 1, schema.getField("another_string"));
    Map<Integer, Schema.Field> got =
        CsvIOParseHelpers.mapFieldPositions(
            csvFormat()
                .withHeader("a_string", "another_string")
                .withHeaderComments((Object) comments),
            schema);
    assertEquals(want, got);
  }

  @Test
  public void givenMatchingHeaderAndSchemaField_mapsPositions() {
    Schema schema =
        Schema.builder()
            .addStringField("a_string")
            .addDoubleField("a_double")
            .addInt32Field("an_integer")
            .build();
    ImmutableMap<Integer, Schema.Field> want =
        ImmutableMap.of(
            0,
            schema.getField("a_string"),
            1,
            schema.getField("an_integer"),
            2,
            schema.getField("a_double"));
    Map<Integer, Schema.Field> got =
        CsvIOParseHelpers.mapFieldPositions(
            csvFormat().withHeader("a_string", "an_integer", "a_double"), schema);
    assertEquals(want, got);
  }

  @Test
  public void givenSchemaContainsNullableFieldTypes() {
    Schema schema =
        Schema.builder()
            .addNullableStringField("a_string")
            .addDoubleField("a_double")
            .addInt32Field("an_integer")
            .addDateTimeField("a_datetime")
            .addNullableStringField("another_string")
            .build();
    ImmutableMap<Integer, Schema.Field> want =
        ImmutableMap.of(
            0,
            schema.getField("an_integer"),
            1,
            schema.getField("a_double"),
            2,
            schema.getField("a_datetime"));
    Map<Integer, Schema.Field> got =
        CsvIOParseHelpers.mapFieldPositions(
            csvFormat().withHeader("an_integer", "a_double", "a_datetime"), schema);
    assertEquals(want, got);
  }

  @Test
  public void givenNonNullableHeaderAndSchemaFieldMismatch_throws() {
    Schema schema =
        Schema.builder()
            .addStringField("another_string")
            .addInt32Field("an_integer")
            .addStringField("a_string")
            .build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.mapFieldPositions(
                    csvFormat().withHeader("an_integer", "a_string"), schema));
    assertEquals(
        "header does not contain required class org.apache.beam.sdk.schemas.Schema field: "
            + schema.getField("another_string").getName(),
        e.getMessage());
  }

  /** End of tests for {@link CsvIOParseHelpers#mapFieldPositions(CSVFormat, Schema)} */

  ////////////////////////////////////////////////////////////////////////////////////////////

  /** Tests for {@link CsvIOParseHelpers#parseCell(String, Schema.Field)}. */
  @Test
  public void ignoresCaseFormat() {
    String allCapsBool = "TRUE";
    Schema schema = Schema.builder().addBooleanField("a_boolean").build();
    assertEquals(true, CsvIOParseHelpers.parseCell(allCapsBool, schema.getField("a_boolean")));
  }

  @Test
  public void givenIntegerWithSurroundingSpaces_throws() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   12   ", 12);
    Schema schema = Schema.builder().addInt32Field("an_integer").addStringField("a_string").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.parseCell(
                    cellToExpectedValue.getKey().toString(), schema.getField("an_integer")));
    assertEquals(
        "For input string: \""
            + cellToExpectedValue.getKey()
            + "\" field "
            + schema.getField("an_integer").getName()
            + " was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenDoubleWithSurroundingSpaces_parses() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   20.04   ", 20.04);
    Schema schema = Schema.builder().addDoubleField("a_double").addInt32Field("an_integer").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_double")));
  }

  @Test
  public void givenStringWithSurroundingSpaces_parsesIncorrectly() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   a  ", "a");
    Schema schema = Schema.builder().addStringField("a_string").addInt64Field("a_long").build();
    assertEquals(
        cellToExpectedValue.getKey(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_string")));
  }

  @Test
  public void givenBigDecimalWithSurroundingSpaces_throws() {
    BigDecimal decimal = new BigDecimal("123.456");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   123.456  ", decimal);
    Schema schema =
        Schema.builder().addDecimalField("a_decimal").addStringField("a_string").build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CsvIOParseHelpers.parseCell(
                cellToExpectedValue.getKey().toString(), schema.getField("a_decimal")));
  }

  @Test
  public void givenShortWithSurroundingSpaces_throws() {
    Short shortNum = Short.parseShort("12");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   12   ", shortNum);
    Schema schema =
        Schema.builder()
            .addInt16Field("a_short")
            .addInt32Field("an_integer")
            .addInt64Field("a_long")
            .build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.parseCell(
                    cellToExpectedValue.getKey().toString(), schema.getField("a_short")));
    assertEquals(
        "For input string: \""
            + cellToExpectedValue.getKey()
            + "\" field "
            + schema.getField("a_short").getName()
            + " was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenLongWithSurroundingSpaces_throws() {
    Long longNum = Long.parseLong("3400000000");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   12   ", longNum);
    Schema schema =
        Schema.builder()
            .addInt16Field("a_short")
            .addInt32Field("an_integer")
            .addInt64Field("a_long")
            .build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.parseCell(
                    cellToExpectedValue.getKey().toString(), schema.getField("a_long")));
    assertEquals(
        "For input string: \""
            + cellToExpectedValue.getKey()
            + "\" field "
            + schema.getField("a_long").getName()
            + " was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenFloatWithSurroundingSpaces_parses() {
    Float floatNum = Float.parseFloat("3.141592");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   3.141592   ", floatNum);
    Schema schema =
        Schema.builder()
            .addFloatField("a_float")
            .addInt32Field("an_integer")
            .addStringField("a_string")
            .build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_float")));
  }

  @Test
  public void givenDatetimeWithSurroundingSpaces() {
    Instant datetime = Instant.parse("1234-01-23T10:00:05.000Z");
    DefaultMapEntry cellToExpectedValue =
        new DefaultMapEntry("   1234-01-23T10:00:05.000Z   ", datetime);
    Schema schema =
        Schema.builder().addDateTimeField("a_datetime").addStringField("a_string").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.parseCell(
                    cellToExpectedValue.getKey().toString(), schema.getField("a_datetime")));
    assertEquals(
        "Invalid format: \"   1234-01-23T10:00:05.000Z   \" field a_datetime was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenByteWithSurroundingSpaces_throws() {
    Byte byteNum = Byte.parseByte("40");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   40   ", byteNum);
    Schema schema = Schema.builder().addByteField("a_byte").addInt32Field("an_integer").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvIOParseHelpers.parseCell(
                    cellToExpectedValue.getKey().toString(), schema.getField("a_byte")));
    assertEquals(
        "For input string: \""
            + cellToExpectedValue.getKey()
            + "\" field "
            + schema.getField("a_byte").getName()
            + " was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenBooleanWithSurroundingSpaces_returnsInverse() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("   true    ", true);
    Schema schema =
        Schema.builder()
            .addBooleanField("a_boolean")
            .addInt32Field("an_integer")
            .addStringField("a_string")
            .build();
    assertEquals(
        false,
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_boolean")));
  }

  @Test
  public void givenMultiLineCell_parses() {
    String multiLineString = "a\na\na\na\na\na\na\na\na\nand";
    Schema schema = Schema.builder().addStringField("a_string").addDoubleField("a_double").build();
    assertEquals(
        multiLineString, CsvIOParseHelpers.parseCell(multiLineString, schema.getField("a_string")));
  }

  @Test
  public void givenValidIntegerCell_parses() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("12", 12);
    Schema schema = Schema.builder().addInt32Field("an_integer").addInt64Field("a_long").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("an_integer")));
  }

  @Test
  public void givenValidDoubleCell_parses() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("10.05", 10.05);
    Schema schema = Schema.builder().addDoubleField("a_double").addStringField("a_string").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_double")));
  }

  @Test
  public void givenValidStringCell_parses() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("lithium", "lithium");
    Schema schema =
        Schema.builder().addStringField("a_string").addDateTimeField("a_datetime").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_string")));
  }

  @Test
  public void givenValidDecimalCell_parses() {
    BigDecimal decimal = new BigDecimal("127.99");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("127.99", decimal);
    Schema schema =
        Schema.builder().addDecimalField("a_decimal").addDoubleField("a_double").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_decimal")));
  }

  @Test
  public void givenValidShortCell_parses() {
    Short shortNum = Short.parseShort("36");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("36", shortNum);
    Schema schema =
        Schema.builder()
            .addInt32Field("an_integer")
            .addInt64Field("a_long")
            .addInt16Field("a_short")
            .build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_short")));
  }

  @Test
  public void givenValidLongCell_parses() {
    Long longNum = Long.parseLong("1234567890");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("1234567890", longNum);
    Schema schema =
        Schema.builder()
            .addInt32Field("an_integer")
            .addInt64Field("a_long")
            .addInt16Field("a_short")
            .build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_long")));
  }

  @Test
  public void givenValidFloatCell_parses() {
    Float floatNum = Float.parseFloat("3.141592");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("3.141592", floatNum);
    Schema schema = Schema.builder().addFloatField("a_float").addDoubleField("a_double").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_float")));
  }

  @Test
  public void givenValidDateTimeCell_parses() {
    Instant datetime = Instant.parse("2020-01-01T00:00:00.000Z");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("2020-01-01T00:00:00.000Z", datetime);
    Schema schema =
        Schema.builder().addDateTimeField("a_datetime").addStringField("a_string").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_datetime")));
  }

  @Test
  public void givenValidByteCell_parses() {
    Byte byteNum = Byte.parseByte("4");
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("4", byteNum);
    Schema schema = Schema.builder().addByteField("a_byte").addInt32Field("an_integer").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_byte")));
  }

  @Test
  public void givenValidBooleanCell_parses() {
    DefaultMapEntry cellToExpectedValue = new DefaultMapEntry("false", false);
    Schema schema =
        Schema.builder().addBooleanField("a_boolean").addStringField("a_string").build();
    assertEquals(
        cellToExpectedValue.getValue(),
        CsvIOParseHelpers.parseCell(
            cellToExpectedValue.getKey().toString(), schema.getField("a_boolean")));
  }

  @Test
  public void givenCellSchemaFieldMismatch_throws() {
    String boolTrue = "true";
    Schema schema = Schema.builder().addBooleanField("a_boolean").addFloatField("a_float").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> CsvIOParseHelpers.parseCell(boolTrue, schema.getField("a_float")));
    assertEquals(
        "For input string: \"" + boolTrue + "\" field a_float was received -- type mismatch",
        e.getMessage());
  }

  @Test
  public void givenCellUnsupportedType_throws() {
    String counting = "[one,two,three]";
    Schema schema =
        Schema.builder()
            .addField("an_array", Schema.FieldType.array(Schema.FieldType.STRING))
            .addStringField("a_string")
            .build();
    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> CsvIOParseHelpers.parseCell(counting, schema.getField("an_array")));
    assertEquals(
        "Unsupported type: "
            + schema.getField("an_array").getType()
            + ", consider using withCustomRecordParsing",
        e.getMessage());
  }

  /** End of tests for {@link CsvIOParseHelpers#parseCell(String, Schema.Field)}. */
  //////////////////////////////////////////////////////////////////////////////////////////////

  /** Return a {@link CSVFormat} with a header and with no duplicate header names allowed. */
  private static CSVFormat csvFormatWithHeader() {
    return csvFormat().withHeader("foo", "bar");
  }

  /** Return a {@link CSVFormat} with no header and with no duplicate header names allowed. */
  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT.withAllowDuplicateHeaderNames(false);
  }
}
