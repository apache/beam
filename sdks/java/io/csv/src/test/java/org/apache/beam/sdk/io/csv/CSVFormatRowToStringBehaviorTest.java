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

import static org.apache.beam.sdk.io.csv.CsvIOTestData.DATA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link org.apache.commons.csv.CSVFormat} parameters in the context of {@link
 * org.apache.beam.sdk.io.csv.CsvRowConversions.RowToCsv}.
 */
@RunWith(JUnit4.class)
public class CSVFormatRowToStringBehaviorTest {

  @Test
  public void withAllowDuplicateHeaderNamesDuplicatesRowFieldOutput() {
    IllegalArgumentException exception =
        assertThrows(
            "allowDuplicateHeaderNames=false",
            IllegalArgumentException.class,
            () ->
                rowToCsv(
                    DATA.allPrimitiveDataTypesRow,
                    csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                        .withHeader("aString", "aString", "aString")
                        .withAllowDuplicateHeaderNames(false)));

    assertEquals(
        "CSVFormat header contains duplicates in the setting of allowDuplicateHeaderNames=false",
        exception.getMessage());

    assertEquals(
        "allowDuplicateHeaderNames=true",
        "a,a,a",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withHeader("aString", "aString", "aString")));
  }

  @Test
  public void withAllowMissingColumnNamesSettingThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            "allowMissingColumnNames=true",
            IllegalArgumentException.class,
            () ->
                rowToCsv(
                    DATA.allPrimitiveDataTypesRow,
                    csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withAllowMissingColumnNames(true)));

    assertEquals(
        "withAllowMissingColumnNames is a illegal CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withAutoFlushThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            "autoFlush=true",
            IllegalArgumentException.class,
            () ->
                rowToCsv(
                    DATA.allPrimitiveDataTypesRow,
                    csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withAutoFlush(true)));

    assertEquals("withAutoFlush is a illegal CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withCommentMarkerDoesNotEffectConversion() {
    Schema schema = Schema.of(Field.of("aString", FieldType.STRING));
    Row row = Row.withSchema(schema).attachValues("$abc");
    assertEquals("$abc", rowToCsv(row, csvFormat(schema).withCommentMarker('$')));
    assertEquals("$abc", rowToCsv(row, csvFormat(schema).withCommentMarker(null)));
  }

  @Test
  public void withDelimiterDrivesCellBorders() {
    assertEquals(
        "false~1~10~1.0~1.0~1~1~a~1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withDelimiter('~')));
  }

  @Test
  public void withEscapeDrivesOutput() {
    Schema schema =
        Schema.of(Field.of("aString", FieldType.STRING), Field.of("anInt", FieldType.INT32));
    Row row = Row.withSchema(schema).attachValues(",a", 1);
    String[] header = new String[] {"anInt", "aString"};
    assertEquals(
        "1,#,a",
        rowToCsv(
            row,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withHeader(header)
                .withEscape('#')
                .withQuoteMode(QuoteMode.NONE)));
    assertEquals(
        "1,\",a\"", rowToCsv(row, csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withHeader(header)));
  }

  @Test
  public void withHeaderDrivesFieldOrderSubsetOutput() {
    assertEquals(
        "1,false,a",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withHeader("anInteger", "aBoolean", "aString")));
  }

  @Test
  public void withHeaderCommentsDoesNotEffectConversion() {
    assertEquals(
        "false,1,10,1.0,1.0,1,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withHeaderComments("some", "header", "comments")));
  }

  @Test
  public void withIgnoreEmptyLinesDoesNotEffectOutput() {
    assertEquals(
        "false,1,10,1.0,1.0,1,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withIgnoreEmptyLines(true)));
    assertEquals(
        "false,1,10,1.0,1.0,1,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withIgnoreEmptyLines(false)));
  }

  @Test
  public void withIgnoreHeaderCaseThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                rowToCsv(
                    DATA.allPrimitiveDataTypesRow,
                    csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withIgnoreHeaderCase(true)));
    assertEquals("withIgnoreHeaderCase is an illegal CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withIgnoreSurroundingSpacesThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                rowToCsv(
                    DATA.allPrimitiveDataTypesRow,
                    csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withIgnoreSurroundingSpaces(true)));
    assertEquals(
        "withIgnoreSurroundingSpaces is an illegal CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withNullString() {
    assertEquals(
        "🦄,🦄,🦄,🦄,🦄,🦄",
        rowToCsv(
            DATA.nullableTypesRowAllNull,
            csvFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withNullString("🦄")));
  }

  @Test
  public void withQuoteDrivesConversion() {
    assertEquals(
        "@false@,1,10,1.0,1.0,1,1,@a@,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withQuote('@')
                .withQuoteMode(QuoteMode.NON_NUMERIC)));
  }

  @Test
  public void withQuoteModeDrivesCellBoundaries() {
    assertEquals(
        "\"false\",\"1\",\"10\",\"1.0\",\"1.0\",\"1\",\"1\",\"a\",\"1\"",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withQuoteMode(QuoteMode.ALL)));
    assertEquals(
        "\"false\",1,10,1.0,1.0,1,1,\"a\",1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withQuoteMode(QuoteMode.NON_NUMERIC)));
    assertEquals(
        "false,1,10,1.0,1.0,1,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withQuoteMode(QuoteMode.MINIMAL)));
    assertEquals(
        ",,,,,",
        rowToCsv(
            DATA.nullableTypesRowAllNull,
            csvFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withQuoteMode(QuoteMode.ALL_NON_NULL)));
    assertEquals(
        "\"true\",,,,\"a\",\"1\"",
        rowToCsv(
            DATA.nullableTypesRowSomeNull,
            csvFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withQuoteMode(QuoteMode.ALL_NON_NULL)));
    assertEquals(
        "false,1,10,1.0,1.0,1,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withEscape('#')
                .withQuoteMode(QuoteMode.NONE)));
  }

  @Test
  public void withRecordSeparator() {}

  @Test
  public void withSkipHeaderRecord() {}

  @Test
  public void withSystemRecordSeparator() {}

  @Test
  public void withTrailingDelimiter() {}

  @Test
  public void withTrim() {}

  private static SerializableFunction<Row, String> rowToCsvFn(Schema schema, CSVFormat csvFormat) {
    return CsvRowConversions.RowToCsv.builder().setCSVFormat(csvFormat).setSchema(schema).build();
  }

  private static String rowToCsv(Row row, CSVFormat csvFormat) {
    Schema schema = checkNotNull(row.getSchema());
    return rowToCsvFn(schema, csvFormat).apply(row);
  }

  private static CSVFormat csvFormat(Schema schema) {
    return CSVFormat.DEFAULT.withHeader(schema.sorted().getFieldNames().toArray(new String[0]));
  }
}
