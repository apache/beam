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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestData.DATA;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
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
 * Tests {@link org.apache.commons.csv.CSVFormat} settings in the context of {@link
 * CsvRowConversions.RowToCsv}.
 */
@RunWith(JUnit4.class)
public class RowToCsvCSVFormatTest {
  @Test
  public void invalidCSVFormatHeader() {
    NullPointerException nullHeaderError =
        assertThrows(
            NullPointerException.class,
            () ->
                CsvRowConversions.RowToCsv.builder()
                    .setCSVFormat(CSVFormat.DEFAULT)
                    .setSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                    .build());
    assertEquals("CSVFormat withHeader is required", nullHeaderError.getMessage());

    IllegalArgumentException emptyHeaderError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvRowConversions.RowToCsv.builder()
                    .setCSVFormat(CSVFormat.DEFAULT.withHeader())
                    .setSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                    .build());

    assertEquals(
        "CSVFormat withHeader requires at least one column", emptyHeaderError.getMessage());

    IllegalArgumentException mismatchHeaderError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvRowConversions.RowToCsv.builder()
                    .setCSVFormat(
                        CSVFormat.DEFAULT.withHeader("aString", "idontexist1", "idontexist2"))
                    .setSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                    .build());

    assertEquals(
        "columns in CSVFormat header do not exist in Schema: idontexist2,idontexist1",
        mismatchHeaderError.getMessage());
  }

  @Test
  public void invalidSchema() {
    IllegalArgumentException emptySchemaError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvRowConversions.RowToCsv.builder()
                    .setCSVFormat(CSVFormat.DEFAULT.withHeader())
                    .setSchema(Schema.of())
                    .build());

    assertEquals("Schema has no fields", emptySchemaError.getMessage());

    IllegalArgumentException invalidArrayFieldsError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CsvRowConversions.RowToCsv.builder()
                    .setCSVFormat(CSVFormat.DEFAULT.withHeader("instant", "instantList"))
                    .setSchema(TIME_CONTAINING_SCHEMA)
                    .build());

    assertEquals(
        "columns in header match fields in Schema with invalid types: instantList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        invalidArrayFieldsError.getMessage());

    // Should not throw Exception when limited to valid fields.
    CsvRowConversions.RowToCsv.builder()
        .setCSVFormat(CSVFormat.DEFAULT.withHeader("instant"))
        .setSchema(TIME_CONTAINING_SCHEMA)
        .build();
  }

  @Test
  public void withAllowDuplicateHeaderNamesDuplicatesRowFieldOutput() {
    assertEquals(
        "allowDuplicateHeaderNames=true",
        "a,a,a",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withAllowDuplicateHeaderNames(true)
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
        "withAllowMissingColumnNames is an illegal CSVFormat setting", exception.getMessage());
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

    assertEquals("withAutoFlush is an illegal CSVFormat setting", exception.getMessage());
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
        "false~10~1.0~1.0~1~a~1",
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
        "false,10,1.0,1.0,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withHeaderComments("some", "header", "comments")));
  }

  @Test
  public void withIgnoreEmptyLinesDoesNotEffectOutput() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withIgnoreEmptyLines(true)));
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
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
  public void withNullStringReplacesNullValues() {
    assertEquals(
        "🦄,🦄,🦄,🦄,🦄,🦄",
        rowToCsv(
            DATA.nullableTypesRowAllNull,
            csvFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withNullString("🦄")));
  }

  @Test
  public void withQuoteDrivesConversion() {
    assertEquals(
        "@false@,10,1.0,1.0,1,@a@,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withQuote('@')
                .withQuoteMode(QuoteMode.NON_NUMERIC)));
  }

  @Test
  public void withQuoteModeDrivesCellBoundaries() {
    assertEquals(
        "\"false\",\"10\",\"1.0\",\"1.0\",\"1\",\"a\",\"1\"",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withQuoteMode(QuoteMode.ALL)));
    assertEquals(
        "\"false\",10,1.0,1.0,1,\"a\",1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withQuoteMode(QuoteMode.NON_NUMERIC)));
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
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
        "false,10,1.0,1.0,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withEscape('#')
                .withQuoteMode(QuoteMode.NONE)));
  }

  @Test
  public void withSystemRecordSeparatorDoesNotEffectOutput() {
    assertEquals(
        rowToCsv(DATA.allPrimitiveDataTypesRow, csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)),
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withSystemRecordSeparator()));
  }

  @Test
  public void withTrailingDelimiterAppendsToLineEnd() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1,",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withTrailingDelimiter(true)));
  }

  @Test
  public void withTrimRemovesCellPadding() {
    assertEquals(
        "false,10,1.0,1.0,1,\"       a           \",1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRowWithPadding, csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)));
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        rowToCsv(
            DATA.allPrimitiveDataTypesRowWithPadding,
            csvFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).withTrim(true)));
  }

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
