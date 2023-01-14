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

import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvRowConversions} validations. */
@RunWith(JUnit4.class)
public class RowToCsvValidationsTest {
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
}
