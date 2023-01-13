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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
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
    assertThrows(
        "allowDuplicateHeaderNames=false",
        IllegalArgumentException.class,
        () ->
            rowToCsv(
                DATA.allPrimitiveDataTypesRow,
                CSVFormat.DEFAULT
                    .withHeader("aString", "aString", "aString")
                    .withAllowDuplicateHeaderNames(false)));

    assertEquals(
        "allowDuplicateHeaderNames=true",
        "a,a,a",
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            CSVFormat.DEFAULT.withHeader("aString", "aString", "aString")));
  }

  @Test
  public void withAllowMissingColumnNamesSettingThrowsException() {
    IllegalArgumentException exception = assertThrows(
            "allowMissingColumnNames=true",
            IllegalArgumentException.class,
            () ->
        rowToCsv(
            DATA.allPrimitiveDataTypesRow,
            CSVFormat.DEFAULT.withAllowMissingColumnNames(true)));

    assertEquals("withAllowMissingColumnNames is a forbidden CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withAutoFlushThrowsException() {
    IllegalArgumentException exception = assertThrows(
            "autoFlush=true",
            IllegalArgumentException.class,
            () ->
            {
              rowToCsv(
                  DATA.allPrimitiveDataTypesRow,
                  CSVFormat.DEFAULT.withAutoFlush(true));
            });

    assertEquals("withAutoFlush is a forbidden CSVFormat setting", exception.getMessage());
  }

  @Test
  public void withCommentMarker() {

  }

  @Test
  public void withDelimiter() {}

  @Test
  public void withEscape() {}

  @Test
  public void withHeader() {}

  @Test
  public void withHeaderComments() {}

  @Test
  public void withIgnoreEmptyLines() {}

  @Test
  public void withIgnoreHeaderCase() {}

  @Test
  public void withIgnoreSurroundingSpaces() {}

  @Test
  public void withNullString() {}

  @Test
  public void withQuote() {}

  @Test
  public void withQuoteMode() {}

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
}
