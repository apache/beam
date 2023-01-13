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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.csv.CsvIOTestData.DATA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.nullableAllPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.nullableAllPrimitiveDataTypesToRowFn;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link org.apache.commons.csv.CSVFormat} parameters in the context of {@link
 * CsvRowConversions}.
 */
@RunWith(JUnit4.class)
public class CSVFormatRowConversionsTest {
  @Test
  public void testDefaultCSVFormat() {
  }

  @Test
  public void testDuplicateHeaderNames() {

  }

  @Test
  public void testMissingColumnNames() {}

  @Test
  public void testAutoFlush() {}

  @Test
  public void testCommentMarker() {}

  @Test
  public void testDelimiterString() {}

  @Test
  public void testEscapeCharacter() {}

  @Test
  public void testHeader() {}

  @Test
  public void testHeaderComments() {}

  @Test
  public void testEmptyLines() {}

  @Test
  public void testHeaderCase() {}

  @Test
  public void testSurroundingSpaces() {}

  @Test
  public void testNullString() {}

  @Test
  public void testQuoteCharacter() {}

  @Test
  public void testQuoteMode() {}

  @Test
  public void testRecordSeparator() {}

  @Test
  public void testSkipHeaderRecord() {}

  @Test
  public void testTrailingDelimiter() {}

  @Test
  public void testTrim() {}

  private static SerializableFunction<Row, String> rowToCsvFn(Schema schema) {
    CSVFormat format =
        CSVFormat.DEFAULT.withHeader(schema.sorted().getFieldNames().toArray(new String[0]));
    return CsvRowConversions.RowToCsv.builder().setCSVFormat(format).setSchema(schema).build();
  }

  private static String rowToCsv(Row row) {
    Schema schema = checkNotNull(row.getSchema());
    return rowToCsvFn(schema).apply(row);
  }
}
