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

import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.ALL_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.rowOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvUtils}. */
@RunWith(JUnit4.class)
public class CsvUtilsTest {

  private final Row row =
      rowOf(
          true,
          (byte) 0,
          Instant.ofEpochMilli(1670358365856L).toDateTime(),
          BigDecimal.valueOf(1L),
          3.12345,
          4.1f,
          (short) 5,
          2,
          7L,
          "asdfjkl;");

  @Test
  public void getCsvBytesToRowFunction() {
    // TODO(https://github.com/apache/beam/issues/24552)
    assertThrows(
        UnsupportedOperationException.class,
        () -> CsvUtils.getCsvBytesToRowFunction(row.getSchema(), null, null).apply(new byte[] {}));
  }

  @Test
  public void getCsvStringToRowFunction() {
    // TODO(https://github.com/apache/beam/issues/24552)
    assertThrows(
        UnsupportedOperationException.class,
        () -> CsvUtils.getCsvStringToRowFunction(row.getSchema(), null, null).apply(""));
  }

  @Test
  public void getRowToCsvBytesFunction() {
    assertArrayEquals(
        "true,0,3.12345,4.1,7,5,2,2022-12-06T20:26:05.856Z,1,asdfjkl;"
            .getBytes(StandardCharsets.UTF_8),
        CsvUtils.getRowToCsvBytesFunction(row.getSchema(), null, null).apply(row));
  }

  @Test
  public void getRowToCsvStringFunction() {
    assertEquals(
        "true,0,3.12345,4.1,7,5,2,2022-12-06T20:26:05.856Z,1,asdfjkl;",
        CsvUtils.getRowToCsvStringFunction(row.getSchema(), null, null).apply(row));
  }

  @Test
  public void getRowToCsvStringFunctionNonDefaultCSVFormat() {
    assertEquals(
        "\"true\",\"0\",\"3.12345\",\"4.1\",\"7\",\"5\",\"2\",\"2022-12-06T20:26:05.856Z\",\"1\",\"asdfjkl;\"",
        CsvUtils.getRowToCsvStringFunction(row.getSchema(), CSVFormat.POSTGRESQL_CSV, null)
            .apply(row));
  }

  @Test
  public void buildHeaderFromDefaults() {
    assertEquals(
        "aBoolean,aByte,aDouble,aFloat,aLong,aShort,anInt,dateTime,decimal,string",
        CsvUtils.buildHeaderFrom(ALL_DATA_TYPES_SCHEMA.getFieldNames(), CSVFormat.DEFAULT));
  }

  @Test
  public void buildHeaderFromNonDefaultCSVFormat() {
    assertEquals(
        "\"aBoolean\",\"aByte\",\"aDouble\",\"aFloat\",\"aLong\",\"aShort\",\"anInt\",\"dateTime\",\"decimal\",\"string\"",
        CsvUtils.buildHeaderFrom(ALL_DATA_TYPES_SCHEMA.getFieldNames(), CSVFormat.POSTGRESQL_CSV));
  }

  @Test
  public void buildHeaderFromListWithDefaultCSVFormat() {
    assertEquals(
        "col1,col2,col3",
        CsvUtils.buildHeaderFrom(Arrays.asList("col1", "col2", "col3"), CSVFormat.DEFAULT));
  }

  @Test
  public void buildHeaderFromListWithNonDefaultCSVFormat() {
    assertEquals(
        "col1\tcol2\tcol3",
        CsvUtils.buildHeaderFrom(Arrays.asList("col1", "col2", "col3"), CSVFormat.TDF));
  }
}
