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
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.csv.CsvRowConversions.RowToCsv;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link org.apache.beam.sdk.io.csv.CsvRowConversions.RowToCsv} with {@link
 * org.apache.commons.csv.CSVFormat.Predefined} types.
 */
@RunWith(JUnit4.class)
public class RowToCsvPredefinedCSVFormatsTest {
  @Test
  public void defaultFormat() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        defaultFormat(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        ",,,,,",
        defaultFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true,,,,a,1",
        defaultFormat(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        defaultFormat(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void excel() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        excel(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        ",,,,,",
        excel(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true,,,,a,1",
        excel(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        excel(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void informixUnload() {
    assertEquals(
        "false|10|1.0|1.0|1|a|1",
        informixUnload(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        "|||||",
        informixUnload(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true||||a|1",
        informixUnload(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        informixUnload(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void informixUnloadCsv() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        informixUnloadCSV(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        ",,,,,",
        informixUnloadCSV(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true,,,,a,1",
        informixUnloadCSV(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        informixUnloadCSV(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void mySql() {
    assertEquals(
        "false\t10\t1.0\t1.0\t1\ta\t1",
        mySql(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        "\\N\t\\N\t\\N\t\\N\t\\N\t\\N",
        mySql(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true\t\\N\t\\N\t\\N\ta\t1",
        mySql(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        mySql(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void rfc4180() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        rfc4180(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        ",,,,,",
        rfc4180(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true,,,,a,1",
        rfc4180(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        rfc4180(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void oracle() {
    assertEquals(
        "false,10,1.0,1.0,1,a,1",
        oracle(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        "\\N,\\N,\\N,\\N,\\N,\\N",
        oracle(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true,\\N,\\N,\\N,a,1",
        oracle(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        oracle(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void postgresqlCSV() {
    assertEquals(
        "\"false\",\"10\",\"1.0\",\"1.0\",\"1\",\"a\",\"1\"",
        postgresqlCSV(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        ",,,,,",
        postgresqlCSV(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "\"true\",,,,\"a\",\"1\"",
        postgresqlCSV(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "\"1970-01-01T00:00:00.001Z\"",
        postgresqlCSV(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void postgresqlText() {
    assertEquals(
        "\"false\"\t\"10\"\t\"1.0\"\t\"1.0\"\t\"1\"\t\"a\"\t\"1\"",
        postgresqlText(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        "\\N\t\\N\t\\N\t\\N\t\\N\t\\N",
        postgresqlText(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "\"true\"\t\\N\t\\N\t\\N\t\"a\"\t\"1\"",
        postgresqlText(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "\"1970-01-01T00:00:00.001Z\"",
        postgresqlText(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  @Test
  public void tdf() {
    assertEquals(
        "false\t10\t1.0\t1.0\t1\ta\t1",
        tdf(ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.allPrimitiveDataTypesRow));

    assertEquals(
        "", tdf(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowAllNull));

    assertEquals(
        "true\t\t\t\ta\t1",
        tdf(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA).apply(DATA.nullableTypesRowSomeNull));

    assertEquals(
        "1970-01-01T00:00:00.001Z",
        tdf(TIME_CONTAINING_SCHEMA, "instant").apply(DATA.timeContainingRow));
  }

  private static RowToCsv defaultFormat(Schema schema, String... header) {
    return rowToCsv(CSVFormat.DEFAULT, schema, header);
  }

  private static RowToCsv excel(Schema schema, String... header) {
    return rowToCsv(CSVFormat.EXCEL.withAllowMissingColumnNames(false), schema, header);
  }

  private static RowToCsv informixUnload(Schema schema, String... header) {
    return rowToCsv(CSVFormat.INFORMIX_UNLOAD, schema, header);
  }

  private static RowToCsv informixUnloadCSV(Schema schema, String... header) {
    return rowToCsv(CSVFormat.INFORMIX_UNLOAD_CSV, schema, header);
  }

  private static RowToCsv mySql(Schema schema, String... header) {
    return rowToCsv(CSVFormat.MYSQL, schema, header);
  }

  private static RowToCsv rfc4180(Schema schema, String... header) {
    return rowToCsv(CSVFormat.RFC4180, schema, header);
  }

  private static RowToCsv oracle(Schema schema, String... header) {
    return rowToCsv(CSVFormat.ORACLE, schema, header);
  }

  private static RowToCsv postgresqlCSV(Schema schema, String... header) {
    return rowToCsv(CSVFormat.POSTGRESQL_CSV, schema, header);
  }

  private static RowToCsv postgresqlText(Schema schema, String... header) {
    return rowToCsv(CSVFormat.POSTGRESQL_TEXT, schema, header);
  }

  private static RowToCsv tdf(Schema schema, String... header) {
    return rowToCsv(CSVFormat.TDF.withIgnoreSurroundingSpaces(false), schema, header);
  }

  private static RowToCsv rowToCsv(CSVFormat csvFormat, Schema schema, String... header) {
    if (header.length == 0) {
      header = schema.sorted().getFieldNames().toArray(new String[0]);
    }
    return RowToCsv.builder().setCSVFormat(csvFormat.withHeader(header)).setSchema(schema).build();
  }
}
