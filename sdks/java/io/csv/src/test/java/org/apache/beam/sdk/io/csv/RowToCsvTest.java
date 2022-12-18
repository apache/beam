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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RowToCsv}. */
@RunWith(JUnit4.class)
public class RowToCsvTest {

  final Row row =
      rowOf(
          false,
          (byte) 1,
          Instant.ofEpochMilli(1670537248873L).toDateTime(),
          BigDecimal.valueOf(123456789L),
          1.23456789,
          1.23f,
          (short) 2,
          3,
          999999L,
          "abcdefg");

  @Test
  public void buildHeader() {
    assertEquals(
        "all schema field names should be included in header",
        "aBoolean,aByte,aDouble,aFloat,aLong,aShort,anInt,dateTime,decimal,string",
        defaultRowToCsv().build().buildHeader());

    assertEquals(
        "schema field names subset should be included in provided order",
        "aShort,string,aByte",
        defaultRowToCsv()
            .setSchemaFields(Arrays.asList("aShort", "string", "aByte"))
            .build()
            .buildHeader());

    assertEquals(
        "all schema fields header should convert using non default CSVFormat",
        "\"aBoolean\",\"aByte\",\"aDouble\",\"aFloat\",\"aLong\",\"aShort\",\"anInt\",\"dateTime\",\"decimal\",\"string\"",
        defaultRowToCsv().setCSVFormat(CSVFormat.POSTGRESQL_CSV).build().buildHeader());

    assertEquals(
        "schema field names subset should convert in provided order using non default CSVFormat",
        "\"aShort\",\"string\",\"aByte\"",
        defaultRowToCsv()
            .setSchemaFields(Arrays.asList("aShort", "string", "aByte"))
            .setCSVFormat(CSVFormat.POSTGRESQL_CSV)
            .build()
            .buildHeader());
  }

  @Test
  public void applyRowToCsvConversion() {
    assertEquals(
        "all schema fields and default CSVFormat should convert all Row values",
        "false,1,1.23456789,1.23,999999,2,3,2022-12-08T22:07:28.873Z,123456789,abcdefg",
        defaultRowToCsv().build().apply(row));

    assertEquals(
        "all schema fields and non default CSVFormat should convert all Row values using provided CSVFormat",
        "\"false\",\"1\",\"1.23456789\",\"1.23\",\"999999\",\"2\",\"3\",\"2022-12-08T22:07:28.873Z\",\"123456789\",\"abcdefg\"",
        defaultRowToCsv().setCSVFormat(CSVFormat.POSTGRESQL_CSV).build().apply(row));

    assertEquals(
        "schema field subset and default CSVFormat should convert specific Row values in provided order",
        "1.23456789,abcdefg,3",
        defaultRowToCsv()
            .setSchemaFields(Arrays.asList("aDouble", "string", "anInt"))
            .build()
            .apply(row));

    assertEquals(
        "schema field subset and non default CSVFormat should convert specific Row values in provided order using provided CSVFormat",
        "abcdefg\t1.23456789\t3",
        defaultRowToCsv()
            .setSchemaFields(Arrays.asList("string", "aDouble", "anInt"))
            .setCSVFormat(CSVFormat.MYSQL)
            .build()
            .apply(row));

    Schema schema =
        Schema.of(
            Field.of("string", FieldType.STRING),
            Field.nullable("aDouble", FieldType.DOUBLE),
            Field.nullable("anInt", FieldType.INT32));

    assertEquals(
        "null Row values should convert to empty strings",
        ",,abcdefg",
        RowToCsv.builder()
            .setSchema(schema)
            .setSchemaFields(schema.sorted().getFieldNames())
            .setCSVFormat(CSVFormat.DEFAULT)
            .build()
            .apply(Row.withSchema(schema).withFieldValue("string", "abcdefg").build()));
  }

  @Test
  public void invalidSchemaFields() {

    assertThrows(
        "schema field names should not be empty",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchemaFields(Arrays.asList("", "string", "aByte"))
                .build()
                .buildHeader());

    assertThrows(
        "schema fields should not be an empty list",
        IllegalArgumentException.class,
        () -> defaultRowToCsv().setSchemaFields(Collections.emptyList()).build().buildHeader());

    assertThrows(
        "field names should match schema",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchemaFields(Arrays.asList("doesnotexist", "string", "aByte"))
                .build()
                .buildHeader());
  }

  @Test
  public void invalidSchema() {
    assertThrows(
        "schema should not be empty",
        IllegalArgumentException.class,
        () -> defaultRowToCsv().setSchema(Schema.of()).build());

    assertThrows(
        "schema should not contain bytes field",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchema(
                    Schema.of(
                        Field.of("badfield", FieldType.BYTES), Field.of("ok", FieldType.STRING)))
                .build());

    assertThrows(
        "schema should not contain array field",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchema(
                    Schema.of(
                        Field.of("badfield", FieldType.array(FieldType.INT16)),
                        Field.of("ok", FieldType.STRING)))
                .build());

    assertThrows(
        "schema should not contain map field",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchema(
                    Schema.of(
                        Field.of("badfield", FieldType.map(FieldType.INT16, FieldType.STRING)),
                        Field.of("ok", FieldType.STRING)))
                .build());

    assertThrows(
        "schema should not contain row field",
        IllegalArgumentException.class,
        () ->
            defaultRowToCsv()
                .setSchema(
                    Schema.of(
                        Field.of("badfield", FieldType.row(ALL_DATA_TYPES_SCHEMA)),
                        Field.of("ok", FieldType.STRING)))
                .build());
  }

  private static RowToCsv.Builder defaultRowToCsv() {
    return RowToCsv.builder()
        .setCSVFormat(CSVFormat.DEFAULT)
        .setSchema(ALL_DATA_TYPES_SCHEMA)
        .setSchemaFields(ALL_DATA_TYPES_SCHEMA.sorted().getFieldNames());
  }
}
