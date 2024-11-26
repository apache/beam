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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.io.BaseEncoding;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryAvroUtils}. */
@RunWith(JUnit4.class)
public class BigQueryAvroUtilsTest {

  private TableSchema tableSchema(Function<TableFieldSchema, TableFieldSchema> fn) {
    TableFieldSchema column = new TableFieldSchema().setName("value");
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(Lists.newArrayList(fn.apply(column)));
    return tableSchema;
  }

  private Schema avroSchema(
      Function<SchemaBuilder.FieldBuilder<Schema>, SchemaBuilder.FieldAssembler<Schema>> fn) {
    return fn.apply(
            SchemaBuilder.record("root")
                .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                .doc("Translated Avro Schema for root")
                .fields()
                .name("value"))
        .endRecord();
  }

  @SuppressWarnings("JavaInstantGetSecondsGetNano")
  @Test
  public void testConvertGenericRecordToTableRow() {
    {
      // bool
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().booleanType().noDefault()))
              .set("value", false)
              .build();
      TableRow expected = new TableRow().set("value", false);
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // int
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().intType().noDefault()))
              .set("value", 5)
              .build();
      TableRow expected = new TableRow().set("value", "5");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // long
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().longType().noDefault()))
              .set("value", 5L)
              .build();
      TableRow expected = new TableRow().set("value", "5");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // float
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().floatType().noDefault()))
              .set("value", 5.5f)
              .build();
      TableRow expected = new TableRow().set("value", 5.5);
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // double
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().doubleType().noDefault()))
              .set("value", 5.55)
              .build();
      TableRow expected = new TableRow().set("value", 5.55);
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // bytes
      byte[] bytes = "chirp,chirp".getBytes(StandardCharsets.UTF_8);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().bytesType().noDefault()))
              .set("value", bb)
              .build();
      TableRow expected = new TableRow().set("value", BaseEncoding.base64().encode(bytes));
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // string
      Schema schema = avroSchema(f -> f.type().stringType().noDefault());
      GenericRecord record = new GenericRecordBuilder(schema).set("value", "test").build();
      GenericRecord utf8Record =
          new GenericRecordBuilder(schema).set("value", new Utf8("test")).build();
      TableRow expected = new TableRow().set("value", "test");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow utf8Row = BigQueryAvroUtils.convertGenericRecordToTableRow(utf8Record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
      assertEquals(expected, utf8Row);
      assertEquals(expected, utf8Row.clone());
    }

    {
      // decimal
      LogicalType lt = LogicalTypes.decimal(38, 9);
      Schema decimalType = lt.addToSchema(SchemaBuilder.builder().bytesType());
      BigDecimal bd = new BigDecimal("123456789.123456789");
      ByteBuffer bytes = new Conversions.DecimalConversion().toBytes(bd, null, lt);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(decimalType).noDefault()))
              .set("value", bytes)
              .build();
      TableRow expected = new TableRow().set("value", bd.toString());
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // date
      LogicalType lt = LogicalTypes.date();
      Schema dateType = lt.addToSchema(SchemaBuilder.builder().intType());
      LocalDate date = LocalDate.of(2000, 1, 1);
      int days = (int) date.toEpochDay();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(dateType).noDefault()))
              .set("value", days)
              .build();
      TableRow expected = new TableRow().set("value", "2000-01-01");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // time-millis
      LogicalType lt = LogicalTypes.timeMillis();
      Schema timeType = lt.addToSchema(SchemaBuilder.builder().intType());
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      int millis = (int) (time.toNanoOfDay() / 1000000);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timeType).noDefault()))
              .set("value", millis)
              .build();
      TableRow expected = new TableRow().set("value", "01:02:03.123");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // time-micros
      LogicalType lt = LogicalTypes.timeMicros();
      Schema timeType = lt.addToSchema(SchemaBuilder.builder().longType());
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      long micros = time.toNanoOfDay() / 1000;
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timeType).noDefault()))
              .set("value", micros)
              .build();
      TableRow expected = new TableRow().set("value", "01:02:03.123456");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // local-timestamp-millis
      LogicalType lt = LogicalTypes.localTimestampMillis();
      Schema timestampType = lt.addToSchema(SchemaBuilder.builder().longType());
      LocalDate date = LocalDate.of(2000, 1, 1);
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      LocalDateTime ts = LocalDateTime.of(date, time);
      long millis = ts.toInstant(ZoneOffset.UTC).toEpochMilli();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timestampType).noDefault()))
              .set("value", millis)
              .build();
      TableRow expected = new TableRow().set("value", "2000-01-01 01:02:03.123");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // local-timestamp-micros
      LogicalType lt = LogicalTypes.localTimestampMicros();
      Schema timestampType = lt.addToSchema(SchemaBuilder.builder().longType());
      LocalDate date = LocalDate.of(2000, 1, 1);
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      LocalDateTime ts = LocalDateTime.of(date, time);
      long seconds = ts.toInstant(ZoneOffset.UTC).getEpochSecond();
      int nanos = ts.toInstant(ZoneOffset.UTC).getNano();
      long micros = seconds * 1000000 + (nanos / 1000);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timestampType).noDefault()))
              .set("value", micros)
              .build();
      TableRow expected = new TableRow().set("value", "2000-01-01 01:02:03.123456");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // timestamp-micros
      LogicalType lt = LogicalTypes.timestampMillis();
      Schema timestampType = lt.addToSchema(SchemaBuilder.builder().longType());
      LocalDate date = LocalDate.of(2000, 1, 1);
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      LocalDateTime ts = LocalDateTime.of(date, time);
      long millis = ts.toInstant(ZoneOffset.UTC).toEpochMilli();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timestampType).noDefault()))
              .set("value", millis)
              .build();
      TableRow expected = new TableRow().set("value", "2000-01-01 01:02:03.123 UTC");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // timestamp-millis
      LogicalType lt = LogicalTypes.timestampMicros();
      Schema timestampType = lt.addToSchema(SchemaBuilder.builder().longType());
      LocalDate date = LocalDate.of(2000, 1, 1);
      LocalTime time = LocalTime.of(1, 2, 3, 123456789);
      LocalDateTime ts = LocalDateTime.of(date, time);
      long seconds = ts.toInstant(ZoneOffset.UTC).getEpochSecond();
      int nanos = ts.toInstant(ZoneOffset.UTC).getNano();
      long micros = seconds * 1000000 + (nanos / 1000);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(timestampType).noDefault()))
              .set("value", micros)
              .build();
      TableRow expected = new TableRow().set("value", "2000-01-01 01:02:03.123456 UTC");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // enum
      Schema enumSchema = SchemaBuilder.enumeration("color").symbols("red", "green", "blue");
      GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(enumSchema, "RED");
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(enumSchema).noDefault()))
              .set("value", symbol)
              .build();
      TableRow expected = new TableRow().set("value", "RED");
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // fixed
      UUID uuid = UUID.randomUUID();
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      bb.rewind();
      byte[] bytes = bb.array();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().fixed("uuid").size(16).noDefault()))
              .set("value", bb)
              .build();
      TableRow expected = new TableRow().set("value", BaseEncoding.base64().encode(bytes));
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // null
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().optional().booleanType())).build();
      TableRow expected = new TableRow();
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // array
      GenericRecord record =
          new GenericRecordBuilder(
                  avroSchema(f -> f.type().array().items().booleanType().noDefault()))
              .set("value", Lists.newArrayList(true, false))
              .build();
      TableRow expected = new TableRow().set("value", Lists.newArrayList(true, false));
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // map
      Map<String, Integer> map = new HashMap<>();
      map.put("left", 1);
      map.put("right", -1);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type().map().values().intType().noDefault()))
              .set("value", map)
              .build();
      TableRow expected =
          new TableRow()
              .set(
                  "value",
                  Lists.newArrayList(
                      new TableRow().set("key", "left").set("value", "1"),
                      new TableRow().set("key", "right").set("value", "-1")));
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }

    {
      // record
      Schema subSchema =
          SchemaBuilder.builder()
              .record("record")
              .fields()
              .name("int")
              .type()
              .intType()
              .noDefault()
              .name("float")
              .type()
              .floatType()
              .noDefault()
              .endRecord();
      GenericRecord subRecord =
          new GenericRecordBuilder(subSchema).set("int", 5).set("float", 5.5f).build();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema(f -> f.type(subSchema).noDefault()))
              .set("value", subRecord)
              .build();
      TableRow expected =
          new TableRow().set("value", new TableRow().set("int", "5").set("float", 5.5));
      TableRow row = BigQueryAvroUtils.convertGenericRecordToTableRow(record);

      assertEquals(expected, row);
      assertEquals(expected, row.clone());
    }
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchema() {
    {
      // REQUIRED
      TableSchema tableSchema = tableSchema(f -> f.setType("BOOLEAN").setMode("REQUIRED"));
      Schema expected = avroSchema(f -> f.type().booleanType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
    }

    {
      // NULLABLE
      TableSchema tableSchema = tableSchema(f -> f.setType("BOOLEAN").setMode("NULLABLE"));
      Schema expected =
          avroSchema(f -> f.type().unionOf().nullType().and().booleanType().endUnion().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
    }

    {
      // default mode -> NULLABLE
      TableSchema tableSchema = tableSchema(f -> f.setType("BOOLEAN"));
      Schema expected =
          avroSchema(f -> f.type().unionOf().nullType().and().booleanType().endUnion().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
    }

    {
      // REPEATED
      TableSchema tableSchema = tableSchema(f -> f.setType("BOOLEAN").setMode("REPEATED"));
      Schema expected = avroSchema(f -> f.type().array().items().booleanType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
    }

    {
      // INTEGER
      TableSchema tableSchema = tableSchema(f -> f.setType("INTEGER").setMode("REQUIRED"));
      Schema expected = avroSchema(f -> f.type().longType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
    }

    {
      // FLOAT
      TableSchema tableSchema = tableSchema(f -> f.setType("FLOAT").setMode("REQUIRED"));
      Schema expected = avroSchema(f -> f.type().doubleType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // BYTES
      TableSchema tableSchema = tableSchema(f -> f.setType("BYTES").setMode("REQUIRED"));
      Schema expected = avroSchema(f -> f.type().bytesType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // STRING
      TableSchema tableSchema = tableSchema(f -> f.setType("STRING").setMode("REQUIRED"));
      Schema expected = avroSchema(f -> f.type().stringType().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // NUMERIC
      TableSchema tableSchema = tableSchema(f -> f.setType("NUMERIC").setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(38, 9).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // NUMERIC with precision
      TableSchema tableSchema =
          tableSchema(f -> f.setType("NUMERIC").setPrecision(42L).setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(42, 0).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // NUMERIC with precision and scale
      TableSchema tableSchema =
          tableSchema(
              f -> f.setType("NUMERIC").setPrecision(42L).setScale(20L).setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(42, 20).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // BIGNUMERIC
      TableSchema tableSchema = tableSchema(f -> f.setType("BIGNUMERIC").setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(77, 38).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // BIGNUMERIC with precision
      TableSchema tableSchema =
          tableSchema(f -> f.setType("BIGNUMERIC").setPrecision(42L).setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(42, 0).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // BIGNUMERIC with precision and scale
      TableSchema tableSchema =
          tableSchema(
              f -> f.setType("BIGNUMERIC").setPrecision(42L).setScale(20L).setMode("REQUIRED"));
      Schema decimalType =
          LogicalTypes.decimal(42, 20).addToSchema(SchemaBuilder.builder().bytesType());
      Schema expected = avroSchema(f -> f.type(decimalType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // DATE
      TableSchema tableSchema = tableSchema(f -> f.setType("DATE").setMode("REQUIRED"));
      Schema dateType = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
      Schema expected = avroSchema(f -> f.type(dateType).noDefault());
      Schema expectedExport =
          avroSchema(f -> f.type().stringBuilder().prop("sqlType", "DATE").endString().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expectedExport, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // TIME
      TableSchema tableSchema = tableSchema(f -> f.setType("TIME").setMode("REQUIRED"));
      Schema timeType = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
      Schema expected = avroSchema(f -> f.type(timeType).noDefault());
      Schema expectedExport =
          avroSchema(f -> f.type().stringBuilder().prop("sqlType", "TIME").endString().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expectedExport, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // DATETIME
      TableSchema tableSchema = tableSchema(f -> f.setType("DATETIME").setMode("REQUIRED"));
      Schema timeType =
          BigQueryAvroUtils.DATETIME_LOGICAL_TYPE.addToSchema(SchemaBuilder.builder().stringType());
      Schema expected = avroSchema(f -> f.type(timeType).noDefault());
      Schema expectedExport =
          avroSchema(
              f -> f.type().stringBuilder().prop("sqlType", "DATETIME").endString().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expectedExport, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // TIMESTAMP
      TableSchema tableSchema = tableSchema(f -> f.setType("TIMESTAMP").setMode("REQUIRED"));
      Schema timestampType =
          LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType());
      Schema expected = avroSchema(f -> f.type(timestampType).noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // GEOGRAPHY
      TableSchema tableSchema = tableSchema(f -> f.setType("GEOGRAPHY").setMode("REQUIRED"));
      Schema expected =
          avroSchema(
              f -> f.type().stringBuilder().prop("sqlType", "GEOGRAPHY").endString().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // JSON
      TableSchema tableSchema = tableSchema(f -> f.setType("JSON").setMode("REQUIRED"));
      Schema expected =
          avroSchema(f -> f.type().stringBuilder().prop("sqlType", "JSON").endString().noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false));
    }

    {
      // STRUCT/RECORD
      TableFieldSchema subInteger =
          new TableFieldSchema().setName("int").setType("INTEGER").setMode("REQUIRED");
      TableFieldSchema subFloat =
          new TableFieldSchema().setName("float").setType("FLOAT").setMode("REQUIRED");
      TableSchema structTableSchema =
          tableSchema(
              f ->
                  f.setType("STRUCT")
                      .setMode("REQUIRED")
                      .setFields(Lists.newArrayList(subInteger, subFloat)));
      TableSchema recordTableSchema =
          tableSchema(
              f ->
                  f.setType("RECORD")
                      .setMode("REQUIRED")
                      .setFields(Lists.newArrayList(subInteger, subFloat)));

      Schema expected =
          avroSchema(
              f ->
                  f.type()
                      .record("value")
                      .fields()
                      .name("int")
                      .type()
                      .longType()
                      .noDefault()
                      .name("float")
                      .type()
                      .doubleType()
                      .noDefault()
                      .endRecord()
                      .noDefault());

      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(structTableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(structTableSchema, false));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(recordTableSchema));
      assertEquals(expected, BigQueryAvroUtils.toGenericAvroSchema(recordTableSchema, false));
    }
  }

  @Test
  public void testFormatTimestamp() {
    long micros = 1452062291123456L;
    String expected = "2016-01-06 06:38:11.123456";
    assertThat(BigQueryAvroUtils.formatDatetime(micros), equalTo(expected));
    assertThat(BigQueryAvroUtils.formatTimestamp(micros), equalTo(expected + " UTC"));
  }

  @Test
  public void testFormatTimestampMillis() {
    long millis = 1452062291123L;
    long micros = millis * 1000L;
    String expected = "2016-01-06 06:38:11.123";
    assertThat(BigQueryAvroUtils.formatDatetime(micros), equalTo(expected));
    assertThat(BigQueryAvroUtils.formatTimestamp(micros), equalTo(expected + " UTC"));
  }

  @Test
  public void testFormatTimestampSeconds() {
    long seconds = 1452062291L;
    long micros = seconds * 1000L * 1000L;
    String expected = "2016-01-06 06:38:11";
    assertThat(BigQueryAvroUtils.formatDatetime(micros), equalTo(expected));
    assertThat(BigQueryAvroUtils.formatTimestamp(micros), equalTo(expected + " UTC"));
  }

  @Test
  public void testFormatTimestampNegative() {
    assertThat(BigQueryAvroUtils.formatDatetime(-1L), equalTo("1969-12-31 23:59:59.999999"));
    assertThat(BigQueryAvroUtils.formatDatetime(-100_000L), equalTo("1969-12-31 23:59:59.900"));
    assertThat(BigQueryAvroUtils.formatDatetime(-1_000_000L), equalTo("1969-12-31 23:59:59"));
    // No leap seconds before 1972. 477 leap years from 1 through 1969.
    assertThat(
        BigQueryAvroUtils.formatDatetime(-(1969L * 365 + 477) * 86400 * 1_000_000),
        equalTo("0001-01-01 00:00:00"));
  }

  @Test
  public void testSchemaCollisionsInAvroConversion() {
    TableSchema schema = new TableSchema();
    schema.setFields(
        Lists.newArrayList(
            new TableFieldSchema()
                .setName("key_value_pair_1")
                .setType("RECORD")
                .setMode("REPEATED")
                .setFields(
                    Lists.newArrayList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema()
                            .setName("value")
                            .setType("RECORD")
                            .setFields(
                                Lists.newArrayList(
                                    new TableFieldSchema()
                                        .setName("string_value")
                                        .setType("STRING"),
                                    new TableFieldSchema().setName("int_value").setType("INTEGER"),
                                    new TableFieldSchema().setName("double_value").setType("FLOAT"),
                                    new TableFieldSchema()
                                        .setName("float_value")
                                        .setType("FLOAT"))))),
            new TableFieldSchema()
                .setName("key_value_pair_2")
                .setType("RECORD")
                .setMode("REPEATED")
                .setFields(
                    Lists.newArrayList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema()
                            .setName("value")
                            .setType("RECORD")
                            .setFields(
                                Lists.newArrayList(
                                    new TableFieldSchema()
                                        .setName("string_value")
                                        .setType("STRING"),
                                    new TableFieldSchema().setName("int_value").setType("INTEGER"),
                                    new TableFieldSchema().setName("double_value").setType("FLOAT"),
                                    new TableFieldSchema()
                                        .setName("float_value")
                                        .setType("FLOAT"))))),
            new TableFieldSchema()
                .setName("key_value_pair_3")
                .setType("RECORD")
                .setMode("REPEATED")
                .setFields(
                    Lists.newArrayList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema()
                            .setName("value")
                            .setType("RECORD")
                            .setFields(
                                Lists.newArrayList(
                                    new TableFieldSchema()
                                        .setName("key_value_pair_1")
                                        .setType("RECORD")
                                        .setMode("REPEATED")
                                        .setFields(
                                            Lists.newArrayList(
                                                new TableFieldSchema()
                                                    .setName("key")
                                                    .setType("STRING"),
                                                new TableFieldSchema()
                                                    .setName("value")
                                                    .setType("RECORD")
                                                    .setFields(
                                                        Lists.newArrayList(
                                                            new TableFieldSchema()
                                                                .setName("string_value")
                                                                .setType("STRING"),
                                                            new TableFieldSchema()
                                                                .setName("int_value")
                                                                .setType("INTEGER"),
                                                            new TableFieldSchema()
                                                                .setName("double_value")
                                                                .setType("FLOAT"),
                                                            new TableFieldSchema()
                                                                .setName("float_value")
                                                                .setType("FLOAT"))))))))),
            new TableFieldSchema().setName("platform").setType("STRING")));
    // To string should be sufficient here as this exercises Avro's conversion feature
    String output = BigQueryAvroUtils.toGenericAvroSchema(schema, false).toString();
    assertThat(output.length(), greaterThan(0));
  }
}
