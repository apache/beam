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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableRow;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSchema;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryUtils}. */
@RunWith(JUnit4.class)
public class BigQueryUtilsTest {
  private static final Schema FLAT_TYPE =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT64)
          .addNullableField("value", Schema.FieldType.DOUBLE)
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("timestamp_variant1", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant2", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant3", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant4", Schema.FieldType.DATETIME)
          .addNullableField("valid", Schema.FieldType.BOOLEAN)
          .addNullableField("binary", Schema.FieldType.BYTES)
          .addNullableField("numeric", Schema.FieldType.DECIMAL)
          .build();

  private static final Schema ARRAY_TYPE =
      Schema.builder().addArrayField("ids", Schema.FieldType.INT64).build();

  private static final Schema ROW_TYPE =
      Schema.builder().addNullableField("row", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final Schema ARRAY_ROW_TYPE =
      Schema.builder().addArrayField("rows", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final Schema MAP_TYPE =
      Schema.builder().addMapField("map", Schema.FieldType.STRING, Schema.FieldType.DOUBLE).build();

  private static final TableFieldSchema ID =
      new TableFieldSchema().setName("id").setType(StandardSQLTypeName.INT64.toString());

  private static final TableFieldSchema VALUE =
      new TableFieldSchema().setName("value").setType(StandardSQLTypeName.FLOAT64.toString());

  private static final TableFieldSchema NAME =
      new TableFieldSchema().setName("name").setType(StandardSQLTypeName.STRING.toString());

  private static final TableFieldSchema TIMESTAMP_VARIANT1 =
      new TableFieldSchema()
          .setName("timestamp_variant1")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT2 =
      new TableFieldSchema()
          .setName("timestamp_variant2")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT3 =
      new TableFieldSchema()
          .setName("timestamp_variant3")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT4 =
      new TableFieldSchema()
          .setName("timestamp_variant4")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());

  private static final TableFieldSchema VALID =
      new TableFieldSchema().setName("valid").setType(StandardSQLTypeName.BOOL.toString());

  private static final TableFieldSchema BINARY =
      new TableFieldSchema().setName("binary").setType(StandardSQLTypeName.BYTES.toString());

  private static final TableFieldSchema NUMERIC =
      new TableFieldSchema().setName("numeric").setType(StandardSQLTypeName.NUMERIC.toString());

  private static final TableFieldSchema IDS =
      new TableFieldSchema()
          .setName("ids")
          .setType(StandardSQLTypeName.INT64.toString())
          .setMode(Mode.REPEATED.toString());

  private static final TableFieldSchema MAP_KEY =
      new TableFieldSchema()
          .setName("key")
          .setType(StandardSQLTypeName.STRING.toString())
          .setMode(Mode.REQUIRED.toString());

  private static final TableFieldSchema MAP_VALUE =
      new TableFieldSchema()
          .setName("value")
          .setType(StandardSQLTypeName.FLOAT64.toString())
          .setMode(Mode.REQUIRED.toString());

  private static final TableFieldSchema ROW =
      new TableFieldSchema()
          .setName("row")
          .setType(StandardSQLTypeName.STRUCT.toString())
          .setMode(Mode.NULLABLE.toString())
          .setFields(
              Arrays.asList(
                  ID,
                  VALUE,
                  NAME,
                  TIMESTAMP_VARIANT1,
                  TIMESTAMP_VARIANT2,
                  TIMESTAMP_VARIANT3,
                  TIMESTAMP_VARIANT4,
                  VALID,
                  BINARY,
                  NUMERIC));

  private static final TableFieldSchema ROWS =
      new TableFieldSchema()
          .setName("rows")
          .setType(StandardSQLTypeName.STRUCT.toString())
          .setMode(Mode.REPEATED.toString())
          .setFields(
              Arrays.asList(
                  ID,
                  VALUE,
                  NAME,
                  TIMESTAMP_VARIANT1,
                  TIMESTAMP_VARIANT2,
                  TIMESTAMP_VARIANT3,
                  TIMESTAMP_VARIANT4,
                  VALID,
                  BINARY,
                  NUMERIC));

  private static final TableFieldSchema MAP =
      new TableFieldSchema()
          .setName("map")
          .setType(StandardSQLTypeName.STRUCT.toString())
          .setMode(Mode.REPEATED.toString())
          .setFields(Arrays.asList(MAP_KEY, MAP_VALUE));

  // Make sure that chosen BYTES test value is the same after a full base64 round trip.
  private static final Row FLAT_ROW =
      Row.withSchema(FLAT_TYPE)
          .addValues(
              123L,
              123.456,
              "test",
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2019-08-16T13:52:07.000"),
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2019-08-17T14:52:07.123"),
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2019-08-18T15:52:07.123"),
              new DateTime(123456),
              false,
              Base64.getDecoder().decode("ABCD1234"),
              new BigDecimal(123.456).setScale(3, RoundingMode.HALF_UP))
          .build();

  private static final TableRow BQ_FLAT_ROW =
      new TableRow()
          .set("id", "123")
          .set("value", "123.456")
          .set("name", "test")
          .set("timestamp_variant1", "2019-08-16 13:52:07 UTC")
          .set("timestamp_variant2", "2019-08-17 14:52:07.123 UTC")
          // we'll loose precession, but it's something BigQuery can output!
          .set("timestamp_variant3", "2019-08-18 15:52:07.123456 UTC")
          .set(
              "timestamp_variant4",
              String.valueOf(
                  new DateTime(123456L, ISOChronology.getInstanceUTC()).getMillis() / 1000.0D))
          .set("valid", "false")
          .set("binary", "ABCD1234")
          .set("numeric", "123.456");

  private static final Row NULL_FLAT_ROW =
      Row.withSchema(FLAT_TYPE)
          .addValues(null, null, null, null, null, null, null, null, null, null)
          .build();

  private static final TableRow BQ_NULL_FLAT_ROW =
      new TableRow()
          .set("id", null)
          .set("value", null)
          .set("name", null)
          .set("timestamp_variant1", null)
          .set("timestamp_variant2", null)
          .set("timestamp_variant3", null)
          .set("timestamp_variant4", null)
          .set("valid", null)
          .set("binary", null)
          .set("numeric", null);

  private static final Row ARRAY_ROW =
      Row.withSchema(ARRAY_TYPE).addValues((Object) Arrays.asList(123L, 124L)).build();

  private static final Row MAP_ROW =
      Row.withSchema(MAP_TYPE).addValues(ImmutableMap.of("test", 123.456)).build();

  private static final TableRow BQ_ARRAY_ROW =
      new TableRow()
          .set(
              "ids",
              Arrays.asList(
                  Collections.singletonMap("v", "123"), Collections.singletonMap("v", "124")));

  private static final Row ROW_ROW = Row.withSchema(ROW_TYPE).addValues(FLAT_ROW).build();

  private static final TableRow BQ_ROW_ROW = new TableRow().set("row", BQ_FLAT_ROW);

  private static final Row ARRAY_ROW_ROW =
      Row.withSchema(ARRAY_ROW_TYPE).addValues((Object) Arrays.asList(FLAT_ROW)).build();

  private static final TableRow BQ_ARRAY_ROW_ROW =
      new TableRow()
          .set("rows", Collections.singletonList(Collections.singletonMap("v", BQ_FLAT_ROW)));

  private static final TableSchema BQ_FLAT_TYPE =
      new TableSchema()
          .setFields(
              Arrays.asList(
                  ID,
                  VALUE,
                  NAME,
                  TIMESTAMP_VARIANT1,
                  TIMESTAMP_VARIANT2,
                  TIMESTAMP_VARIANT3,
                  TIMESTAMP_VARIANT4,
                  VALID,
                  BINARY,
                  NUMERIC));

  private static final TableSchema BQ_ARRAY_TYPE = new TableSchema().setFields(Arrays.asList(IDS));

  private static final TableSchema BQ_ROW_TYPE = new TableSchema().setFields(Arrays.asList(ROW));

  private static final TableSchema BQ_ARRAY_ROW_TYPE =
      new TableSchema().setFields(Arrays.asList(ROWS));

  private static final TableSchema BQ_MAP_TYPE = new TableSchema().setFields(Arrays.asList(MAP));

  private static final Schema AVRO_FLAT_TYPE =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT64)
          .addNullableField("value", Schema.FieldType.DOUBLE)
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("valid", Schema.FieldType.BOOLEAN)
          .build();

  private static final Schema AVRO_ARRAY_TYPE =
      Schema.builder().addArrayField("rows", Schema.FieldType.row(AVRO_FLAT_TYPE)).build();

  private static final Schema AVRO_ARRAY_ARRAY_TYPE =
      Schema.builder().addArrayField("array_rows", Schema.FieldType.row(AVRO_ARRAY_TYPE)).build();

  @Test
  public void testToTableSchema_flat() {
    TableSchema schema = toTableSchema(FLAT_TYPE);

    assertThat(
        schema.getFields(),
        containsInAnyOrder(
            ID,
            VALUE,
            NAME,
            TIMESTAMP_VARIANT1,
            TIMESTAMP_VARIANT2,
            TIMESTAMP_VARIANT3,
            TIMESTAMP_VARIANT4,
            VALID,
            BINARY,
            NUMERIC));
  }

  @Test
  public void testToTableSchema_array() {
    TableSchema schema = toTableSchema(ARRAY_TYPE);

    assertThat(schema.getFields(), contains(IDS));
  }

  @Test
  public void testToTableSchema_row() {
    TableSchema schema = toTableSchema(ROW_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("row"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), nullValue());
    assertThat(
        field.getFields(),
        containsInAnyOrder(
            ID,
            VALUE,
            NAME,
            TIMESTAMP_VARIANT1,
            TIMESTAMP_VARIANT2,
            TIMESTAMP_VARIANT3,
            TIMESTAMP_VARIANT4,
            VALID,
            BINARY,
            NUMERIC));
  }

  @Test
  public void testToTableSchema_array_row() {
    TableSchema schema = toTableSchema(ARRAY_ROW_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("rows"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), equalTo(Mode.REPEATED.toString()));
    assertThat(
        field.getFields(),
        containsInAnyOrder(
            ID,
            VALUE,
            NAME,
            TIMESTAMP_VARIANT1,
            TIMESTAMP_VARIANT2,
            TIMESTAMP_VARIANT3,
            TIMESTAMP_VARIANT4,
            VALID,
            BINARY,
            NUMERIC));
  }

  @Test
  public void testToTableSchema_map() {
    TableSchema schema = toTableSchema(MAP_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("map"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), equalTo(Mode.REPEATED.toString()));
    assertThat(field.getFields(), containsInAnyOrder(MAP_KEY, MAP_VALUE));
  }

  @Test
  public void testToTableRow_flat() {
    TableRow row = toTableRow().apply(FLAT_ROW);
    System.out.println(row);

    assertThat(row.size(), equalTo(10));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
  }

  @Test
  public void testToTableRow_array() {
    TableRow row = toTableRow().apply(ARRAY_ROW);

    assertThat(row, hasEntry("ids", Arrays.asList("123", "124")));
    assertThat(row.size(), equalTo(1));
  }

  @Test
  public void testToTableRow_map() {
    TableRow row = toTableRow().apply(MAP_ROW);

    assertThat(row.size(), equalTo(1));
    row = ((List<TableRow>) row.get("map")).get(0);
    assertThat(row.size(), equalTo(2));
    assertThat(row, hasEntry("key", "test"));
    assertThat(row, hasEntry("value", "123.456"));
  }

  @Test
  public void testToTableRow_row() {
    TableRow row = toTableRow().apply(ROW_ROW);

    assertThat(row.size(), equalTo(1));
    row = (TableRow) row.get("row");
    assertThat(row.size(), equalTo(10));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
  }

  @Test
  public void testToTableRow_array_row() {
    TableRow row = toTableRow().apply(ARRAY_ROW_ROW);

    assertThat(row.size(), equalTo(1));
    row = ((List<TableRow>) row.get("rows")).get(0);
    assertThat(row.size(), equalTo(10));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
  }

  @Test
  public void testToTableRow_null_row() {
    TableRow row = toTableRow().apply(NULL_FLAT_ROW);

    assertThat(row.size(), equalTo(10));
    assertThat(row, hasEntry("id", null));
    assertThat(row, hasEntry("value", null));
    assertThat(row, hasEntry("name", null));
    assertThat(row, hasEntry("timestamp_variant1", null));
    assertThat(row, hasEntry("timestamp_variant2", null));
    assertThat(row, hasEntry("timestamp_variant3", null));
    assertThat(row, hasEntry("timestamp_variant4", null));
    assertThat(row, hasEntry("valid", null));
    assertThat(row, hasEntry("binary", null));
    assertThat(row, hasEntry("numeric", null));
  }

  private static final BigQueryUtils.ConversionOptions TRUNCATE_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.TRUNCATE)
          .build();

  private static final BigQueryUtils.ConversionOptions REJECT_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.REJECT)
          .build();

  @Test
  public void testSubMilliPrecisionRejected() {
    assertThrows(
        "precision",
        IllegalArgumentException.class,
        () -> BigQueryUtils.convertAvroFormat(FieldType.DATETIME, 1000000001L, REJECT_OPTIONS));
  }

  @Test
  public void testMilliPrecisionOk() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(FieldType.DATETIME, millis * 1000, REJECT_OPTIONS),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testSubMilliPrecisionTruncated() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(FieldType.DATETIME, millis * 1000 + 123, TRUNCATE_OPTIONS),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testDateType() {
    LocalDate d = LocalDate.parse("2020-06-04");
    assertThat(
        BigQueryUtils.convertAvroFormat(
            FieldType.logicalType(SqlTypes.DATE), (int) d.toEpochDay(), REJECT_OPTIONS),
        equalTo(d));
  }

  @Test
  public void testMicroPrecisionTimeType() {
    LocalTime t = LocalTime.parse("12:34:56.789876");
    assertThat(
        BigQueryUtils.convertAvroFormat(
            FieldType.logicalType(SqlTypes.TIME), t.toNanoOfDay() / 1000, REJECT_OPTIONS),
        equalTo(t));
  }

  @Test
  public void testBytesType() {
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    assertThat(
        BigQueryUtils.convertAvroFormat(FieldType.BYTES, ByteBuffer.wrap(bytes), REJECT_OPTIONS),
        equalTo(bytes));
  }

  @Test
  public void testFromTableSchema_flat() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_FLAT_TYPE);
    assertEquals(FLAT_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_array() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ARRAY_TYPE);
    assertEquals(ARRAY_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_row() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ROW_TYPE);
    assertEquals(ROW_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_array_row() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ARRAY_ROW_TYPE);
    assertEquals(ARRAY_ROW_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_map() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_MAP_TYPE);
    assertEquals(MAP_TYPE, beamSchema);
  }

  @Test
  public void testToBeamRow_flat() {
    Row beamRow = BigQueryUtils.toBeamRow(FLAT_TYPE, BQ_FLAT_ROW);
    assertEquals(FLAT_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_null() {
    Row beamRow = BigQueryUtils.toBeamRow(FLAT_TYPE, BQ_NULL_FLAT_ROW);
    assertEquals(NULL_FLAT_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_array() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE, BQ_ARRAY_ROW);
    assertEquals(ARRAY_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_row() {
    Row beamRow = BigQueryUtils.toBeamRow(ROW_TYPE, BQ_ROW_ROW);
    assertEquals(ROW_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_array_row() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_ROW_TYPE, BQ_ARRAY_ROW_ROW);
    assertEquals(ARRAY_ROW_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_avro_array_row() {
    Row flatRowExpected =
        Row.withSchema(AVRO_FLAT_TYPE).addValues(123L, 123.456, "test", false).build();
    Row expected =
        Row.withSchema(AVRO_ARRAY_TYPE).addValues((Object) Arrays.asList(flatRowExpected)).build();
    GenericData.Record record = new GenericData.Record(AvroUtils.toAvroSchema(AVRO_ARRAY_TYPE));
    GenericData.Record flat = new GenericData.Record(AvroUtils.toAvroSchema(AVRO_FLAT_TYPE));
    flat.put("id", 123L);
    flat.put("value", 123.456);
    flat.put("name", "test");
    flat.put("valid", false);
    record.put("rows", Arrays.asList(flat));
    Row beamRow =
        BigQueryUtils.toBeamRow(
            record, AVRO_ARRAY_TYPE, BigQueryUtils.ConversionOptions.builder().build());
    assertEquals(expected, beamRow);
  }

  @Test
  public void testToBeamRow_avro_array_array_row() {
    Row flatRowExpected =
        Row.withSchema(AVRO_FLAT_TYPE).addValues(123L, 123.456, "test", false).build();
    Row arrayRowExpected =
        Row.withSchema(AVRO_ARRAY_TYPE).addValues((Object) Arrays.asList(flatRowExpected)).build();
    Row expected =
        Row.withSchema(AVRO_ARRAY_ARRAY_TYPE)
            .addValues((Object) Arrays.asList(arrayRowExpected))
            .build();
    GenericData.Record arrayRecord =
        new GenericData.Record(AvroUtils.toAvroSchema(AVRO_ARRAY_TYPE));
    GenericData.Record flat = new GenericData.Record(AvroUtils.toAvroSchema(AVRO_FLAT_TYPE));
    GenericData.Record record =
        new GenericData.Record(AvroUtils.toAvroSchema(AVRO_ARRAY_ARRAY_TYPE));
    flat.put("id", 123L);
    flat.put("value", 123.456);
    flat.put("name", "test");
    flat.put("valid", false);
    arrayRecord.put("rows", Arrays.asList(flat));
    record.put("array_rows", Arrays.asList(arrayRecord));
    Row beamRow =
        BigQueryUtils.toBeamRow(
            record, AVRO_ARRAY_ARRAY_TYPE, BigQueryUtils.ConversionOptions.builder().build());
    assertEquals(expected, beamRow);
  }
}
