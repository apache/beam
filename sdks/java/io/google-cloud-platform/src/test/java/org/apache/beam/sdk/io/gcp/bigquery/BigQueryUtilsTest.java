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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSpec;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
          .addNullableField("timestamp_variant5", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant6", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant7", Schema.FieldType.DATETIME)
          .addNullableField("timestamp_variant8", Schema.FieldType.DATETIME)
          .addNullableField("datetime", Schema.FieldType.logicalType(SqlTypes.DATETIME))
          .addNullableField("datetime0ms", Schema.FieldType.logicalType(SqlTypes.DATETIME))
          .addNullableField("datetime0s_ns", Schema.FieldType.logicalType(SqlTypes.DATETIME))
          .addNullableField("datetime0s_0ns", Schema.FieldType.logicalType(SqlTypes.DATETIME))
          .addNullableField("date", Schema.FieldType.logicalType(SqlTypes.DATE))
          .addNullableField("time", Schema.FieldType.logicalType(SqlTypes.TIME))
          .addNullableField("time0ms", Schema.FieldType.logicalType(SqlTypes.TIME))
          .addNullableField("time0s_ns", Schema.FieldType.logicalType(SqlTypes.TIME))
          .addNullableField("time0s_0ns", Schema.FieldType.logicalType(SqlTypes.TIME))
          .addNullableField("valid", Schema.FieldType.BOOLEAN)
          .addNullableField("binary", Schema.FieldType.BYTES)
          .addNullableField("raw_bytes", Schema.FieldType.BYTES)
          .addNullableField("numeric", Schema.FieldType.DECIMAL)
          .addNullableField("boolean", Schema.FieldType.BOOLEAN)
          .addNullableField("long", Schema.FieldType.INT64)
          .addNullableField("double", Schema.FieldType.DOUBLE)
          .build();

  private static final Schema ENUM_TYPE =
      Schema.builder()
          .addNullableField(
              "color", Schema.FieldType.logicalType(EnumerationType.create("RED", "GREEN", "BLUE")))
          .build();

  private static final Schema ENUM_STRING_TYPE =
      Schema.builder().addNullableField("color", Schema.FieldType.STRING).build();

  private static final Schema MAP_TYPE =
      Schema.builder().addStringField("key").addDoubleField("value").build();

  private static final Schema ARRAY_TYPE_NULLS =
      Schema.builder().addArrayField("ids", Schema.FieldType.INT64.withNullable(true)).build();

  private static final Schema ARRAY_TYPE =
      Schema.builder().addArrayField("ids", Schema.FieldType.INT64).build();

  private static final Schema ROW_TYPE =
      Schema.builder().addNullableField("row", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final Schema ARRAY_ROW_TYPE =
      Schema.builder().addArrayField("rows", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final Schema MAP_ARRAY_TYPE =
      Schema.builder().addArrayField("map", Schema.FieldType.row(MAP_TYPE)).build();

  private static final Schema MAP_MAP_TYPE =
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
  private static final TableFieldSchema TIMESTAMP_VARIANT5 =
      new TableFieldSchema()
          .setName("timestamp_variant5")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT6 =
      new TableFieldSchema()
          .setName("timestamp_variant6")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT7 =
      new TableFieldSchema()
          .setName("timestamp_variant7")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());
  private static final TableFieldSchema TIMESTAMP_VARIANT8 =
      new TableFieldSchema()
          .setName("timestamp_variant8")
          .setType(StandardSQLTypeName.TIMESTAMP.toString());

  private static final TableFieldSchema DATETIME =
      new TableFieldSchema().setName("datetime").setType(StandardSQLTypeName.DATETIME.toString());

  private static final TableFieldSchema DATETIME_0MS =
      new TableFieldSchema()
          .setName("datetime0ms")
          .setType(StandardSQLTypeName.DATETIME.toString());

  private static final TableFieldSchema DATETIME_0S_NS =
      new TableFieldSchema()
          .setName("datetime0s_ns")
          .setType(StandardSQLTypeName.DATETIME.toString());

  private static final TableFieldSchema DATETIME_0S_0NS =
      new TableFieldSchema()
          .setName("datetime0s_0ns")
          .setType(StandardSQLTypeName.DATETIME.toString());

  private static final TableFieldSchema DATE =
      new TableFieldSchema().setName("date").setType(StandardSQLTypeName.DATE.toString());

  private static final TableFieldSchema TIME =
      new TableFieldSchema().setName("time").setType(StandardSQLTypeName.TIME.toString());

  private static final TableFieldSchema TIME_0MS =
      new TableFieldSchema().setName("time0ms").setType(StandardSQLTypeName.TIME.toString());

  private static final TableFieldSchema TIME_0S_NS =
      new TableFieldSchema().setName("time0s_ns").setType(StandardSQLTypeName.TIME.toString());

  private static final TableFieldSchema TIME_0S_0NS =
      new TableFieldSchema().setName("time0s_0ns").setType(StandardSQLTypeName.TIME.toString());

  private static final TableFieldSchema VALID =
      new TableFieldSchema().setName("valid").setType(StandardSQLTypeName.BOOL.toString());

  private static final TableFieldSchema BINARY =
      new TableFieldSchema().setName("binary").setType(StandardSQLTypeName.BYTES.toString());

  private static final TableFieldSchema RAW_BYTES =
      new TableFieldSchema().setName("raw_bytes").setType(StandardSQLTypeName.BYTES.toString());

  private static final TableFieldSchema NUMERIC =
      new TableFieldSchema().setName("numeric").setType(StandardSQLTypeName.NUMERIC.toString());

  private static final TableFieldSchema BOOLEAN =
      new TableFieldSchema().setName("boolean").setType(StandardSQLTypeName.BOOL.toString());

  private static final TableFieldSchema LONG =
      new TableFieldSchema().setName("long").setType(StandardSQLTypeName.INT64.toString());

  private static final TableFieldSchema DOUBLE =
      new TableFieldSchema().setName("double").setType(StandardSQLTypeName.FLOAT64.toString());

  private static final TableFieldSchema COLOR =
      new TableFieldSchema().setName("color").setType(StandardSQLTypeName.STRING.toString());

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
                  TIMESTAMP_VARIANT5,
                  TIMESTAMP_VARIANT6,
                  TIMESTAMP_VARIANT7,
                  TIMESTAMP_VARIANT8,
                  DATETIME,
                  DATETIME_0MS,
                  DATETIME_0S_NS,
                  DATETIME_0S_0NS,
                  DATE,
                  TIME,
                  TIME_0MS,
                  TIME_0S_NS,
                  TIME_0S_0NS,
                  VALID,
                  BINARY,
                  RAW_BYTES,
                  NUMERIC,
                  BOOLEAN,
                  LONG,
                  DOUBLE));

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
                  TIMESTAMP_VARIANT5,
                  TIMESTAMP_VARIANT6,
                  TIMESTAMP_VARIANT7,
                  TIMESTAMP_VARIANT8,
                  DATETIME,
                  DATETIME_0MS,
                  DATETIME_0S_NS,
                  DATETIME_0S_0NS,
                  DATE,
                  TIME,
                  TIME_0MS,
                  TIME_0S_NS,
                  TIME_0S_0NS,
                  VALID,
                  BINARY,
                  RAW_BYTES,
                  NUMERIC,
                  BOOLEAN,
                  LONG,
                  DOUBLE));

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
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2024-08-10T16:52:07.1"),
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2024-08-10T16:52:07.12"),
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2024-08-10T16:52:07.1234"),
              ISODateTimeFormat.dateHourMinuteSecondFraction()
                  .withZoneUTC()
                  .parseDateTime("2024-08-10T16:52:07.12345"),
              LocalDateTime.parse("2020-11-02T12:34:56.789876"),
              LocalDateTime.parse("2020-11-02T12:34:56"),
              LocalDateTime.parse("2020-11-02T12:34:00.789876"),
              LocalDateTime.parse("2020-11-02T12:34"),
              LocalDate.parse("2020-11-02"),
              LocalTime.parse("12:34:56.789876"),
              LocalTime.parse("12:34:56"),
              LocalTime.parse("12:34:00.789876"),
              LocalTime.parse("12:34"),
              false,
              Base64.getDecoder().decode("ABCD1234"),
              Base64.getDecoder().decode("ABCD1234"),
              new BigDecimal("123.456").setScale(3, RoundingMode.HALF_UP),
              true,
              123L,
              123.456d)
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
          .set("timestamp_variant5", "2024-08-10 16:52:07.1 UTC")
          .set("timestamp_variant6", "2024-08-10 16:52:07.12 UTC")
          // we'll loose precession, but it's something BigQuery can output!
          .set("timestamp_variant7", "2024-08-10 16:52:07.1234 UTC")
          .set("timestamp_variant8", "2024-08-10 16:52:07.12345 UTC")
          .set("datetime", "2020-11-02T12:34:56.789876")
          .set("datetime0ms", "2020-11-02T12:34:56")
          .set("datetime0s_ns", "2020-11-02T12:34:00.789876")
          .set("datetime0s_0ns", "2020-11-02T12:34:00")
          .set("date", "2020-11-02")
          .set("time", "12:34:56.789876")
          .set("time0ms", "12:34:56")
          .set("time0s_ns", "12:34:00.789876")
          .set("time0s_0ns", "12:34:00")
          .set("valid", "false")
          .set("binary", "ABCD1234")
          .set("raw_bytes", Base64.getDecoder().decode("ABCD1234"))
          .set("numeric", "123.456")
          .set("boolean", true)
          .set("long", 123L)
          .set("double", 123.456d);

  private static final Row NULL_FLAT_ROW =
      Row.withSchema(FLAT_TYPE)
          .addValues(
              null, null, null, null, null, null, null, null, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null, null, null, null, null, null)
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
          .set("timestamp_variant5", null)
          .set("timestamp_variant6", null)
          .set("timestamp_variant7", null)
          .set("timestamp_variant8", null)
          .set("datetime", null)
          .set("datetime0ms", null)
          .set("datetime0s_ns", null)
          .set("datetime0s_0ns", null)
          .set("date", null)
          .set("time", null)
          .set("time0ms", null)
          .set("time0s_ns", null)
          .set("time0s_0ns", null)
          .set("valid", null)
          .set("binary", null)
          .set("raw_bytes", null)
          .set("numeric", null)
          .set("boolean", null)
          .set("long", null)
          .set("double", null);

  private static final Row ENUM_ROW =
      Row.withSchema(ENUM_TYPE).addValues(new EnumerationType.Value(1)).build();

  private static final Row ENUM_STRING_ROW =
      Row.withSchema(ENUM_STRING_TYPE).addValues("GREEN").build();

  private static final TableRow BQ_ENUM_ROW = new TableRow().set("color", "GREEN");

  private static final Row ARRAY_ROW_NULLS =
      Row.withSchema(ARRAY_TYPE_NULLS).addValues((Object) Arrays.asList(123L, null, null)).build();

  private static final Row ARRAY_ROW =
      Row.withSchema(ARRAY_TYPE).addValues((Object) Arrays.asList(123L, 124L)).build();

  private static final Row MAP_ROW =
      Row.withSchema(MAP_MAP_TYPE).addValues(ImmutableMap.of("test", 123.456)).build();

  private static final TableRow BQ_ARRAY_ROW_NULLS =
      new TableRow()
          .set(
              "ids",
              Arrays.asList(
                  Collections.singletonMap("v", "123"),
                  Collections.singletonMap("v", null),
                  Collections.singletonMap("v", null)));

  private static final TableRow BQ_ARRAY_ROW =
      new TableRow()
          .set(
              "ids",
              Arrays.asList(
                  Collections.singletonMap("v", "123"), Collections.singletonMap("v", "124")));

  // sometimes, a TableRow array will not be of format [{v: value1}, {v: value2}, ...]
  // it will instead be of format [value1, value2, ...]
  // this "inline row" covers the latter case
  private static final TableRow BQ_INLINE_ARRAY_ROW_NULLS =
      new TableRow().set("ids", Arrays.asList("123", null, null));
  private static final TableRow BQ_INLINE_ARRAY_ROW =
      new TableRow().set("ids", Arrays.asList("123", "124"));

  private static final Row ROW_ROW = Row.withSchema(ROW_TYPE).addValues(FLAT_ROW).build();

  private static final TableRow BQ_ROW_ROW = new TableRow().set("row", BQ_FLAT_ROW);

  private static final Row ARRAY_ROW_ROW =
      Row.withSchema(ARRAY_ROW_TYPE).addValues((Object) Arrays.asList(FLAT_ROW)).build();

  private static final TableRow BQ_ARRAY_ROW_ROW_V =
      new TableRow()
          .set("rows", Collections.singletonList(Collections.singletonMap("v", BQ_FLAT_ROW)));

  private static final TableRow BQ_ARRAY_ROW_ROW =
      new TableRow().set("rows", Collections.singletonList(BQ_FLAT_ROW));

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
                  TIMESTAMP_VARIANT5,
                  TIMESTAMP_VARIANT6,
                  TIMESTAMP_VARIANT7,
                  TIMESTAMP_VARIANT8,
                  DATETIME,
                  DATETIME_0MS,
                  DATETIME_0S_NS,
                  DATETIME_0S_0NS,
                  DATE,
                  TIME,
                  TIME_0MS,
                  TIME_0S_NS,
                  TIME_0S_0NS,
                  VALID,
                  BINARY,
                  RAW_BYTES,
                  NUMERIC,
                  BOOLEAN,
                  LONG,
                  DOUBLE));

  private static final TableSchema BQ_ENUM_TYPE = new TableSchema().setFields(Arrays.asList(COLOR));

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
            TIMESTAMP_VARIANT5,
            TIMESTAMP_VARIANT6,
            TIMESTAMP_VARIANT7,
            TIMESTAMP_VARIANT8,
            DATETIME,
            DATETIME_0MS,
            DATETIME_0S_NS,
            DATETIME_0S_0NS,
            DATE,
            TIME,
            TIME_0MS,
            TIME_0S_NS,
            TIME_0S_0NS,
            VALID,
            BINARY,
            RAW_BYTES,
            NUMERIC,
            BOOLEAN,
            LONG,
            DOUBLE));
  }

  @Test
  public void testToTableSchema_enum() {
    TableSchema schema = toTableSchema(ENUM_TYPE);

    assertThat(schema.getFields(), containsInAnyOrder(COLOR));
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
            TIMESTAMP_VARIANT5,
            TIMESTAMP_VARIANT6,
            TIMESTAMP_VARIANT7,
            TIMESTAMP_VARIANT8,
            DATETIME,
            DATETIME_0MS,
            DATETIME_0S_NS,
            DATETIME_0S_0NS,
            DATE,
            TIME,
            TIME_0MS,
            TIME_0S_NS,
            TIME_0S_0NS,
            VALID,
            BINARY,
            RAW_BYTES,
            NUMERIC,
            BOOLEAN,
            LONG,
            DOUBLE));
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
            TIMESTAMP_VARIANT5,
            TIMESTAMP_VARIANT6,
            TIMESTAMP_VARIANT7,
            TIMESTAMP_VARIANT8,
            DATETIME,
            DATETIME_0MS,
            DATETIME_0S_NS,
            DATETIME_0S_0NS,
            DATE,
            TIME,
            TIME_0MS,
            TIME_0S_NS,
            TIME_0S_0NS,
            VALID,
            BINARY,
            RAW_BYTES,
            NUMERIC,
            BOOLEAN,
            LONG,
            DOUBLE));
  }

  @Test
  public void testToTableSchema_map() {
    TableSchema schema = toTableSchema(MAP_MAP_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("map"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), equalTo(Mode.REPEATED.toString()));
    assertThat(field.getFields(), containsInAnyOrder(MAP_KEY, MAP_VALUE));
  }

  @Test
  public void testToTableSchema_map_array() {
    TableSchema schema = toTableSchema(MAP_ARRAY_TYPE);

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

    assertThat(row.size(), equalTo(27));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("timestamp_variant1", "2019-08-16 13:52:07.000 UTC"));
    assertThat(row, hasEntry("timestamp_variant2", "2019-08-17 14:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant3", "2019-08-18 15:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant4", "1970-01-01 00:02:03.456 UTC"));
    assertThat(row, hasEntry("timestamp_variant5", "2024-08-10 16:52:07.100 UTC"));
    assertThat(row, hasEntry("timestamp_variant6", "2024-08-10 16:52:07.120 UTC"));
    assertThat(row, hasEntry("timestamp_variant7", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant8", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("datetime", "2020-11-02T12:34:56.789876"));
    assertThat(row, hasEntry("datetime0ms", "2020-11-02T12:34:56"));
    assertThat(row, hasEntry("datetime0s_ns", "2020-11-02T12:34:00.789876"));
    assertThat(row, hasEntry("datetime0s_0ns", "2020-11-02T12:34:00"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
    assertThat(row, hasEntry("time0ms", "12:34:56"));
    assertThat(row, hasEntry("time0s_ns", "12:34:00.789876"));
    assertThat(row, hasEntry("time0s_0ns", "12:34:00"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("raw_bytes", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
    assertThat(row, hasEntry("boolean", "true"));
    assertThat(row, hasEntry("long", "123"));
    assertThat(row, hasEntry("double", "123.456"));
  }

  @Test
  public void testToTableRow_enum() {
    TableRow row = toTableRow().apply(ENUM_ROW);

    assertThat(row.size(), equalTo(1));
    assertThat(row, hasEntry("color", "GREEN"));
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
    assertThat(row.size(), equalTo(27));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("timestamp_variant1", "2019-08-16 13:52:07.000 UTC"));
    assertThat(row, hasEntry("timestamp_variant2", "2019-08-17 14:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant3", "2019-08-18 15:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant4", "1970-01-01 00:02:03.456 UTC"));
    assertThat(row, hasEntry("timestamp_variant5", "2024-08-10 16:52:07.100 UTC"));
    assertThat(row, hasEntry("timestamp_variant6", "2024-08-10 16:52:07.120 UTC"));
    assertThat(row, hasEntry("timestamp_variant7", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant8", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("datetime", "2020-11-02T12:34:56.789876"));
    assertThat(row, hasEntry("datetime0ms", "2020-11-02T12:34:56"));
    assertThat(row, hasEntry("datetime0s_ns", "2020-11-02T12:34:00.789876"));
    assertThat(row, hasEntry("datetime0s_0ns", "2020-11-02T12:34:00"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
    assertThat(row, hasEntry("time0ms", "12:34:56"));
    assertThat(row, hasEntry("time0s_ns", "12:34:00.789876"));
    assertThat(row, hasEntry("time0s_0ns", "12:34:00"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("raw_bytes", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
    assertThat(row, hasEntry("boolean", "true"));
    assertThat(row, hasEntry("long", "123"));
    assertThat(row, hasEntry("double", "123.456"));
  }

  @Test
  public void testToTableRow_array_row() {
    TableRow row = toTableRow().apply(ARRAY_ROW_ROW);

    assertThat(row.size(), equalTo(1));
    row = ((List<TableRow>) row.get("rows")).get(0);
    assertThat(row.size(), equalTo(27));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("timestamp_variant1", "2019-08-16 13:52:07.000 UTC"));
    assertThat(row, hasEntry("timestamp_variant2", "2019-08-17 14:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant3", "2019-08-18 15:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant4", "1970-01-01 00:02:03.456 UTC"));
    assertThat(row, hasEntry("timestamp_variant5", "2024-08-10 16:52:07.100 UTC"));
    assertThat(row, hasEntry("timestamp_variant6", "2024-08-10 16:52:07.120 UTC"));
    assertThat(row, hasEntry("timestamp_variant7", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("timestamp_variant8", "2024-08-10 16:52:07.123 UTC"));
    assertThat(row, hasEntry("datetime", "2020-11-02T12:34:56.789876"));
    assertThat(row, hasEntry("datetime0ms", "2020-11-02T12:34:56"));
    assertThat(row, hasEntry("datetime0s_ns", "2020-11-02T12:34:00.789876"));
    assertThat(row, hasEntry("datetime0s_0ns", "2020-11-02T12:34:00"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
    assertThat(row, hasEntry("time0ms", "12:34:56"));
    assertThat(row, hasEntry("time0s_ns", "12:34:00.789876"));
    assertThat(row, hasEntry("time0s_0ns", "12:34:00"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("raw_bytes", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
    assertThat(row, hasEntry("boolean", "true"));
    assertThat(row, hasEntry("long", "123"));
    assertThat(row, hasEntry("double", "123.456"));
  }

  @Test
  public void testToTableRow_null_row() {
    TableRow row = toTableRow().apply(NULL_FLAT_ROW);

    assertThat(row.size(), equalTo(27));
    assertThat(row, hasEntry("id", null));
    assertThat(row, hasEntry("value", null));
    assertThat(row, hasEntry("name", null));
    assertThat(row, hasEntry("timestamp_variant1", null));
    assertThat(row, hasEntry("timestamp_variant2", null));
    assertThat(row, hasEntry("timestamp_variant3", null));
    assertThat(row, hasEntry("timestamp_variant4", null));
    assertThat(row, hasEntry("timestamp_variant5", null));
    assertThat(row, hasEntry("timestamp_variant6", null));
    assertThat(row, hasEntry("timestamp_variant7", null));
    assertThat(row, hasEntry("timestamp_variant8", null));
    assertThat(row, hasEntry("datetime", null));
    assertThat(row, hasEntry("datetime0ms", null));
    assertThat(row, hasEntry("datetime0s_ns", null));
    assertThat(row, hasEntry("datetime0s_0ns", null));
    assertThat(row, hasEntry("date", null));
    assertThat(row, hasEntry("time", null));
    assertThat(row, hasEntry("time0ms", null));
    assertThat(row, hasEntry("time0s_ns", null));
    assertThat(row, hasEntry("time0s_0ns", null));
    assertThat(row, hasEntry("valid", null));
    assertThat(row, hasEntry("binary", null));
    assertThat(row, hasEntry("raw_bytes", null));
    assertThat(row, hasEntry("numeric", null));
    assertThat(row, hasEntry("boolean", null));
    assertThat(row, hasEntry("long", null));
    assertThat(row, hasEntry("double", null));
  }

  private static final BigQueryUtils.ConversionOptions TRUNCATE_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.TRUNCATE)
          .build();

  private static final BigQueryUtils.ConversionOptions REJECT_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.REJECT)
          .build();

  private static final BigQueryUtils.SchemaConversionOptions INFER_MAPS_OPTIONS =
      BigQueryUtils.SchemaConversionOptions.builder().setInferMaps(true).build();

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
  public void testMicroPrecisionDateTimeType() {
    LocalDateTime dt = LocalDateTime.parse("2020-06-04T12:34:56.789876");
    assertThat(
        BigQueryUtils.convertAvroFormat(
            FieldType.logicalType(SqlTypes.DATETIME), new Utf8(dt.toString()), REJECT_OPTIONS),
        equalTo(dt));
  }

  @Test
  public void testNumericType() {
    // BigQuery NUMERIC type has precision 38 and scale 9
    BigDecimal n = new BigDecimal("123456789.987654321").setScale(9);
    assertThat(
        BigQueryUtils.convertAvroFormat(
            FieldType.DECIMAL,
            new Conversions.DecimalConversion().toBytes(n, null, LogicalTypes.decimal(38, 9)),
            REJECT_OPTIONS),
        equalTo(n));
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
  public void testFromTableSchema_enum() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ENUM_TYPE);
    assertEquals(ENUM_STRING_TYPE, beamSchema);
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
  public void testFromTableSchema_map_array() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_MAP_TYPE);
    assertEquals(MAP_ARRAY_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_map_map() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_MAP_TYPE, INFER_MAPS_OPTIONS);
    assertEquals(MAP_MAP_TYPE, beamSchema);
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
  public void testToBeamRow_enum() {
    Row beamRow = BigQueryUtils.toBeamRow(ENUM_STRING_TYPE, BQ_ENUM_ROW);
    assertEquals(ENUM_STRING_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_arrayNulls() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE_NULLS, BQ_ARRAY_ROW_NULLS);
    assertEquals(ARRAY_ROW_NULLS, beamRow);
  }

  @Test
  public void testToBeamRow_array() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE, BQ_ARRAY_ROW);
    assertEquals(ARRAY_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_inlineArrayNulls() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE_NULLS, BQ_INLINE_ARRAY_ROW_NULLS);
    assertEquals(ARRAY_ROW_NULLS, beamRow);
  }

  @Test
  public void testToBeamRow_inlineArray() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE, BQ_INLINE_ARRAY_ROW);
    assertEquals(ARRAY_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_row() {
    Row beamRow = BigQueryUtils.toBeamRow(ROW_TYPE, BQ_ROW_ROW);
    assertEquals(ROW_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_array_row_v() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_ROW_TYPE, BQ_ARRAY_ROW_ROW_V);
    assertEquals(ARRAY_ROW_ROW, beamRow);
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

  @Test
  public void testToBeamRow_projection() {
    long testId = 123L;
    // recordSchema is a projection of FLAT_TYPE schema
    org.apache.avro.Schema recordSchema =
        org.apache.avro.SchemaBuilder.record("__root__").fields().optionalLong("id").endRecord();
    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("id", testId);

    Row expected = Row.withSchema(FLAT_TYPE).withFieldValue("id", testId).build();
    Row actual =
        BigQueryUtils.toBeamRow(
            record, FLAT_TYPE, BigQueryUtils.ConversionOptions.builder().build());
    assertEquals(expected, actual);
  }

  @Test
  public void testToTableSpec() {
    TableReference withProject =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    TableReference withoutProject =
        new TableReference().setDatasetId("dataset").setTableId("table");
    TableReference withDatasetOnly = new TableReference().setDatasetId("dataset");
    TableReference withTableOnly = new TableReference().setTableId("table");

    assertEquals("project.dataset.table", toTableSpec(withProject));
    assertEquals("dataset.table", toTableSpec(withoutProject));
    assertThrows(
        "must include at least a dataset and a table",
        IllegalArgumentException.class,
        () -> toTableSpec(withDatasetOnly));
    assertThrows(
        "must include at least a dataset and a table",
        IllegalArgumentException.class,
        () -> toTableSpec(withTableOnly));
  }

  @Test
  public void testToTableReference() {
    {
      TableReference tr =
          BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/tables/mytable");
      assertEquals("myproject", tr.getProjectId());
      assertEquals("mydataset", tr.getDatasetId());
      assertEquals("mytable", tr.getTableId());
    }

    {
      // Test colon(":") after project format
      TableReference tr = BigQueryUtils.toTableReference("myprojectwithcolon:mydataset.mytable");
      assertEquals("myprojectwithcolon", tr.getProjectId());
      assertEquals("mydataset", tr.getDatasetId());
      assertEquals("mytable", tr.getTableId());
    }

    {
      // Test dot(".") after project format
      TableReference tr = BigQueryUtils.toTableReference("myprojectwithdot.mydataset.mytable");
      assertEquals("myprojectwithdot", tr.getProjectId());
      assertEquals("mydataset", tr.getDatasetId());
      assertEquals("mytable", tr.getTableId());
    }

    {
      // Test project that contains a dot and colon
      TableReference tr = BigQueryUtils.toTableReference("project.with:domain.mydataset.mytable");
      assertEquals("project.with:domain", tr.getProjectId());
      assertEquals("mydataset", tr.getDatasetId());
      assertEquals("mytable", tr.getTableId());
    }

    // Invalid scenarios
    assertNull(BigQueryUtils.toTableReference(""));
    assertNull(BigQueryUtils.toTableReference(":."));
    assertNull(BigQueryUtils.toTableReference(".."));
    assertNull(BigQueryUtils.toTableReference("myproject"));
    assertNull(BigQueryUtils.toTableReference("myproject:"));
    assertNull(BigQueryUtils.toTableReference("myproject."));
    assertNull(BigQueryUtils.toTableReference("myproject:mydataset"));
    assertNull(BigQueryUtils.toTableReference("myproject:mydataset."));
    assertNull(BigQueryUtils.toTableReference("myproject:mydataset.mytable."));
    assertNull(BigQueryUtils.toTableReference("myproject:mydataset:mytable:"));
    assertNull(BigQueryUtils.toTableReference("myproject:my dataset:mytable:"));
    assertNull(BigQueryUtils.toTableReference(".invalidleadingdot.mydataset.mytable"));
    assertNull(BigQueryUtils.toTableReference("invalidtrailingdot.mydataset.mytable."));
    assertNull(BigQueryUtils.toTableReference(":invalidleadingcolon.mydataset.mytable"));
    assertNull(BigQueryUtils.toTableReference("invalidtrailingcolon.mydataset.mytable:"));
    assertNull(BigQueryUtils.toTableReference("projectendswithhyphen-.mydataset.mytable"));
    assertNull(
        BigQueryUtils.toTableReference(
            "projectnamegoesbeyondthe30characterlimit.mydataset.mytable"));

    assertNull(
        BigQueryUtils.toTableReference("/projects/extraslash/datasets/mydataset/tables/mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects//extraslash/datasets/mydataset/tables/mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects/extraslash//datasets/mydataset/tables/mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects/extraslash/datasets//mydataset/tables/mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects/extraslash/datasets/mydataset//tables/mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects/extraslash/datasets/mydataset/tables//mytable"));
    assertNull(
        BigQueryUtils.toTableReference("projects/extraslash/datasets/mydataset/tables/mytable/"));

    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/tables//"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets//tables/mytable/"));
    assertNull(BigQueryUtils.toTableReference("projects//datasets/mydataset/tables/mytable/"));
    assertNull(BigQueryUtils.toTableReference("projects//datasets//tables//"));

    assertNull(
        BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/tables/mytable/"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/tables/"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/tables"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset/"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/mydataset"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets/"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/datasets"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject/"));
    assertNull(BigQueryUtils.toTableReference("projects/myproject"));
    assertNull(BigQueryUtils.toTableReference("projects/"));
    assertNull(BigQueryUtils.toTableReference("projects"));
  }

  @Test
  public void testTrimSchema() {
    assertEquals(BQ_FLAT_TYPE, BigQueryUtils.trimSchema(BQ_FLAT_TYPE, null));
    assertEquals(BQ_FLAT_TYPE, BigQueryUtils.trimSchema(BQ_FLAT_TYPE, Collections.emptyList()));

    {
      TableSchema expected = new TableSchema().setFields(Arrays.asList(ID, VALUE, NAME));
      assertEquals(
          expected, BigQueryUtils.trimSchema(BQ_FLAT_TYPE, Arrays.asList("id", "value", "name")));
    }

    {
      TableFieldSchema filteredRow =
          new TableFieldSchema()
              .setName("row")
              .setType(StandardSQLTypeName.STRUCT.toString())
              .setMode(Mode.NULLABLE.toString())
              .setFields(Arrays.asList(ID, VALUE, NAME));
      TableSchema expected = new TableSchema().setFields(Collections.singletonList(filteredRow));
      assertEquals(
          expected,
          BigQueryUtils.trimSchema(BQ_ROW_TYPE, Arrays.asList("row.id", "row.value", "row.name")));
    }
  }
}
