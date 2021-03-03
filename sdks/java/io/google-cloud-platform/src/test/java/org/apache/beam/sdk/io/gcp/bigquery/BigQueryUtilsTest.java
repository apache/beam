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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
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
          .addNullableField("datetime", Schema.FieldType.logicalType(SqlTypes.DATETIME))
          .addNullableField("date", Schema.FieldType.logicalType(SqlTypes.DATE))
          .addNullableField("time", Schema.FieldType.logicalType(SqlTypes.TIME))
          .addNullableField("valid", Schema.FieldType.BOOLEAN)
          .addNullableField("binary", Schema.FieldType.BYTES)
          .addNullableField("numeric", Schema.FieldType.DECIMAL)
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

  private static final TableFieldSchema DATETIME =
      new TableFieldSchema().setName("datetime").setType(StandardSQLTypeName.DATETIME.toString());

  private static final TableFieldSchema DATE =
      new TableFieldSchema().setName("date").setType(StandardSQLTypeName.DATE.toString());

  private static final TableFieldSchema TIME =
      new TableFieldSchema().setName("time").setType(StandardSQLTypeName.TIME.toString());

  private static final TableFieldSchema VALID =
      new TableFieldSchema().setName("valid").setType(StandardSQLTypeName.BOOL.toString());

  private static final TableFieldSchema BINARY =
      new TableFieldSchema().setName("binary").setType(StandardSQLTypeName.BYTES.toString());

  private static final TableFieldSchema NUMERIC =
      new TableFieldSchema().setName("numeric").setType(StandardSQLTypeName.NUMERIC.toString());

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
                  DATETIME,
                  DATE,
                  TIME,
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
                  DATETIME,
                  DATE,
                  TIME,
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
              LocalDateTime.parse("2020-11-02T12:34:56.789876"),
              LocalDate.parse("2020-11-02"),
              LocalTime.parse("12:34:56.789876"),
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
          .set("datetime", "2020-11-02 12:34:56.789876")
          .set("date", "2020-11-02")
          .set("time", "12:34:56.789876")
          .set("valid", "false")
          .set("binary", "ABCD1234")
          .set("numeric", "123.456");

  private static final Row NULL_FLAT_ROW =
      Row.withSchema(FLAT_TYPE)
          .addValues(null, null, null, null, null, null, null, null, null, null, null, null, null)
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
          .set("datetime", null)
          .set("date", null)
          .set("time", null)
          .set("valid", null)
          .set("binary", null)
          .set("numeric", null);

  private static final Row ENUM_ROW =
      Row.withSchema(ENUM_TYPE).addValues(new EnumerationType.Value(1)).build();

  private static final Row ENUM_STRING_ROW =
      Row.withSchema(ENUM_STRING_TYPE).addValues("GREEN").build();

  private static final TableRow BQ_ENUM_ROW = new TableRow().set("color", "GREEN");

  private static final Row ARRAY_ROW =
      Row.withSchema(ARRAY_TYPE).addValues((Object) Arrays.asList(123L, 124L)).build();

  private static final Row MAP_ROW =
      Row.withSchema(MAP_MAP_TYPE).addValues(ImmutableMap.of("test", 123.456)).build();

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
                  DATETIME,
                  DATE,
                  TIME,
                  VALID,
                  BINARY,
                  NUMERIC));

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
            DATETIME,
            DATE,
            TIME,
            VALID,
            BINARY,
            NUMERIC));
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
            DATETIME,
            DATE,
            TIME,
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
            DATETIME,
            DATE,
            TIME,
            VALID,
            BINARY,
            NUMERIC));
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
  public void testToTableRow_flat() {
    TableRow row = toTableRow().apply(FLAT_ROW);
    System.out.println(row);

    assertThat(row.size(), equalTo(13));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("datetime", "2020-11-02 12:34:56.789876"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
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
    assertThat(row.size(), equalTo(13));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("datetime", "2020-11-02 12:34:56.789876"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
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
    assertThat(row.size(), equalTo(13));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("datetime", "2020-11-02 12:34:56.789876"));
    assertThat(row, hasEntry("date", "2020-11-02"));
    assertThat(row, hasEntry("time", "12:34:56.789876"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
    assertThat(row, hasEntry("binary", "ABCD1234"));
    assertThat(row, hasEntry("numeric", "123.456"));
  }

  @Test
  public void testToTableRow_null_row() {
    TableRow row = toTableRow().apply(NULL_FLAT_ROW);

    assertThat(row.size(), equalTo(13));
    assertThat(row, hasEntry("id", null));
    assertThat(row, hasEntry("value", null));
    assertThat(row, hasEntry("name", null));
    assertThat(row, hasEntry("timestamp_variant1", null));
    assertThat(row, hasEntry("timestamp_variant2", null));
    assertThat(row, hasEntry("timestamp_variant3", null));
    assertThat(row, hasEntry("timestamp_variant4", null));
    assertThat(row, hasEntry("datetime", null));
    assertThat(row, hasEntry("date", null));
    assertThat(row, hasEntry("time", null));
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

  private static final TableSchema BASE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(new TableFieldSchema().setType("STRING").setName("stringValue"))
                  .add(new TableFieldSchema().setType("BYTES").setName("bytesValue"))
                  .add(new TableFieldSchema().setType("INT64").setName("int64Value"))
                  .add(new TableFieldSchema().setType("INTEGER").setName("intValue"))
                  .add(new TableFieldSchema().setType("FLOAT64").setName("float64Value"))
                  .add(new TableFieldSchema().setType("FLOAT").setName("floatValue"))
                  .add(new TableFieldSchema().setType("BOOL").setName("boolValue"))
                  .add(new TableFieldSchema().setType("BOOLEAN").setName("booleanValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValue"))
                  .add(new TableFieldSchema().setType("TIME").setName("timeValue"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValue"))
                  .add(new TableFieldSchema().setType("DATE").setName("dateValue"))
                  .build());

  private static final DescriptorProto BASE_TABLE_SCHEMA_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("stringValue")
                  .setNumber(1)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytesValue")
                  .setNumber(2)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("int64Value")
                  .setNumber(3)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("intValue")
                  .setNumber(4)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("float64Value")
                  .setNumber(5)
                  .setType(Type.TYPE_FLOAT)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("floatValue")
                  .setNumber(6)
                  .setType(Type.TYPE_FLOAT)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("boolValue")
                  .setNumber(7)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("booleanValue")
                  .setNumber(8)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampValue")
                  .setNumber(9)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timeValue")
                  .setNumber(10)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimeValue")
                  .setNumber(11)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("dateValue")
                  .setNumber(12)
                  .setType(Type.TYPE_INT32)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .build();

  private static final TableSchema NESTED_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(
                      new TableFieldSchema()
                          .setType("STRUCT")
                          .setName("nestedValue1")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("RECORD")
                          .setName("nestedValue2")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .build());

  // For now, test that no exceptions are thrown.
  @Test
  public void testDescriptorFromTableSchema() {
    DescriptorProto descriptor = BigQueryUtils.descriptorSchemaFromTableSchema(BASE_TABLE_SCHEMA);
    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedTypes =
        BASE_TABLE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedTypes, types);
  }

  @Test
  public void testNestedFromTableSchema() {
    DescriptorProto descriptor = BigQueryUtils.descriptorSchemaFromTableSchema(NESTED_TABLE_SCHEMA);
    Map<String, Type> expectedBaseTypes =
        BASE_TABLE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));

    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, String> typeNames =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getTypeName));
    assertEquals(2, types.size());

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(2, nestedTypes.size());
    assertEquals(Type.TYPE_MESSAGE, types.get("nestedValue1"));
    String nestedTypeName1 = typeNames.get("nestedValue1");
    Map<String, Type> nestedTypes1 =
        nestedTypes.get(nestedTypeName1).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes1);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedValue2"));
    String nestedTypeName2 = typeNames.get("nestedValue2");
    Map<String, Type> nestedTypes2 =
        nestedTypes.get(nestedTypeName2).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes2);
  }

  @Test
  public void testRepeatedDescriptorFromTableSchema() {
    BigQueryUtils.descriptorSchemaFromTableSchema(BASE_TABLE_SCHEMA);
  }

  private TableRow getBaseRecord() {
    return new TableRow()
        .set("stringValue", "string")
        .set("bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
        .set("int64Value", 42L)
        .set("intValue", 43L)
        .set("float64Value", (float) 2.8168)
        .set("floatValue", (float) 2.817)
        .set("boolValue", true)
        .set("booleanValue", true)
        .set("timestampValue", 1L)
        .set("timeValue", 2L)
        .set("datetimeValue", 3L)
        .set("dateValue", 4);
  }

  @Test
  public void testMessageFromTableRow() throws Exception {
    TableRow baseRecord = getBaseRecord();
    Map<String, Object> baseRecordFields = ImmutableMap.copyOf(baseRecord);

    TableRow tableRow =
        new TableRow().set("nestedValue1", baseRecord).set("nestedValue2", baseRecord);
    Descriptor descriptor = BigQueryUtils.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA);
    DynamicMessage msg = BigQueryUtils.messageFromTableRow(descriptor, tableRow);
    assertEquals(2, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg1 = (DynamicMessage) msg.getField(fieldDescriptors.get("nestedValue1"));
    Map<String, Object> nestedMsg1Fields =
        nestedMsg1.getAllFields().entrySet().stream()
            .map(
                entry -> {
                  if (entry.getKey().getType() == FieldDescriptor.Type.BYTES) {
                    ByteString byteString = (ByteString) entry.getValue();
                    return new AbstractMap.SimpleEntry<>(
                        entry.getKey(), BaseEncoding.base64().encode(byteString.toByteArray()));
                  } else {
                    return entry;
                  }
                })
            .collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(baseRecordFields, nestedMsg1Fields);

    DynamicMessage nestedMsg2 = (DynamicMessage) msg.getField(fieldDescriptors.get("nestedValue2"));
    Map<String, Object> nestedMsg2Fields =
        nestedMsg2.getAllFields().entrySet().stream()
            .map(
                entry -> {
                  if (entry.getKey().getType() == FieldDescriptor.Type.BYTES) {
                    ByteString byteString = (ByteString) entry.getValue();
                    return new AbstractMap.SimpleEntry<>(
                        entry.getKey(), BaseEncoding.base64().encode(byteString.toByteArray()));
                  } else {
                    return entry;
                  }
                })
            .collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(baseRecordFields, nestedMsg2Fields);
  }
}
