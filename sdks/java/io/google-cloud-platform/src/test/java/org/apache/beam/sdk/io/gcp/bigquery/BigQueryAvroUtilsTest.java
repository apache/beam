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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryAvroUtils}. */
@RunWith(JUnit4.class)
public class BigQueryAvroUtilsTest {
  private List<TableFieldSchema> subFields =
      Lists.newArrayList(
          new TableFieldSchema().setName("value").setType("STRING").setMode("NULLABLE"));
  /*
   * Note that the quality and quantity fields do not have their mode set, so they should default
   * to NULLABLE. This is an important test of BigQuery semantics.
   *
   * All the other fields we set in this function are required on the Schema response.
   *
   * See https://cloud.google.com/bigquery/docs/reference/v2/tables#schema
   */
  private List<TableFieldSchema> fields =
      Lists.newArrayList(
          new TableFieldSchema().setName("int").setType("INTEGER").setMode("REQUIRED"),
          new TableFieldSchema().setName("long").setType("INTEGER").setMode("NULLABLE"),
          new TableFieldSchema().setName("string").setType("STRING").setMode("NULLABLE"),
          new TableFieldSchema().setName("float").setType("FLOAT") /* default to NULLABLE */,
          new TableFieldSchema().setName("double").setType("FLOAT") /* default to NULLABLE */,
          new TableFieldSchema().setName("timestamp").setType("TIMESTAMP").setMode("NULLABLE"),
          new TableFieldSchema().setName("numeric").setType("NUMERIC").setMode("NULLABLE"),
          new TableFieldSchema().setName("bignumeric").setType("BIGNUMERIC").setMode("NULLABLE"),
          new TableFieldSchema().setName("bool").setType("BOOLEAN").setMode("NULLABLE"),
          new TableFieldSchema().setName("bytes").setType("BYTES").setMode("NULLABLE"),
          new TableFieldSchema().setName("date").setType("DATE").setMode("NULLABLE"),
          new TableFieldSchema().setName("datetime").setType("DATETIME").setMode("NULLABLE"),
          new TableFieldSchema().setName("time").setType("TIME").setMode("NULLABLE"),
          new TableFieldSchema()
              .setName("record")
              .setType("RECORD")
              .setMode("NULLABLE")
              .setFields(subFields),
          new TableFieldSchema()
              .setName("multirecord")
              .setType("RECORD")
              .setMode("REPEATED")
              .setFields(subFields),
          new TableFieldSchema().setName("geography").setType("GEOGRAPHY").setMode("NULLABLE"));

  private ByteBuffer convertToBytes(BigDecimal bigDecimal, int precision, int scale) {
    LogicalType bigDecimalLogicalType = LogicalTypes.decimal(precision, scale);
    return new Conversions.DecimalConversion().toBytes(bigDecimal, null, bigDecimalLogicalType);
  }

  @Test
  public void testConvertGenericRecordToTableRow() throws Exception {
    BigDecimal numeric = new BigDecimal("123456789.123456789");
    ByteBuffer numericBytes = convertToBytes(numeric, 38, 9);
    BigDecimal bigNumeric =
        new BigDecimal(
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967");
    ByteBuffer bigNumericBytes = convertToBytes(bigNumeric, 77, 38);
    Schema avroSchema = ReflectData.get().getSchema(TestRecord.class);

    {
      // Test nullable fields.
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("int", 5);
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow row = new TableRow().set("int", "5").set("multirecord", new ArrayList<TableRow>());
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
    }

    {
      // Test type conversion for:
      // INTEGER, FLOAT, NUMERIC, TIMESTAMP, BOOLEAN, BYTES, DATE, DATETIME, TIME.
      byte[] bytes = "chirp,chirp".getBytes(StandardCharsets.UTF_8);
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.rewind();

      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("int", 5);
      record.put("long", 5L);
      record.put("string", "value");
      record.put("float", 5.5f);
      record.put("double", 5.55);
      record.put("timestamp", 5L);
      record.put("numeric", numericBytes);
      record.put("bignumeric", bigNumericBytes);
      record.put("bool", true);
      record.put("bytes", byteBuffer);
      record.put("date", new Utf8("2000-01-01"));
      record.put("datetime", new String("2000-01-01 00:00:00.000005"));
      record.put("time", new Utf8("00:00:00.000005"));
      record.put("geography", new String("LINESTRING(1 2, 3 4, 5 6, 7 8)"));

      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      // alphabetically sorted fields
      TableRow row =
          new TableRow()
              .set("bignumeric", bigNumeric.toString())
              .set("bool", Boolean.TRUE)
              .set("bytes", BaseEncoding.base64().encode(bytes))
              .set("date", "2000-01-01")
              .set("datetime", "2000-01-01 00:00:00.000005")
              .set("double", 5.55)
              .set("float", 5.5)
              .set("geography", "LINESTRING(1 2, 3 4, 5 6, 7 8)")
              .set("int", "5")
              .set("long", "5")
              .set("multirecord", new ArrayList<TableRow>())
              .set("numeric", numeric.toString())
              .set("string", "value")
              .set("time", "00:00:00.000005")
              .set("timestamp", "1970-01-01 00:00:00.000005 UTC");

      assertEquals(convertedRow, row);

      // make sure converted TableRow row is cloneable
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
      assertEquals(row, convertedRow);
    }

    {
      // Test repeated fields.
      Schema subSchema = AvroCoder.of(TestRecord.SubRecord.class).getSchema();
      GenericRecord nestedRecord = new GenericData.Record(subSchema);
      nestedRecord.put("value", "other");
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("int", 5);
      record.put("multirecord", Lists.newArrayList(nestedRecord));
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow row =
          new TableRow()
              .set("int", "5")
              .set("multirecord", Lists.newArrayList(new TableRow().set("value", "other")));
      assertEquals(row, convertedRow);

      // make sure converted TableRow row is cloneable
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
      assertEquals(row, convertedRow);
    }
  }

  private static SchemaBuilder.TypeBuilder<Schema> typeBuilder() {
    return SchemaBuilder.builder();
  }

  private static Schema required(Schema type) {
    return type;
  }

  private static Schema nullable(Schema type) {
    return SchemaBuilder.builder().unionOf().nullType().and().type(type).endUnion();
  }

  private static Schema array(Schema type) {
    return SchemaBuilder.builder().array().items(type);
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchema() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    Schema avroSchema = BigQueryAvroUtils.toGenericAvroSchema(tableSchema);

    assertThat(avroSchema.getField("int").schema(), equalTo(required(typeBuilder().longType())));
    assertThat(avroSchema.getField("long").schema(), equalTo(nullable(typeBuilder().longType())));
    assertThat(
        avroSchema.getField("string").schema(), equalTo(nullable(typeBuilder().stringType())));
    assertThat(
        avroSchema.getField("float").schema(), equalTo(nullable(typeBuilder().doubleType())));
    assertThat(
        avroSchema.getField("double").schema(), equalTo(nullable(typeBuilder().doubleType())));
    assertThat(
        avroSchema.getField("timestamp").schema(),
        equalTo(nullable(LogicalTypes.timestampMicros().addToSchema(typeBuilder().longType()))));
    assertThat(
        avroSchema.getField("numeric").schema(),
        equalTo(nullable(LogicalTypes.decimal(38, 9).addToSchema(typeBuilder().bytesType()))));
    assertThat(
        avroSchema.getField("bignumeric").schema(),
        equalTo(nullable(LogicalTypes.decimal(77, 38).addToSchema(typeBuilder().bytesType()))));
    assertThat(
        avroSchema.getField("bool").schema(), equalTo(nullable(typeBuilder().booleanType())));
    assertThat(avroSchema.getField("bytes").schema(), equalTo(nullable(typeBuilder().bytesType())));
    assertThat(
        avroSchema.getField("date").schema(),
        equalTo(nullable(LogicalTypes.date().addToSchema(typeBuilder().intType()))));
    assertThat(
        avroSchema.getField("datetime").schema(),
        equalTo(
            nullable(
                BigQueryAvroUtils.DATETIME_LOGICAL_TYPE.addToSchema(typeBuilder().stringType()))));
    assertThat(
        avroSchema.getField("time").schema(),
        equalTo(nullable(LogicalTypes.timeMicros().addToSchema(typeBuilder().longType()))));
    assertThat(
        avroSchema.getField("geography").schema(),
        equalTo(nullable(typeBuilder().stringBuilder().prop("sqlType", "GEOGRAPHY").endString())));
    assertThat(
        avroSchema.getField("record").schema(),
        equalTo(
            nullable(
                typeBuilder()
                    .record("record")
                    .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                    .doc("Translated Avro Schema for scion")
                    .fields()
                    .name("value")
                    .type(nullable(typeBuilder().stringType()))
                    .noDefault()
                    .endRecord())));
    assertThat(
        avroSchema.getField("multirecord").schema(),
        equalTo(
            array(
                typeBuilder()
                    .record("multirecord")
                    .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                    .doc("Translated Avro Schema for scion")
                    .fields()
                    .name("value")
                    .type(nullable(typeBuilder().stringType()))
                    .noDefault()
                    .endRecord())));
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchemaWithoutLogicalTypes() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    Schema avroSchema = BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false);

    assertThat(avroSchema.getField("int").schema(), equalTo(required(typeBuilder().longType())));
    assertThat(avroSchema.getField("long").schema(), equalTo(nullable(typeBuilder().longType())));
    assertThat(
        avroSchema.getField("string").schema(), equalTo(nullable(typeBuilder().stringType())));
    assertThat(
        avroSchema.getField("float").schema(), equalTo(nullable(typeBuilder().doubleType())));
    assertThat(
        avroSchema.getField("double").schema(), equalTo(nullable(typeBuilder().doubleType())));
    assertThat(
        avroSchema.getField("timestamp").schema(),
        equalTo(nullable(LogicalTypes.timestampMicros().addToSchema(typeBuilder().longType()))));
    assertThat(
        avroSchema.getField("numeric").schema(),
        equalTo(nullable(LogicalTypes.decimal(38, 9).addToSchema(typeBuilder().bytesType()))));
    assertThat(
        avroSchema.getField("bignumeric").schema(),
        equalTo(nullable(LogicalTypes.decimal(77, 38).addToSchema(typeBuilder().bytesType()))));
    assertThat(
        avroSchema.getField("bool").schema(), equalTo(nullable(typeBuilder().booleanType())));
    assertThat(avroSchema.getField("bytes").schema(), equalTo(nullable(typeBuilder().bytesType())));
    assertThat(
        avroSchema.getField("date").schema(),
        equalTo(nullable(typeBuilder().stringBuilder().prop("sqlType", "DATE").endString())));
    assertThat(
        avroSchema.getField("datetime").schema(),
        equalTo(nullable(typeBuilder().stringBuilder().prop("sqlType", "DATETIME").endString())));
    assertThat(
        avroSchema.getField("time").schema(),
        equalTo(nullable(typeBuilder().stringBuilder().prop("sqlType", "TIME").endString())));
    assertThat(
        avroSchema.getField("geography").schema(),
        equalTo(nullable(typeBuilder().stringBuilder().prop("sqlType", "GEOGRAPHY").endString())));
    assertThat(
        avroSchema.getField("record").schema(),
        equalTo(
            nullable(
                typeBuilder()
                    .record("record")
                    .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                    .doc("Translated Avro Schema for scion")
                    .fields()
                    .name("value")
                    .type(nullable(typeBuilder().stringType()))
                    .noDefault()
                    .endRecord())));
    assertThat(
        avroSchema.getField("multirecord").schema(),
        equalTo(
            array(
                typeBuilder()
                    .record("multirecord")
                    .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                    .doc("Translated Avro Schema for scion")
                    .fields()
                    .name("value")
                    .type(nullable(typeBuilder().stringType()))
                    .noDefault()
                    .endRecord())));
  }

  @Test
  public void testFormatTimestamp() {
    assertThat(
        BigQueryAvroUtils.formatTimestamp(1452062291123456L),
        equalTo("2016-01-06 06:38:11.123456 UTC"));
  }

  @Test
  public void testFormatTimestampLeadingZeroesOnMicros() {
    assertThat(
        BigQueryAvroUtils.formatTimestamp(1452062291000456L),
        equalTo("2016-01-06 06:38:11.000456 UTC"));
  }

  @Test
  public void testFormatTimestampTrailingZeroesOnMicros() {
    assertThat(
        BigQueryAvroUtils.formatTimestamp(1452062291123000L),
        equalTo("2016-01-06 06:38:11.123 UTC"));
  }

  @Test
  public void testFormatTimestampNegative() {
    assertThat(BigQueryAvroUtils.formatTimestamp(-1L), equalTo("1969-12-31 23:59:59.999999 UTC"));
    assertThat(
        BigQueryAvroUtils.formatTimestamp(-100_000L), equalTo("1969-12-31 23:59:59.900 UTC"));
    assertThat(BigQueryAvroUtils.formatTimestamp(-1_000_000L), equalTo("1969-12-31 23:59:59 UTC"));
    // No leap seconds before 1972. 477 leap years from 1 through 1969.
    assertThat(
        BigQueryAvroUtils.formatTimestamp(-(1969L * 365 + 477) * 86400 * 1_000_000),
        equalTo("0001-01-01 00:00:00 UTC"));
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

  /** Pojo class used as the record type in tests. */
  @SuppressWarnings("unused") // Used by Avro reflection.
  static class TestRecord {
    @AvroName("int")
    int integer;

    @AvroName("long")
    @Nullable
    Long ln;

    @Nullable String string;

    @AvroName("float")
    @Nullable
    Float flt;

    @AvroName("double")
    @Nullable
    Double dbl;

    @AvroSchema(value = "[\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]")
    Instant timestamp;

    @AvroSchema(
        value =
            "[\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}]")
    BigDecimal numeric;

    @AvroSchema(
        value =
            "[\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 77, \"scale\": 38}]")
    BigDecimal bignumeric;

    @AvroSchema(value = "[\"null\", {\"type\": \"string\", \"sqlType\": \"GEOGRAPHY\"}]")
    String geography;

    @Nullable Boolean bool;
    @Nullable ByteBuffer bytes;
    @Nullable Utf8 date;
    @Nullable String datetime;
    @Nullable Utf8 time;
    @Nullable SubRecord record;
    SubRecord[] multirecord;

    static class SubRecord {
      @Nullable String value;
    }

    public TestRecord() {
      multirecord = new SubRecord[1];
      multirecord[0] = new SubRecord();
    }
  }
}
