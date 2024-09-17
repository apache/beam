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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtils.DATETIME_LOGICAL_TYPE;
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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryAvroUtils}. */
@RunWith(JUnit4.class)
public class BigQueryAvroUtilsTest {
  private List<TableFieldSchema> subFields =
      Lists.newArrayList(
          new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"));
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
          new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED"),
          new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"),
          new TableFieldSchema().setName("quality").setType("FLOAT") /* default to NULLABLE */,
          new TableFieldSchema().setName("quantity").setType("INTEGER") /* default to NULLABLE */,
          new TableFieldSchema().setName("birthday").setType("TIMESTAMP").setMode("NULLABLE"),
          new TableFieldSchema().setName("birthdayMoney").setType("NUMERIC").setMode("NULLABLE"),
          new TableFieldSchema()
              .setName("lotteryWinnings")
              .setType("BIGNUMERIC")
              .setMode("NULLABLE"),
          new TableFieldSchema().setName("flighted").setType("BOOLEAN").setMode("NULLABLE"),
          new TableFieldSchema().setName("sound").setType("BYTES").setMode("NULLABLE"),
          new TableFieldSchema().setName("anniversaryDate").setType("DATE").setMode("NULLABLE"),
          new TableFieldSchema()
              .setName("anniversaryDatetime")
              .setType("DATETIME")
              .setMode("NULLABLE"),
          new TableFieldSchema().setName("anniversaryTime").setType("TIME").setMode("NULLABLE"),
          new TableFieldSchema()
              .setName("scion")
              .setType("RECORD")
              .setMode("NULLABLE")
              .setFields(subFields),
          new TableFieldSchema()
              .setName("associates")
              .setType("RECORD")
              .setMode("REPEATED")
              .setFields(subFields),
          new TableFieldSchema().setName("geoPositions").setType("GEOGRAPHY").setMode("NULLABLE"));

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
    Schema avroSchema = ReflectData.get().getSchema(Bird.class);

    {
      // Test nullable fields.
      GenericRecord record =
          new GenericRecordBuilder(avroSchema)
              .set("number", 5L)
              .set("associates", new ArrayList<GenericRecord>())
              .build();
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow row = new TableRow().set("number", "5").set("associates", new ArrayList<TableRow>());
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
    }
    {
      // Test type conversion for:
      // INTEGER, FLOAT, NUMERIC, TIMESTAMP, BOOLEAN, BYTES, DATE, DATETIME, TIME.
      byte[] soundBytes = "chirp,chirp".getBytes(StandardCharsets.UTF_8);
      GenericRecord record =
          new GenericRecordBuilder(avroSchema)
              .set("number", 5L)
              .set("quality", 5.0)
              .set("birthday", 5L)
              .set("birthdayMoney", numericBytes)
              .set("lotteryWinnings", bigNumericBytes)
              .set("flighted", Boolean.TRUE)
              .set("sound", ByteBuffer.wrap(soundBytes))
              .set("anniversaryDate", new Utf8("2000-01-01"))
              .set("anniversaryDatetime", "2000-01-01 00:00:00.000005")
              .set("anniversaryTime", new Utf8("00:00:00.000005"))
              .set("geoPositions", "LINESTRING(1 2, 3 4, 5 6, 7 8)")
              .set("associates", new ArrayList<GenericRecord>())
              .build();

      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow row =
          new TableRow()
              .set("anniversaryDate", "2000-01-01")
              .set("anniversaryDatetime", "2000-01-01 00:00:00.000005")
              .set("anniversaryTime", "00:00:00.000005")
              .set("associates", new ArrayList<TableRow>())
              .set("birthday", "1970-01-01 00:00:00.000005 UTC")
              .set("birthdayMoney", numeric.toString())
              .set("flighted", Boolean.TRUE)
              .set("geoPositions", "LINESTRING(1 2, 3 4, 5 6, 7 8)")
              .set("lotteryWinnings", bigNumeric.toString())
              .set("number", "5")
              .set("quality", 5.0)
              .set("sound", BaseEncoding.base64().encode(soundBytes));
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(clonedRow, convertedRow);
    }
    {
      // Test repeated fields.
      Schema subBirdSchema = ReflectData.get().getSchema(Bird.SubBird.class);
      GenericRecord nestedRecord =
          new GenericRecordBuilder(subBirdSchema).set("species", "other").build();
      GenericRecord record =
          new GenericRecordBuilder(avroSchema)
              .set("number", 5L)
              .set("associates", Lists.newArrayList(nestedRecord))
              .set("birthdayMoney", numericBytes)
              .set("lotteryWinnings", bigNumericBytes)
              .build();
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record);
      TableRow row =
          new TableRow()
              .set("associates", Lists.newArrayList(new TableRow().set("species", "other")))
              .set("number", "5")
              .set("birthdayMoney", numeric.toString())
              .set("lotteryWinnings", bigNumeric.toString());
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
    }
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchemaDisabledLogicalTypes() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    Schema avroSchema = BigQueryAvroUtils.toGenericAvroSchema(tableSchema, false);

    assertThat(avroSchema.getField("number").schema(), equalTo(SchemaBuilder.builder().longType()));
    assertThat(
        avroSchema.getField("species").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion()));
    assertThat(
        avroSchema.getField("quality").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().doubleType().endUnion()));
    assertThat(
        avroSchema.getField("quantity").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().longType().endUnion()));
    assertThat(
        avroSchema.getField("birthday").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(
                    LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("birthdayMoney").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.decimal(38, 9).addToSchema(SchemaBuilder.builder().bytesType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("lotteryWinnings").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.decimal(77, 38).addToSchema(SchemaBuilder.builder().bytesType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("flighted").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().booleanType().endUnion()));
    assertThat(
        avroSchema.getField("sound").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion()));
    assertThat(
        avroSchema.getField("anniversaryDate").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .stringBuilder()
                .prop("sqlType", "DATE")
                .endString()
                .endUnion()));
    assertThat(
        avroSchema.getField("anniversaryDatetime").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .stringBuilder()
                .prop("sqlType", "DATETIME")
                .endString()
                .endUnion()));
    assertThat(
        avroSchema.getField("anniversaryTime").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .stringBuilder()
                .prop("sqlType", "TIME")
                .endString()
                .endUnion()));
    assertThat(
        avroSchema.getField("geoPositions").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .stringBuilder()
                .prop("sqlType", "GEOGRAPHY")
                .endString()
                .endUnion()));
    assertThat(
        avroSchema.getField("scion").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .record("scion")
                .doc("Translated Avro Schema for scion")
                .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                .fields()
                .name("species")
                .type()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .noDefault()
                .endRecord()
                .endUnion()));

    assertThat(
        avroSchema.getField("associates").schema(),
        equalTo(
            SchemaBuilder.array()
                .items()
                .record("associates")
                .doc("Translated Avro Schema for associates")
                .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                .fields()
                .name("species")
                .type()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .noDefault()
                .endRecord()));
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchema() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    Schema avroSchema = BigQueryAvroUtils.toGenericAvroSchema(tableSchema);

    assertThat(avroSchema.getField("number").schema(), equalTo(SchemaBuilder.builder().longType()));
    assertThat(
        avroSchema.getField("species").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion()));
    assertThat(
        avroSchema.getField("quality").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().doubleType().endUnion()));
    assertThat(
        avroSchema.getField("quantity").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().longType().endUnion()));
    assertThat(
        avroSchema.getField("birthday").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(
                    LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("birthdayMoney").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.decimal(38, 9).addToSchema(SchemaBuilder.builder().bytesType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("lotteryWinnings").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.decimal(77, 38).addToSchema(SchemaBuilder.builder().bytesType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("flighted").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().booleanType().endUnion()));
    assertThat(
        avroSchema.getField("sound").schema(),
        equalTo(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion()));
    assertThat(
        avroSchema.getField("anniversaryDate").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("anniversaryDatetime").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(DATETIME_LOGICAL_TYPE.addToSchema(SchemaBuilder.builder().stringType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("anniversaryTime").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .type(LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()))
                .endUnion()));
    assertThat(
        avroSchema.getField("geoPositions").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .stringBuilder()
                .prop("sqlType", "GEOGRAPHY")
                .endString()
                .endUnion()));
    assertThat(
        avroSchema.getField("scion").schema(),
        equalTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .record("scion")
                .doc("Translated Avro Schema for scion")
                .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                .fields()
                .name("species")
                .type()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .noDefault()
                .endRecord()
                .endUnion()));

    assertThat(
        avroSchema.getField("associates").schema(),
        equalTo(
            SchemaBuilder.array()
                .items()
                .record("associates")
                .doc("Translated Avro Schema for associates")
                .namespace("org.apache.beam.sdk.io.gcp.bigquery")
                .fields()
                .name("species")
                .type()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .noDefault()
                .endRecord()));
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
        equalTo("2016-01-06 06:38:11.123000 UTC"));
  }

  @Test
  public void testFormatTimestampNegative() {
    assertThat(BigQueryAvroUtils.formatTimestamp(-1L), equalTo("1969-12-31 23:59:59.999999 UTC"));
    assertThat(
        BigQueryAvroUtils.formatTimestamp(-100_000L), equalTo("1969-12-31 23:59:59.900000 UTC"));
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
  static class Bird {
    long number;
    @Nullable String species;
    @Nullable Double quality;
    @Nullable Long quantity;

    @AvroSchema(value = "[\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]")
    Instant birthday;

    @AvroSchema(
        value =
            "[\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}]")
    BigDecimal birthdayMoney;

    @AvroSchema(
        value =
            "[\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 77, \"scale\": 38}]")
    BigDecimal lotteryWinnings;

    @AvroSchema(value = "[\"null\", {\"type\": \"string\", \"sqlType\": \"GEOGRAPHY\"}]")
    String geoPositions;

    @Nullable Boolean flighted;
    @Nullable ByteBuffer sound;
    @Nullable Utf8 anniversaryDate;
    @Nullable String anniversaryDatetime;
    @Nullable Utf8 anniversaryTime;
    @Nullable SubBird scion;
    SubBird[] associates;

    static class SubBird {
      @Nullable String species;

      public SubBird() {}
    }

    public Bird() {
      associates = new SubBird[1];
      associates[0] = new SubBird();
    }
  }
}
