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
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
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

  @Test
  public void testConvertGenericRecordToTableRow() throws Exception {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);

    // BigQuery encodes NUMERIC values to Avro using the BYTES type with the DECIMAL logical
    // type. AvroCoder can't apply logical types to Schemas directly, so we need to get the
    // Schema for the Bird class defined below, then replace the field used to test NUMERIC with
    // a field that has the appropriate Schema.
    BigDecimal birthdayMoney = new BigDecimal("123456789.123456789");
    Schema birthdayMoneySchema = Schema.create(Type.BYTES);
    LogicalType birthdayMoneyLogicalType =
        LogicalTypes.decimal(birthdayMoney.precision(), birthdayMoney.scale());
    // DecimalConversion.toBytes returns a ByteBuffer, which can be mutated by callees if passed
    // to other methods. We wrap the byte array as a ByteBuffer when adding it to the
    // GenericRecords below.
    byte[] birthdayMoneyBytes =
        new Conversions.DecimalConversion()
            .toBytes(birthdayMoney, birthdayMoneySchema, birthdayMoneyLogicalType)
            .array();

    // In order to update the Schema for birthdayMoney, we need to recreate all of the Fields.
    List<Schema.Field> avroFields = new ArrayList<>();
    for (Schema.Field field : AvroCoder.of(Bird.class).getSchema().getFields()) {
      Schema schema = field.schema();
      if ("birthdayMoney".equals(field.name())) {
        // birthdayMoney is a nullable field with type BYTES/DECIMAL.
        schema =
            Schema.createUnion(
                Schema.create(Type.NULL),
                birthdayMoneyLogicalType.addToSchema(birthdayMoneySchema));
      }
      // After a Field is added to a Schema, it is assigned a position, so we can't simply reuse
      // the existing Field.
      avroFields.add(new Schema.Field(field.name(), schema, field.doc(), field.defaultValue()));
    }
    Schema avroSchema = Schema.createRecord(avroFields);

    {
      // Test nullable fields.
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow().set("number", "5").set("associates", new ArrayList<TableRow>());
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
    }
    {
      // Test type conversion for:
      // INTEGER, FLOAT, NUMERIC, TIMESTAMP, BOOLEAN, BYTES, DATE, DATETIME, TIME.
      GenericRecord record = new GenericData.Record(avroSchema);
      byte[] soundBytes = "chirp,chirp".getBytes(StandardCharsets.UTF_8);
      ByteBuffer soundByteBuffer = ByteBuffer.wrap(soundBytes);
      soundByteBuffer.rewind();
      record.put("number", 5L);
      record.put("quality", 5.0);
      record.put("birthday", 5L);
      record.put("birthdayMoney", ByteBuffer.wrap(birthdayMoneyBytes));
      record.put("flighted", Boolean.TRUE);
      record.put("sound", soundByteBuffer);
      record.put("anniversaryDate", new Utf8("2000-01-01"));
      record.put("anniversaryDatetime", new String("2000-01-01 00:00:00.000005"));
      record.put("anniversaryTime", new Utf8("00:00:00.000005"));
      record.put("geoPositions", new String("LINESTRING(1 2, 3 4, 5 6, 7 8)"));
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row =
          new TableRow()
              .set("number", "5")
              .set("birthday", "1970-01-01 00:00:00.000005 UTC")
              .set("birthdayMoney", birthdayMoney.toString())
              .set("quality", 5.0)
              .set("associates", new ArrayList<TableRow>())
              .set("flighted", Boolean.TRUE)
              .set("sound", BaseEncoding.base64().encode(soundBytes))
              .set("anniversaryDate", "2000-01-01")
              .set("anniversaryDatetime", "2000-01-01 00:00:00.000005")
              .set("anniversaryTime", "00:00:00.000005")
              .set("geoPositions", "LINESTRING(1 2, 3 4, 5 6, 7 8)");
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
      assertEquals(row, convertedRow);
    }
    {
      // Test repeated fields.
      Schema subBirdSchema = AvroCoder.of(Bird.SubBird.class).getSchema();
      GenericRecord nestedRecord = new GenericData.Record(subBirdSchema);
      nestedRecord.put("species", "other");
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      record.put("associates", Lists.newArrayList(nestedRecord));
      record.put("birthdayMoney", ByteBuffer.wrap(birthdayMoneyBytes));
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row =
          new TableRow()
              .set("associates", Lists.newArrayList(new TableRow().set("species", "other")))
              .set("number", "5")
              .set("birthdayMoney", birthdayMoney.toString());
      assertEquals(row, convertedRow);
      TableRow clonedRow = convertedRow.clone();
      assertEquals(convertedRow, clonedRow);
    }
  }

  @Test
  public void testConvertBigQuerySchemaToAvroSchema() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    Schema avroSchema =
        BigQueryAvroUtils.toGenericAvroSchema("testSchema", tableSchema.getFields());

    assertThat(avroSchema.getField("number").schema(), equalTo(Schema.create(Type.LONG)));
    assertThat(
        avroSchema.getField("species").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING))));
    assertThat(
        avroSchema.getField("quality").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.DOUBLE))));
    assertThat(
        avroSchema.getField("quantity").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG))));
    assertThat(
        avroSchema.getField("birthday").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG))));
    assertThat(
        avroSchema.getField("birthdayMoney").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BYTES))));
    assertThat(
        avroSchema.getField("flighted").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BOOLEAN))));
    assertThat(
        avroSchema.getField("sound").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BYTES))));
    assertThat(
        avroSchema.getField("anniversaryDate").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING))));
    assertThat(
        avroSchema.getField("anniversaryDatetime").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING))));
    assertThat(
        avroSchema.getField("anniversaryTime").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING))));
    assertThat(
        avroSchema.getField("geoPositions").schema(),
        equalTo(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING))));

    assertThat(
        avroSchema.getField("scion").schema(),
        equalTo(
            Schema.createUnion(
                Schema.create(Type.NULL),
                Schema.createRecord(
                    "scion",
                    "Translated Avro Schema for scion",
                    "org.apache.beam.sdk.io.gcp.bigquery",
                    false,
                    ImmutableList.of(
                        new Field(
                            "species",
                            Schema.createUnion(
                                Schema.create(Type.NULL), Schema.create(Type.STRING)),
                            null,
                            (Object) null))))));
    assertThat(
        avroSchema.getField("associates").schema(),
        equalTo(
            Schema.createArray(
                Schema.createRecord(
                    "associates",
                    "Translated Avro Schema for associates",
                    "org.apache.beam.sdk.io.gcp.bigquery",
                    false,
                    ImmutableList.of(
                        new Field(
                            "species",
                            Schema.createUnion(
                                Schema.create(Type.NULL), Schema.create(Type.STRING)),
                            null,
                            (Object) null))))));
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

  /** Pojo class used as the record type in tests. */
  @DefaultCoder(AvroCoder.class)
  @SuppressWarnings("unused") // Used by Avro reflection.
  static class Bird {
    long number;
    @Nullable String species;
    @Nullable Double quality;
    @Nullable Long quantity;
    @Nullable Long birthday; // Exercises TIMESTAMP.
    @Nullable ByteBuffer birthdayMoney; // Exercises NUMERIC.
    @Nullable String geoPositions; // Exercises GEOGRAPHY.
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
