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

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BigQueryAvroUtils}.
 */
@RunWith(JUnit4.class)
public class BigQueryAvroUtilsTest {
  @Test
  public void testConvertGenericRecordToTableRow() throws Exception {
    TableSchema tableSchema = new TableSchema();
    List<TableFieldSchema> subFields = Lists.<TableFieldSchema>newArrayList(
        new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"));
    /*
     * Note that the quality and quantity fields do not have their mode set, so they should default
     * to NULLABLE. This is an important test of BigQuery semantics.
     *
     * All the other fields we set in this function are required on the Schema response.
     *
     * See https://cloud.google.com/bigquery/docs/reference/v2/tables#schema
     */
    List<TableFieldSchema> fields =
        Lists.<TableFieldSchema>newArrayList(
            new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED"),
            new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("quality").setType("FLOAT") /* default to NULLABLE */,
            new TableFieldSchema().setName("quantity").setType("INTEGER") /* default to NULLABLE */,
            new TableFieldSchema().setName("birthday").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("flighted").setType("BOOLEAN").setMode("NULLABLE"),
            new TableFieldSchema().setName("sound").setType("BYTES").setMode("NULLABLE"),
            new TableFieldSchema().setName("scion").setType("RECORD").setMode("NULLABLE")
                .setFields(subFields),
            new TableFieldSchema().setName("associates").setType("RECORD").setMode("REPEATED")
                .setFields(subFields));
    tableSchema.setFields(fields);
    Schema avroSchema = AvroCoder.of(Bird.class).getSchema();

    {
      // Test nullable fields.
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("number", "5")
          .set("associates", new ArrayList<TableRow>());
      assertEquals(row, convertedRow);
    }
    {
      // Test type conversion for TIMESTAMP, INTEGER, BOOLEAN, BYTES, and FLOAT.
      GenericRecord record = new GenericData.Record(avroSchema);
      byte[] soundBytes = "chirp,chirp".getBytes();
      ByteBuffer soundByteBuffer = ByteBuffer.allocate(soundBytes.length).put(soundBytes);
      soundByteBuffer.rewind();
      record.put("number", 5L);
      record.put("quality", 5.0);
      record.put("birthday", 5L);
      record.put("flighted", Boolean.TRUE);
      record.put("sound", soundByteBuffer);
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("number", "5")
          .set("birthday", "1970-01-01 00:00:00.000005 UTC")
          .set("quality", 5.0)
          .set("associates", new ArrayList<TableRow>())
          .set("flighted", Boolean.TRUE)
          .set("sound", BaseEncoding.base64().encode(soundBytes));
      assertEquals(row, convertedRow);
    }
    {
      // Test repeated fields.
      Schema subBirdSchema = AvroCoder.of(Bird.SubBird.class).getSchema();
      GenericRecord nestedRecord = new GenericData.Record(subBirdSchema);
      nestedRecord.put("species", "other");
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      record.put("associates", Lists.<GenericRecord>newArrayList(nestedRecord));
      TableRow convertedRow = BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("associates", Lists.<TableRow>newArrayList(
              new TableRow().set("species", "other")))
          .set("number", "5");
      assertEquals(row, convertedRow);
    }
  }

  /**
   * Pojo class used as the record type in tests.
   */
  @DefaultCoder(AvroCoder.class)
  @SuppressWarnings("unused")  // Used by Avro reflection.
  static class Bird {
    long number;
    @Nullable String species;
    @Nullable Double quality;
    @Nullable Long quantity;
    @Nullable Long birthday;  // Exercises TIMESTAMP.
    @Nullable Boolean flighted;
    @Nullable ByteBuffer sound;
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
