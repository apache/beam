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
package org.apache.beam.sdk.io.parquet;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link GenericRecordReadConverter}. */
public class GenericRecordToRowTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  org.apache.beam.sdk.schemas.Schema payloadSchema =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addField("name", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
          .addField("favorite_number", org.apache.beam.sdk.schemas.Schema.FieldType.INT32)
          .addField("favorite_color", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
          .addField("price", org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE)
          .build();

  @Test
  public void testConvertsGenericRecordToRow() {
    String schemaString =
        "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"User\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\", \"type\": \"int\"},\n"
            + "     {\"name\": \"favorite_color\", \"type\": \"string\"},\n"
            + "     {\"name\": \"price\", \"type\": \"double\"}\n"
            + " ]\n"
            + "}";
    Schema schema = (new Schema.Parser()).parse(schemaString);

    GenericRecord before = new GenericData.Record(schema);
    before.put("name", "Bob");
    before.put("favorite_number", 256);
    before.put("favorite_color", "red");
    before.put("price", 2.4);

    AvroCoder<GenericRecord> coder = AvroCoder.of(schema);

    PCollection<Row> rows =
        pipeline
            .apply("create PCollection<GenericRecord>", Create.of(before).withCoder(coder))
            .apply(
                "convert", GenericRecordReadConverter.builder().beamSchema(payloadSchema).build());

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(payloadSchema).addValues("Bob", 256, "red", 2.4).build());
    pipeline.run();
  }
}
