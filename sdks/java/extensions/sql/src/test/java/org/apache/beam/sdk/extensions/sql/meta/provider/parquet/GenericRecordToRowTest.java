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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

public class GenericRecordToRowTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema sc =
      SchemaBuilder.record("HandshakeRequest")
          .namespace("org.apache.avro.ipc")
          .fields()
          .name("clientProtocol")
          .type()
          .nullable()
          .stringType()
          .noDefault()
          .endRecord();

  org.apache.beam.sdk.schemas.Schema payloadSchema =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addNullableField("clientProtocol", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
          .build();

  GenericRecord g1 = new GenericRecordBuilder(sc).set("clientProtocol", "http").build();
  GenericRecord g2 = new GenericRecordBuilder(sc).set("clientProtocol", "ftp").build();

  @Test
  public void testConvertsGenericRecordToRow() {
    PCollection<Row> rows =
        pipeline
            .apply("create PCollection<GenericRecord>", Create.of(g1, g2).withCoder(AvroCoder.of(sc)))
            .apply("convert", new ParquetTableProvider.GenericRecordReadConverter());

    PAssert.that(rows).containsInAnyOrder(
            Row.withSchema(payloadSchema).addValues("ftp").build(),
            Row.withSchema(payloadSchema).addValues("http").build()
    );
    pipeline.run();
  }
}
