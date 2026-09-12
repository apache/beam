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
package org.apache.beam.sdk.io.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MongoDbReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class MongoDbReadSchemaTransformProviderTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testInvalidConfigMissingUri() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbReadSchemaTransformConfiguration.builder()
              .setDatabase("db")
              .setCollection("col")
              .setSchema("{}")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigMissingDatabase() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbReadSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setCollection("col")
              .setSchema("{}")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigMissingCollection() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbReadSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setDatabase("db")
              .setSchema("{}")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigMissingSchema() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbReadSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setDatabase("db")
              .setCollection("col")
              .build()
              .validate();
        });
  }

  @Test
  public void testConfigurationSchema() throws Exception {
    Schema schema =
        SchemaRegistry.createDefault().getSchema(MongoDbReadSchemaTransformConfiguration.class);

    // We expect 6 fields: uri, database, collection, schema, filter, errorHandling
    assertEquals(6, schema.getFieldCount());
    assertNotNull(schema.getField("uri"));
    assertNotNull(schema.getField("database"));
    assertNotNull(schema.getField("collection"));
    assertNotNull(schema.getField("schema"));
    assertNotNull(schema.getField("filter"));
    assertNotNull(schema.getField("errorHandling"));
  }

  @Test
  public void testExpandWithFilter() {
    MongoDbReadSchemaTransformConfiguration config =
        MongoDbReadSchemaTransformConfiguration.builder()
            .setUri("mongodb://localhost:27017")
            .setDatabase("db")
            .setCollection("col")
            .setSchema("{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}}")
            .setFilter("{\"name\": \"John\"}")
            .build();

    MongoDbReadSchemaTransformProvider provider = new MongoDbReadSchemaTransformProvider();
    PCollectionRowTuple output =
        provider.from(config).expand(PCollectionRowTuple.empty(Pipeline.create()));

    assertNotNull(output.get("output"));
  }

  @Test
  public void testDocumentToRowFn() {
    Schema beamSchema = Schema.builder().addStringField("name").addInt32Field("age").build();

    Document doc = new Document().append("name", "John").append("age", 30);

    PCollection<Document> inputDocs =
        p.apply(
            Create.of(Collections.singletonList(doc))
                .withCoder(MongoDbWriteSchemaTransformProvider.DocumentCoder.of()));

    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollectionTuple outputTuple =
        inputDocs.apply(
            "ConvertToRows",
            ParDo.of(
                    new MongoDbReadSchemaTransformProvider.DocumentToRowFn(
                        beamSchema, false, errorSchema))
                .withOutputTags(
                    MongoDbReadSchemaTransformProvider.OUTPUT_TAG,
                    TupleTagList.of(MongoDbReadSchemaTransformProvider.ERROR_TAG)));

    PCollection<Row> outputRows =
        outputTuple.get(MongoDbReadSchemaTransformProvider.OUTPUT_TAG).setRowSchema(beamSchema);
    outputTuple.get(MongoDbReadSchemaTransformProvider.ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(outputRows)
        .satisfies(
            rows -> {
              Row row = rows.iterator().next();
              assertEquals("John", row.getString("name"));
              assertEquals(Integer.valueOf(30), row.getInt32("age"));
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Test
  public void testDocumentToRowFnWithErrors() {
    Schema beamSchema = Schema.builder().addInt32Field("age").build();

    // Invalid document: age value is a string "not_an_int" which cannot be converted to INT32
    Document invalidDoc = new Document().append("age", "not_an_int");

    PCollection<Document> inputDocs =
        p.apply(
            Create.of(Collections.singletonList(invalidDoc))
                .withCoder(MongoDbWriteSchemaTransformProvider.DocumentCoder.of()));

    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollectionTuple outputTuple =
        inputDocs.apply(
            "ConvertToRowsWithErrors",
            ParDo.of(
                    new MongoDbReadSchemaTransformProvider.DocumentToRowFn(
                        beamSchema, true, errorSchema))
                .withOutputTags(
                    MongoDbReadSchemaTransformProvider.OUTPUT_TAG,
                    TupleTagList.of(MongoDbReadSchemaTransformProvider.ERROR_TAG)));

    PCollection<Row> errorRows =
        outputTuple.get(MongoDbReadSchemaTransformProvider.ERROR_TAG).setRowSchema(errorSchema);
    PCollection<Row> outputRows =
        outputTuple.get(MongoDbReadSchemaTransformProvider.OUTPUT_TAG).setRowSchema(beamSchema);

    PAssert.that(outputRows).empty();
    PAssert.that(errorRows)
        .satisfies(
            rows -> {
              Row errorRow = rows.iterator().next();
              byte[] failedRowBytes = errorRow.getBytes("failed_row");
              String failedJson = new String(failedRowBytes, StandardCharsets.UTF_8);
              String errMsg = errorRow.getString("error_message");
              assertNotNull(failedJson);
              assertNotNull(errMsg);
              return null;
            });

    p.run().waitUntilFinish();
  }
}
