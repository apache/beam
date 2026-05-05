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

import java.util.Collections;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
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

/** Tests for {@link MongoDbWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class MongoDbWriteSchemaTransformProviderTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testInvalidConfigMissingUri() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbWriteSchemaTransformConfiguration.builder()
              .setDatabase("db")
              .setCollection("col")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigMissingDatabase() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbWriteSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setCollection("col")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigMissingCollection() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          MongoDbWriteSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setDatabase("db")
              .build()
              .validate();
        });
  }

  @Test
  public void testInvalidConfigNegativeBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MongoDbWriteSchemaTransformConfiguration.builder()
              .setUri("mongodb://localhost:27017")
              .setDatabase("db")
              .setCollection("col")
              .setBatchSize(-1L)
              .build()
              .validate();
        });
  }

  @Test
  public void testConfigurationSchema() throws Exception {
    Schema schema =
        SchemaRegistry.createDefault().getSchema(MongoDbWriteSchemaTransformConfiguration.class);

    // We expect 11 fields now after adding errorHandling
    assertEquals(11, schema.getFieldCount());
    assertNotNull(schema.getField("uri"));
    assertNotNull(schema.getField("database"));
    assertNotNull(schema.getField("collection"));
    assertNotNull(schema.getField("batchSize"));
    assertNotNull(schema.getField("ordered"));
  }

  @Test
  public void testRowToBsonDocumentFn() {
    Schema beamSchema =
        Schema.builder()
            .addStringField("name")
            .addInt32Field("age")
            .addNullableStringField("country")
            .build();

    Row row =
        Row.withSchema(beamSchema)
            .withFieldValue("name", "John")
            .withFieldValue("age", 30)
            .withFieldValue("country", null)
            .build();

    PCollection<Row> inputRows =
        p.apply(Create.of(Collections.singletonList(row))).setRowSchema(beamSchema);

    Schema errorSchema = ErrorHandling.errorSchema(beamSchema);
    PCollectionTuple outputTuple =
        inputRows.apply(
            "ConvertToDocument",
            ParDo.of(
                    new MongoDbWriteSchemaTransformProvider.RowToBsonDocumentFn(false, errorSchema))
                .withOutputTags(
                    MongoDbWriteSchemaTransformProvider.OUTPUT_TAG,
                    TupleTagList.of(MongoDbWriteSchemaTransformProvider.ERROR_TAG)));

    PCollection<Document> bsonDocuments =
        outputTuple.get(MongoDbWriteSchemaTransformProvider.OUTPUT_TAG);

    outputTuple.get(MongoDbWriteSchemaTransformProvider.ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(bsonDocuments)
        .satisfies(
            documents -> {
              Document doc = documents.iterator().next();
              assertEquals("John", doc.get("name"));
              assertEquals(30, doc.get("age"));
              // The RowToBsonDocumentFn retains nulls explicitly in the BSON document
              assertEquals(null, doc.get("country"));
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Test
  public void testUpdateConfigurationSchema() throws NoSuchSchemaException {
    Schema schema = SchemaRegistry.createDefault().getSchema(MongoDbUpdateConfiguration.class);
    assertNotNull(schema);
    assertEquals(4, schema.getFieldCount());
    assertNotNull(schema.getField("findKey"));
    assertNotNull(schema.getField("updateKey"));
    assertNotNull(schema.getField("isUpsert"));
    assertNotNull(schema.getField("updateFields"));
  }

  @Test
  public void testBuildTransformWithUpdateConfiguration() {
    MongoDbUpdateField field =
        MongoDbUpdateField.builder()
            .setUpdateOperator("$set")
            .setDestField("name")
            .setSourceField("src_name")
            .build();

    MongoDbUpdateConfiguration updateConfig =
        MongoDbUpdateConfiguration.builder()
            .setFindKey("id")
            .setIsUpsert(true)
            .setUpdateFields(Collections.singletonList(field))
            .build();

    MongoDbWriteSchemaTransformConfiguration config =
        MongoDbWriteSchemaTransformConfiguration.builder()
            .setUri("mongodb://localhost:27017")
            .setDatabase("db")
            .setCollection("col")
            .setUpdateConfiguration(updateConfig)
            .build();

    SchemaTransform transform = new MongoDbWriteSchemaTransformProvider().from(config);
    assertNotNull(transform);

    // Verify that expand executes without mapping errors
    Schema inputSchema = Schema.builder().addStringField("id").addStringField("src_name").build();
    PCollection<Row> input = p.apply(Create.empty(inputSchema));
    PCollectionRowTuple.of("input", input).apply(transform);

    p.run();
  }
}
