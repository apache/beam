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
package org.apache.beam.sdk.io.datadog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.schemas.AutoValueSchema;
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
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class DatadogWriteSchemaTransformProviderTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatadogWriteSchemaTransformProviderTest.class);

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static final Schema SCHEMA =
      Schema.builder()
          .addStringField("ddsource")
          .addNullableField("ddtags", Schema.FieldType.STRING)
          .addStringField("hostname")
          .addNullableField("service", Schema.FieldType.STRING)
          .addStringField("message")
          .build();

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA)
              .withFieldValue("ddsource", "my-source")
              .withFieldValue("ddtags", "tag1:value1,tag2")
              .withFieldValue("hostname", "my-host")
              .withFieldValue("service", "my-service")
              .withFieldValue("message", "Hello World 1")
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("ddsource", "my-source-2")
              .withFieldValue("ddtags", null)
              .withFieldValue("hostname", "my-host-2")
              .withFieldValue("service", null)
              .withFieldValue("message", "Hello World 2")
              .build());

  private List<DatadogEvent> events;

  @org.junit.Before
  public void setUp() {
    events =
        Arrays.asList(
            DatadogEvent.newBuilder()
                .withSource("my-source")
                .withTags("tag1:value1,tag2")
                .withHostname("my-host")
                .withService("my-service")
                .withMessage("Hello World 1")
                .build(),
            DatadogEvent.newBuilder()
                .withSource("my-source-2")
                .withHostname("my-host-2")
                .withMessage("Hello World 2")
                .build());
  }

  @Test
  public void testRowToDatadogEventFn() throws NonDeterministicException {
    PCollection<Row> input = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));

    PCollection<DatadogEvent> output =
        input.apply(
            "RowToDatadogEvent",
            ParDo.of(new DatadogWriteSchemaTransformProvider.RowToDatadogEventFn()));

    output.setCoder(DatadogEventCoder.of());

    PAssert.that(output).containsInAnyOrder(events);
    p.run().waitUntilFinish();
  }

  @Test
  public void testRowToDatadogEventFnWithMissingFields() throws NonDeterministicException {
    Schema missingFieldsSchema =
        Schema.builder()
            .addStringField("ddsource")
            .addStringField("hostname")
            .addStringField("message")
            .build();

    Row row =
        Row.withSchema(missingFieldsSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("hostname", "my-host")
            .withFieldValue("message", "Hello World 1")
            .build();

    DatadogEvent expectedEvent =
        DatadogEvent.newBuilder()
            .withSource("my-source")
            .withHostname("my-host")
            .withMessage("Hello World 1")
            .build();

    PCollection<Row> input = p.apply(Create.of(row).withRowSchema(missingFieldsSchema));
    PCollection<DatadogEvent> output =
        input.apply(
            "RowToDatadogEvent",
            ParDo.of(new DatadogWriteSchemaTransformProvider.RowToDatadogEventFn()));

    output.setCoder(DatadogEventCoder.of());

    PAssert.that(output).containsInAnyOrder(expectedEvent);
    p.run().waitUntilFinish();
  }

  @Test
  public void testRowToDatadogEventFnWithExtraFields() throws NonDeterministicException {
    Schema extraFieldsSchema =
        Schema.builder()
            .addStringField("ddsource")
            .addNullableField("ddtags", Schema.FieldType.STRING)
            .addStringField("hostname")
            .addNullableField("service", Schema.FieldType.STRING)
            .addStringField("message")
            .addStringField("extra_field")
            .build();

    Row row =
        Row.withSchema(extraFieldsSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("ddtags", "tag1:value1,tag2")
            .withFieldValue("hostname", "my-host")
            .withFieldValue("service", "my-service")
            .withFieldValue("message", "Hello World 1")
            .withFieldValue("extra_field", "extra_value")
            .build();

    DatadogEvent expectedEvent =
        DatadogEvent.newBuilder()
            .withSource("my-source")
            .withTags("tag1:value1,tag2")
            .withHostname("my-host")
            .withService("my-service")
            .withMessage("Hello World 1")
            .build();

    PCollection<Row> input = p.apply(Create.of(row).withRowSchema(extraFieldsSchema));
    PCollection<DatadogEvent> output =
        input.apply(
            "RowToDatadogEvent",
            ParDo.of(new DatadogWriteSchemaTransformProvider.RowToDatadogEventFn()));

    output.setCoder(DatadogEventCoder.of());

    PAssert.that(output).containsInAnyOrder(expectedEvent);
    p.run().waitUntilFinish();
  }

  @Test
  public void testBuildTransform() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .build();

    SchemaTransform transform = provider.from(configuration);

    PCollection<Row> input = p.apply("CreateEmpty", Create.empty(SCHEMA));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of("input", input);
    transform.expand(inputTuple);

    p.run().waitUntilFinish();
  }

  @Test
  public void testBuildTransformWithAllParameters() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .setBatchCount(10)
            .setMaxBufferSize(100L)
            .setParallelism(2)
            .build();

    SchemaTransform transform = provider.from(configuration);

    PCollection<Row> input = p.apply("Create", Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of("input", input);
    transform.expand(inputTuple);

    p.run().waitUntilFinish();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildTransformMissingUrl() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    provider.from(
        DatadogWriteSchemaTransformConfiguration.builder().setApiKey("test-api-key").build());
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildTransformMissingApiKey() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    provider.from(DatadogWriteSchemaTransformConfiguration.builder().setUrl("test-url").build());
  }

  @Test
  public void testConfigurationSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerSchemaProvider(ErrorHandling.class, new AutoValueSchema());
    Schema schema = registry.getSchema(DatadogWriteSchemaTransformConfiguration.class);
    Schema errorHandlingSchema = registry.getSchema(ErrorHandling.class);

    LOG.info(schema.getFieldNames().toString());

    assertEquals(6, schema.getFieldCount());
    assertTrue(schema.hasField("url"));
    assertTrue(schema.hasField("apiKey"));
    assertTrue(schema.hasField("batchCount"));
    assertTrue(schema.hasField("maxBufferSize"));
    assertTrue(schema.hasField("parallelism"));
    assertTrue(schema.hasField("errorHandling"));

    LOG.info(schema.getField("url").getType().getTypeName().toString());
    assertEquals(Schema.FieldType.STRING.withNullable(false), schema.getField("url").getType());
    assertEquals(Schema.FieldType.STRING.withNullable(false), schema.getField("apiKey").getType());
    assertEquals(
        Schema.FieldType.INT32.withNullable(true), schema.getField("batchCount").getType());
    assertEquals(
        Schema.FieldType.INT64.withNullable(true), schema.getField("maxBufferSize").getType());
    assertEquals(
        Schema.FieldType.INT32.withNullable(true), schema.getField("parallelism").getType());
    assertEquals(
        Schema.FieldType.row(errorHandlingSchema).withNullable(true),
        schema.getField("errorHandling").getType());
  }
}
