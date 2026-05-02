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
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
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

  @Rule public TestPipeline p = TestPipeline.create();

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
  public void testWriteInvalidConfigurations() {

    // apiKey not set
    assertThrows(
        IllegalStateException.class,
        () -> {
          DatadogWriteSchemaTransformConfiguration.builder()
              .setUrl("http://localhost:8080")
              //   .setApiKey("test-api-key") # ApiKey is mandatory
              .build()
              .validate();
        });

    // url not set
    assertThrows(
        IllegalStateException.class,
        () -> {
          DatadogWriteSchemaTransformConfiguration.builder()
              //   .setUrl("http://localhost:8080") # Url is mandatory
              .setApiKey("test-api-key")
              .build()
              .validate();
        });
  }

  @Test
  public void testWriteBuildTransform() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .build();

    provider.from(configuration);
  }

  @Test
  public void testWriteBuildTransformAndRun() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .build();

    SchemaTransform transform = provider.from(configuration);

    PCollection<Row> input = p.apply("Create", Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);
    PCollectionRowTuple output = transform.expand(inputTuple);
    assertTrue(output.getAll().isEmpty());

    try {
      p.run().waitUntilFinish();
      fail("Expected a PipelineExecutionException due to connection refusal.");
    } catch (PipelineExecutionException e) {
      Throwable cause = e.getCause();
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(
          "Expected cause to be java.net.ConnectException",
          cause instanceof java.net.ConnectException);
    }
  }

  @Test
  public void testWriteBuildTransformWithCorrectFields() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == DatadogWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider datadogProvider = providers.get(0);
    assertEquals(datadogProvider.outputCollectionNames(), Lists.newArrayList(OUTPUT, ERROR));

    assertEquals(
        Sets.newHashSet(
            "url",
            "api_key",
            "min_batch_count",
            "batch_count",
            "max_buffer_size",
            "parallelism",
            "error_handling"),
        datadogProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testRowToDatadogEvent() {
    for (int i = 0; i < ROWS.size(); i++) {
      DatadogEvent actual = DatadogWriteSchemaTransformProvider.rowToEvent(ROWS.get(i));
      assertEquals(events.get(i), actual);
    }
  }

  @Test
  public void testRowToDatadogEventWithMissingOptionalFields() {
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

    DatadogEvent actual = DatadogWriteSchemaTransformProvider.rowToEvent(row);
    assertEquals(expectedEvent, actual);
  }

  @Test
  public void testRowToDatadogEventWithExtraFields_DiscardsExtraFields() {
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

    DatadogEvent actual = DatadogWriteSchemaTransformProvider.rowToEvent(row);
    assertEquals(expectedEvent, actual);
  }

  @Test(expected = NullPointerException.class)
  public void testRowToDatadogEventWithNullRequiredField() {
    Schema nullSchema =
        Schema.builder()
            .addStringField("ddsource")
            .addNullableField("ddtags", Schema.FieldType.STRING)
            .addStringField("hostname")
            .addNullableField("service", Schema.FieldType.STRING)
            .addNullableField("message", Schema.FieldType.STRING)
            .build();

    Row row =
        Row.withSchema(nullSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("ddtags", "tag1:value1,tag2")
            .withFieldValue("hostname", "my-host")
            .withFieldValue("service", "my-service")
            .withFieldValue("message", null)
            .build();

    DatadogWriteSchemaTransformProvider.rowToEvent(row);
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
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);
    PCollectionRowTuple output = transform.expand(inputTuple);
    assertTrue(output.getAll().isEmpty());

    try {
      p.run().waitUntilFinish();
      fail("Expected a PipelineExecutionException due to connection refusal.");
    } catch (PipelineExecutionException e) {
      Throwable cause = e.getCause();
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(
          "Expected cause to be java.net.ConnectException",
          cause instanceof java.net.ConnectException);
    }
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
  public void testBuildTransformWithInvalidParallelism() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .setParallelism(0)
            .build();

    SchemaTransform transform = provider.from(configuration);

    PCollection<Row> input = p.apply("Create", Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);
    PCollectionRowTuple output = transform.expand(inputTuple);
    assertTrue(output.getAll().isEmpty());

    try {
      p.run().waitUntilFinish();
      fail("Expected a PipelineExecutionException to be thrown.");
    } catch (PipelineExecutionException e) {
      Throwable cause = e.getCause();
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(
          "Expected cause to be of type IllegalArgumentException",
          cause instanceof IllegalArgumentException);
      assertTrue(
          "Expected message to contain 'bound must be positive'",
          cause.getMessage().contains("bound must be positive"));
    }
  }

  @Test
  public void testBuildTransformWithInvalidBatchCount() {
    // DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    // DatadogWriteSchemaTransformConfiguration configuration =
    DatadogWriteSchemaTransformConfiguration.builder()
        .setApiKey("test-api-key")
        .setUrl("http://localhost:8080")
        .setBatchCount(0)
        .setMinBatchCount(1)
        .build()
        .validate();

    // SchemaTransform transform = provider.from(configuration);

    // PCollection<Row> input = p.apply("Create", Create.of(ROWS).withRowSchema(SCHEMA));
    // PCollectionRowTuple inputTuple = PCollectionRowTuple.of("input", input);
    // try {
    //   transform.expand(inputTuple);
    //   fail("Expected an IllegalArgumentException to be thrown.");
    // } catch (IllegalArgumentException e) {
    //   assertTrue(
    //       "Expected message to contain 'inputBatchCount must be greater than or equal to 1'",
    //       e.getMessage().contains("inputBatchCount must be greater than or equal to 1"));
    // }
  }

  @Test
  public void testBuildTransformFromRowConfiguration() throws NoSuchSchemaException {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    Schema configSchema = provider.configurationSchema();
    Schema errorHandlingSchema = configSchema.getField("error_handling").getType().getRowSchema();

    Row errorHandlingRow =
        Row.withSchema(errorHandlingSchema).withFieldValue(OUTPUT, ERROR).build();

    Row configRow =
        Row.withSchema(configSchema)
            .withFieldValue("url", "http://localhost:8080")
            .withFieldValue("api_key", "test-api-key")
            .withFieldValue("min_batch_count", null)
            .withFieldValue("batch_count", 10)
            .withFieldValue("max_buffer_size", 100L)
            .withFieldValue("parallelism", 2)
            .withFieldValue("error_handling", errorHandlingRow)
            .build();

    SchemaTransform transform = provider.from(configRow);

    PCollection<Row> input = p.apply("Create", Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);
    PCollectionRowTuple output = transform.expand(inputTuple);
    assertEquals(1, output.getAll().size());
    assertTrue(output.has(ERROR));

    p.run().waitUntilFinish();
  }

  @Test(expected = ClassCastException.class)
  public void testRowToDatadogEventWithWrongType() {
    Schema wrongSchema =
        Schema.builder()
            .addStringField("ddsource")
            .addInt64Field("ddtags")
            .addStringField("hostname")
            .addStringField("message")
            .build();

    Row row =
        Row.withSchema(wrongSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("ddtags", 123L)
            .withFieldValue("hostname", "my-host")
            .withFieldValue("message", "Hello World 1")
            .build();

    DatadogWriteSchemaTransformProvider.rowToEvent(row);
  }

  @Test
  public void testErrorHandling() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    ErrorHandling errorHandling = ErrorHandling.builder().setOutput(ERROR).build();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .setBatchCount(10)
            .setMaxBufferSize(1L)
            .setParallelism(1)
            .setErrorHandling(errorHandling)
            .build();

    Schema nullSchema =
        Schema.builder()
            .addStringField("ddsource")
            .addNullableField("ddtags", Schema.FieldType.STRING)
            .addStringField("hostname")
            .addNullableField("service", Schema.FieldType.STRING)
            .addNullableField("message", Schema.FieldType.STRING)
            .build();

    Row row =
        Row.withSchema(nullSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("ddtags", "tag1:value1,tag2")
            .withFieldValue("hostname", "my-host")
            .withFieldValue("service", "my-service")
            .withFieldValue("message", null)
            .build();

    PCollection<Row> input = p.apply(Create.of(row).withRowSchema(nullSchema));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);

    SchemaTransform transform = provider.from(configuration);
    PCollectionRowTuple outputTuple = transform.expand(inputTuple);

    assertTrue(outputTuple.has(ERROR));
    PAssert.that(outputTuple.get(ERROR))
        .satisfies(
            (errors) -> {
              assertEquals(1, errors.spliterator().getExactSizeIfKnown());
              Row error = errors.iterator().next();
              assertEquals(row, error.getRow("failed_row"));
              assertTrue(
                  "Expected error message to contain 'Message is required.'",
                  error.getString("error_message").contains("Message is required."));
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Test
  public void testErrorHandlingWithMissingRequiredField() {
    DatadogWriteSchemaTransformProvider provider = new DatadogWriteSchemaTransformProvider();
    ErrorHandling errorHandling = ErrorHandling.builder().setOutput(ERROR).build();
    DatadogWriteSchemaTransformConfiguration configuration =
        DatadogWriteSchemaTransformConfiguration.builder()
            .setApiKey("test-api-key")
            .setUrl("http://localhost:8080")
            .setErrorHandling(errorHandling)
            .build();

    Schema missingFieldSchema =
        Schema.builder().addStringField("ddsource").addStringField("hostname").build();

    Row row =
        Row.withSchema(missingFieldSchema)
            .withFieldValue("ddsource", "my-source")
            .withFieldValue("hostname", "my-host")
            .build();

    PCollection<Row> input = p.apply(Create.of(row).withRowSchema(missingFieldSchema));
    PCollectionRowTuple inputTuple = PCollectionRowTuple.of(INPUT, input);

    SchemaTransform transform = provider.from(configuration);
    PCollectionRowTuple outputTuple = transform.expand(inputTuple);

    assertTrue(outputTuple.has(ERROR));
    PAssert.that(outputTuple.get(ERROR))
        .satisfies(
            (errors) -> {
              assertEquals(1, errors.spliterator().getExactSizeIfKnown());
              Row error = errors.iterator().next();
              assertEquals(row, error.getRow("failed_row"));
              assertTrue(
                  "Expected error message to contain 'Message is required.'",
                  error.getString("error_message").contains("Message is required."));
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Test
  public void testConfigurationSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerSchemaProvider(ErrorHandling.class, new AutoValueSchema());
    Schema schema = registry.getSchema(DatadogWriteSchemaTransformConfiguration.class);
    Schema errorHandlingSchema = registry.getSchema(ErrorHandling.class);

    LOG.info("Schema fields: {}", schema.getFieldNames());

    assertEquals(7, schema.getFieldCount());
    assertTrue(schema.hasField("url"));
    assertTrue(schema.hasField("apiKey"));
    assertTrue(schema.hasField("minBatchCount"));
    assertTrue(schema.hasField("batchCount"));
    assertTrue(schema.hasField("maxBufferSize"));
    assertTrue(schema.hasField("parallelism"));
    assertTrue(schema.hasField("errorHandling"));

    LOG.info("URL field type: {}", schema.getField("url").getType().getTypeName());
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
