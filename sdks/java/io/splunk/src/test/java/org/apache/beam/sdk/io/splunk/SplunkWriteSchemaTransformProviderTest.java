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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.sdk.io.splunk.SplunkWriteSchemaTransformProvider.ERROR;
import static org.apache.beam.sdk.io.splunk.SplunkWriteSchemaTransformProvider.INPUT;
import static org.apache.beam.sdk.io.splunk.SplunkWriteSchemaTransformProvider.OUTPUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link SplunkWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class SplunkWriteSchemaTransformProviderTest {

  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.HEC_URL_PATH;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Splunk HEC server.
  @Rule public MockServerRule mockServerRule = new MockServerRule(this);
  private MockServerClient mockServerClient;

  private static final Schema FIELDS_SCHEMA =
      Schema.builder()
          .addStringField("customfield")
          .addInt64Field("count")
          .addArrayField("tags", Schema.FieldType.STRING)
          .build();

  private static final Schema SCHEMA =
      Schema.builder()
          .addNullableField("time", Schema.FieldType.INT64)
          .addStringField("host")
          .addStringField("source")
          .addStringField("sourcetype")
          .addNullableField("index", Schema.FieldType.STRING)
          .addNullableRowField("fields", FIELDS_SCHEMA)
          .addStringField("event")
          .build();

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA)
              .withFieldValue("time", 12345L)
              .withFieldValue("host", "test-host-1")
              .withFieldValue("source", "test-source-1")
              .withFieldValue("sourcetype", "test-source-type-1")
              .withFieldValue("index", "test-index-1")
              .withFieldValue(
                  "fields",
                  Row.withSchema(FIELDS_SCHEMA)
                      .withFieldValue("customfield", "value")
                      .withFieldValue("count", 1L)
                      .withFieldValue("tags", Arrays.asList("a", "b"))
                      .build())
              .withFieldValue("event", "test-event-1")
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("time", null)
              .withFieldValue("host", "test-host-2")
              .withFieldValue("source", "test-source-2")
              .withFieldValue("sourcetype", "test-source-type-2")
              .withFieldValue("index", null)
              .withFieldValue("fields", null)
              .withFieldValue("event", "test-event-2")
              .build());

  private static List<SplunkEvent> expectedEvents() {
    JsonArray tags = new JsonArray();
    tags.add("a");
    tags.add("b");
    JsonObject fields = new JsonObject();
    fields.addProperty("customfield", "value");
    fields.addProperty("count", 1L);
    fields.add("tags", tags);
    return Arrays.asList(
        SplunkEvent.newBuilder()
            .withTime(12345L)
            .withHost("test-host-1")
            .withSource("test-source-1")
            .withSourceType("test-source-type-1")
            .withIndex("test-index-1")
            .withFields(fields)
            .withEvent("test-event-1")
            .create(),
        SplunkEvent.newBuilder()
            .withHost("test-host-2")
            .withSource("test-source-2")
            .withSourceType("test-source-type-2")
            .withEvent("test-event-2")
            .create());
  }

  private String mockServerUrl() {
    return Joiner.on(':').join("http://localhost", mockServerRule.getPort());
  }

  private void mockServerListening(int statusCode) {
    mockServerClient
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }

  @Test
  public void testWriteInvalidConfigurations() {
    // token not set
    assertThrows(
        IllegalStateException.class,
        () -> SplunkWriteSchemaTransformConfiguration.builder().setUrl(mockServerUrl()).build());

    // url not set
    assertThrows(
        IllegalStateException.class,
        () -> SplunkWriteSchemaTransformConfiguration.builder().setToken("test-token").build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SplunkWriteSchemaTransformConfiguration.builder()
                .setUrl("")
                .setToken("test-token")
                .build()
                .validate());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SplunkWriteSchemaTransformConfiguration.builder()
                .setUrl(mockServerUrl())
                .setToken("test-token")
                .setBatchCount(0)
                .build()
                .validate());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SplunkWriteSchemaTransformConfiguration.builder()
                .setUrl(mockServerUrl())
                .setToken("test-token")
                .setParallelism(0)
                .build()
                .validate());
  }

  @Test
  public void testWriteBuildTransformWithCorrectFields() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == SplunkWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider splunkProvider = providers.get(0);
    assertEquals(splunkProvider.outputCollectionNames(), Lists.newArrayList(ERROR));

    assertEquals(
        Sets.newHashSet(
            "url",
            "token",
            "batch_count",
            "parallelism",
            "disable_certificate_validation",
            "root_ca_certificate_path",
            "enable_batch_logs",
            "enable_gzip_http_compression",
            "error_handling"),
        splunkProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testRowToSplunkEvent() {
    List<SplunkEvent> events = expectedEvents();
    for (int i = 0; i < ROWS.size(); i++) {
      assertEquals(events.get(i), SplunkWriteSchemaTransformProvider.rowToEvent(ROWS.get(i)));
    }
  }

  @Test
  public void testRowToSplunkEventWithOnlyEvent() {
    Schema schema = Schema.builder().addStringField("event").build();
    Row row = Row.withSchema(schema).withFieldValue("event", "test-event").build();

    assertEquals(
        SplunkEvent.newBuilder().withEvent("test-event").create(),
        SplunkWriteSchemaTransformProvider.rowToEvent(row));
  }

  @Test
  public void testRowToSplunkEventWithJsonStringFields() {
    Schema schema = Schema.builder().addStringField("fields").addStringField("event").build();
    Row row =
        Row.withSchema(schema)
            .withFieldValue("fields", "{\"customfield\": \"value\"}")
            .withFieldValue("event", "test-event")
            .build();

    JsonObject fields = new JsonObject();
    fields.addProperty("customfield", "value");
    assertEquals(
        SplunkEvent.newBuilder().withFields(fields).withEvent("test-event").create(),
        SplunkWriteSchemaTransformProvider.rowToEvent(row));
  }

  @Test
  public void testRowToSplunkEventWithExtraFields_DiscardsExtraFields() {
    Schema schema =
        Schema.builder()
            .addStringField("host")
            .addStringField("event")
            .addStringField("extra_field")
            .build();
    Row row =
        Row.withSchema(schema)
            .withFieldValue("host", "test-host")
            .withFieldValue("event", "test-event")
            .withFieldValue("extra_field", "extra_value")
            .build();

    assertEquals(
        SplunkEvent.newBuilder().withHost("test-host").withEvent("test-event").create(),
        SplunkWriteSchemaTransformProvider.rowToEvent(row));
  }

  @Test(expected = NullPointerException.class)
  public void testRowToSplunkEventWithNullEvent() {
    Schema schema =
        Schema.builder()
            .addStringField("host")
            .addNullableField("event", Schema.FieldType.STRING)
            .build();
    Row row =
        Row.withSchema(schema)
            .withFieldValue("host", "test-host")
            .withFieldValue("event", null)
            .build();

    SplunkWriteSchemaTransformProvider.rowToEvent(row);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRowToSplunkEventWithWrongFieldsType() {
    Schema schema = Schema.builder().addInt64Field("fields").addStringField("event").build();
    Row row =
        Row.withSchema(schema)
            .withFieldValue("fields", 1L)
            .withFieldValue("event", "test-event")
            .build();

    SplunkWriteSchemaTransformProvider.rowToEvent(row);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSuccessfulWrite() {
    mockServerListening(200);

    SplunkWriteSchemaTransformConfiguration configuration =
        SplunkWriteSchemaTransformConfiguration.builder()
            .setUrl(mockServerUrl())
            .setToken("test-token")
            .setBatchCount(ROWS.size())
            .setParallelism(1)
            .setErrorHandling(ErrorHandling.builder().setOutput(OUTPUT).build())
            .build();
    SchemaTransform transform = new SplunkWriteSchemaTransformProvider().from(configuration);

    PCollection<Row> input = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of(INPUT, input));

    PAssert.that(output.get(OUTPUT)).empty();

    p.run();

    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteErrorsWithErrorHandling() {
    mockServerListening(404);

    SplunkWriteSchemaTransformConfiguration configuration =
        SplunkWriteSchemaTransformConfiguration.builder()
            .setUrl(mockServerUrl())
            .setToken("test-token")
            .setBatchCount(ROWS.size())
            .setParallelism(1)
            .setErrorHandling(ErrorHandling.builder().setOutput(OUTPUT).build())
            .build();
    SchemaTransform transform = new SplunkWriteSchemaTransformProvider().from(configuration);

    PCollection<Row> input = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of(INPUT, input));

    PAssert.that(output.get(OUTPUT))
        .satisfies(
            errors -> {
              int count = 0;
              for (Row error : errors) {
                count++;
                assertNull(error.getRow("failed_row"));
                assertEquals((Integer) 404, error.getInt32("statusCode"));
                assertTrue(error.getString("payload").contains("test-event-"));
              }
              assertEquals(ROWS.size(), count);
              return null;
            });

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testConversionErrorsWithErrorHandling() {
    mockServerListening(200);

    SplunkWriteSchemaTransformConfiguration configuration =
        SplunkWriteSchemaTransformConfiguration.builder()
            .setUrl(mockServerUrl())
            .setToken("test-token")
            .setErrorHandling(ErrorHandling.builder().setOutput(OUTPUT).build())
            .build();
    SchemaTransform transform = new SplunkWriteSchemaTransformProvider().from(configuration);

    Schema schema = Schema.builder().addStringField("host").build();
    Row row = Row.withSchema(schema).withFieldValue("host", "test-host").build();

    PCollection<Row> input = p.apply(Create.of(row).withRowSchema(schema));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of(INPUT, input));

    PAssert.that(output.get(OUTPUT))
        .satisfies(
            errors -> {
              Row error = errors.iterator().next();
              assertEquals(1, errors.spliterator().getExactSizeIfKnown());
              assertEquals(row, error.getRow("failed_row"));
              assertEquals(row.toString(), error.getString("payload"));
              assertEquals(
                  (Integer) java.net.HttpURLConnection.HTTP_BAD_REQUEST,
                  error.getInt32("statusCode"));
              assertTrue(error.getString("statusMessage").contains("Event is required."));
              return null;
            });

    p.run();

    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(0));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteErrorFailsPipelineWithoutErrorHandling() {
    mockServerListening(404);

    SplunkWriteSchemaTransformConfiguration configuration =
        SplunkWriteSchemaTransformConfiguration.builder()
            .setUrl(mockServerUrl())
            .setToken("test-token")
            .setBatchCount(ROWS.size())
            .build();
    SchemaTransform transform = new SplunkWriteSchemaTransformProvider().from(configuration);

    PCollection<Row> input = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of(INPUT, input));
    assertEquals(1, output.getAll().size());
    assertTrue(output.has(ERROR));

    assertThrows(PipelineExecutionException.class, () -> p.run().waitUntilFinish());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildTransformFromRowConfiguration() {
    mockServerListening(200);

    SplunkWriteSchemaTransformProvider provider = new SplunkWriteSchemaTransformProvider();
    Schema configSchema = provider.configurationSchema();
    Schema errorHandlingSchema = configSchema.getField("error_handling").getType().getRowSchema();

    Row configRow =
        Row.withSchema(configSchema)
            .withFieldValue("url", mockServerUrl())
            .withFieldValue("token", "test-token")
            .withFieldValue("batch_count", 1)
            .withFieldValue("parallelism", 1)
            .withFieldValue("disable_certificate_validation", false)
            .withFieldValue("root_ca_certificate_path", null)
            .withFieldValue("enable_batch_logs", true)
            .withFieldValue("enable_gzip_http_compression", false)
            .withFieldValue(
                "error_handling",
                Row.withSchema(errorHandlingSchema).withFieldValue("output", OUTPUT).build())
            .build();
    SchemaTransform transform = provider.from(configRow);

    PCollection<Row> input = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of(INPUT, input));

    PAssert.that(output.get(OUTPUT)).empty();

    p.run();

    mockServerClient.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(ROWS.size()));
  }
}
