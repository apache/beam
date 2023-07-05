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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.api.client.util.Clock;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.avro.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PubsubReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class PubsubReadSchemaTransformProviderTest {

  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("number", Schema.FieldType.INT64));

  private static final String SUBSCRIPTION = "projects/project/subscriptions/subscription";
  private static final String TOPIC = "projects/project/topics/topic";

  private static final List<TestCase> cases =
      Arrays.asList(
          testCase(
                  "no configured topic or subscription",
                  PubsubReadSchemaTransformConfiguration.builder().setDataSchema(SCHEMA).build())
              .expectInvalidConfiguration(),
          testCase(
                  "both topic and subscription configured",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setSubscription(TOPIC)
                      .setDataSchema(SCHEMA)
                      .build())
              .expectInvalidConfiguration(),
          testCase(
                  "invalid format configured",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setDataSchema(SCHEMA)
                      .setFormat("invalidformat")
                      .build())
              .expectInvalidConfiguration(),
          testCase(
                  "configuration with subscription",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setDataSchema(SCHEMA)
                      .build())
              .withExpectedPubsubRead(PubsubIO.readMessages().fromSubscription(SUBSCRIPTION)),
          testCase(
                  "configuration with topic",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setTopic(TOPIC)
                      .setDataSchema(SCHEMA)
                      .build())
              .withExpectedPubsubRead(PubsubIO.readMessages().fromTopic(TOPIC)),
          testCase(
                  "configuration with subscription, timestamp and id attributes",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setTimestampAttribute("timestampAttribute")
                      .setIdAttribute("idAttribute")
                      .setDataSchema(SCHEMA)
                      .build())
              .withExpectedPubsubRead(
                  PubsubIO.readMessages()
                      .fromSubscription(SUBSCRIPTION)
                      .withTimestampAttribute("timestampAttribute")
                      .withIdAttribute("idAttribute")),
          testCase(
                  "configuration with subscription and dead letter queue",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setDataSchema(SCHEMA)
                      .setDeadLetterQueue(TOPIC)
                      .build())
              .withExpectedPubsubRead(PubsubIO.readMessages().fromSubscription(SUBSCRIPTION))
              .withExpectedDeadLetterQueue(PubsubIO.writeMessages().to(TOPIC)),
          testCase(
                  "configuration with subscription, timestamp attribute, and dead letter queue",
                  PubsubReadSchemaTransformConfiguration.builder()
                      .setSubscription(SUBSCRIPTION)
                      .setTimestampAttribute("timestampAttribute")
                      .setDataSchema(SCHEMA)
                      .setDeadLetterQueue(TOPIC)
                      .build())
              .withExpectedPubsubRead(
                  PubsubIO.readMessages()
                      .fromSubscription(SUBSCRIPTION)
                      .withTimestampAttribute("timestampAttribute"))
              .withExpectedDeadLetterQueue(
                  PubsubIO.writeMessages().to(TOPIC).withTimestampAttribute("timestampAttribute")));

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<PubsubReadSchemaTransformConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(PubsubReadSchemaTransformConfiguration.class);
  private static final SerializableFunction<PubsubReadSchemaTransformConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA).withFieldValue("name", "a").withFieldValue("number", 100L).build(),
          Row.withSchema(SCHEMA).withFieldValue("name", "b").withFieldValue("number", 200L).build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("name", "c")
              .withFieldValue("number", 300L)
              .build());

  private static final Clock CLOCK = (Clock & Serializable) () -> 1656788475425L;

  private static final AvroPayloadSerializerProvider AVRO_PAYLOAD_SERIALIZER_PROVIDER =
      new AvroPayloadSerializerProvider();
  private static final PayloadSerializer AVRO_PAYLOAD_SERIALIZER =
      AVRO_PAYLOAD_SERIALIZER_PROVIDER.getSerializer(SCHEMA, new HashMap<>());

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testBuildDeadLetterQueueWrite() {
    for (TestCase testCase : cases) {
      PubsubIO.Write<PubsubMessage> dlq =
          testCase.pubsubReadSchemaTransform().buildDeadLetterQueueWrite();

      if (testCase.expectedDeadLetterQueue == null) {
        assertNull(testCase.name, dlq);
        return;
      }

      Map<DisplayData.Identifier, DisplayData.Item> actual = DisplayData.from(dlq).asMap();
      Map<DisplayData.Identifier, DisplayData.Item> expected = testCase.expectedDeadLetterQueue;

      assertEquals(testCase.name, expected, actual);
    }
  }

  @Test
  public void testReadAvro() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
    PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform transform =
        schemaTransformWithClock("avro");
    PubsubTestClient.PubsubTestClientFactory clientFactory =
        clientFactory(incomingAvroMessagesOf(CLOCK.currentTimeMillis()));
    transform.setClientFactory(clientFactory);
    PCollectionRowTuple reads = begin.apply(transform);

    PAssert.that(reads.get(PubsubReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(ROWS);

    p.run().waitUntilFinish();
    clientFactory.close();
  }

  @Test
  public void testReadJson() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
    PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform transform =
        schemaTransformWithClock("json");
    PubsubTestClient.PubsubTestClientFactory clientFactory =
        clientFactory(incomingJsonMessagesOf(CLOCK.currentTimeMillis()));
    transform.setClientFactory(clientFactory);
    PCollectionRowTuple reads = begin.apply(transform);

    PAssert.that(reads.get(PubsubReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(ROWS);

    p.run().waitUntilFinish();

    clientFactory.close();
  }

  @Test
  public void testBuildPubSubRead() {
    for (TestCase testCase : cases) {
      if (testCase.invalidConfigurationExpected) {
        continue;
      }
      Map<DisplayData.Identifier, DisplayData.Item> actual =
          DisplayData.from(testCase.pubsubReadSchemaTransform().buildPubsubRead()).asMap();

      Map<DisplayData.Identifier, DisplayData.Item> expected = testCase.expectedPubsubRead;

      assertEquals(testCase.name, expected, actual);
    }
  }

  @Test
  public void testInvalidConfiguration() {
    for (TestCase testCase : cases) {
      PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
      if (testCase.invalidConfigurationExpected) {
        assertThrows(
            testCase.name,
            RuntimeException.class,
            () -> begin.apply(testCase.pubsubReadSchemaTransform()));
      }
    }
  }

  @Test
  public void testInvalidInput() {
    PCollectionRowTuple begin = PCollectionRowTuple.of("BadInput", p.apply(Create.of(ROWS)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            begin.apply(
                new PubsubReadSchemaTransformProvider()
                    .from(
                        PubsubReadSchemaTransformConfiguration.builder()
                            .setDataSchema(SCHEMA)
                            .build())));
  }

  private PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform schemaTransformWithClock(
      String format) {
    PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform transform =
        (PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform)
            new PubsubReadSchemaTransformProvider()
                .from(
                    PubsubReadSchemaTransformConfiguration.builder()
                        .setDataSchema(SCHEMA)
                        .setSubscription(SUBSCRIPTION)
                        .setFormat(format)
                        .build());

    transform.setClock(CLOCK);

    return transform;
  }

  private static PubsubTestClient.PubsubTestClientFactory clientFactory(
      List<PubsubClient.IncomingMessage> messages) {
    return PubsubTestClient.createFactoryForPull(
        CLOCK, PubsubClient.subscriptionPathFromPath(SUBSCRIPTION), 60, messages);
  }

  private static List<PubsubClient.IncomingMessage> incomingAvroMessagesOf(long millisSinceEpoch) {
    return ROWS.stream()
        .map(row -> incomingAvroMessageOf(row, millisSinceEpoch))
        .collect(Collectors.toList());
  }

  private static PubsubClient.IncomingMessage incomingAvroMessageOf(
      Row row, long millisSinceEpoch) {
    byte[] bytes = AVRO_PAYLOAD_SERIALIZER.serialize(row);
    return incomingMessageOf(bytes, millisSinceEpoch);
  }

  private static List<PubsubClient.IncomingMessage> incomingJsonMessagesOf(long millisSinceEpoch) {
    return PubsubReadSchemaTransformProviderTest.ROWS.stream()
        .map(row -> incomingJsonMessageOf(row, millisSinceEpoch))
        .collect(Collectors.toList());
  }

  private static PubsubClient.IncomingMessage incomingJsonMessageOf(
      Row row, long millisSinceEpoch) {
    String name = Objects.requireNonNull(row.getString("name"));
    long number = Objects.requireNonNull(row.getInt64("number"));
    return incomingJsonMessageOf(name, number, millisSinceEpoch);
  }

  private static PubsubClient.IncomingMessage incomingJsonMessageOf(
      String name, long number, long millisSinceEpoch) {
    Gson gson = new Gson();
    JsonObject obj = new JsonObject();
    obj.add("name", new JsonPrimitive(name));
    obj.add("number", new JsonPrimitive(number));
    byte[] bytes = gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
    return incomingMessageOf(bytes, millisSinceEpoch);
  }

  private static PubsubClient.IncomingMessage incomingMessageOf(
      byte[] bytes, long millisSinceEpoch) {
    int nanos = Long.valueOf(millisSinceEpoch).intValue() * 1000;
    Timestamp timestamp = Timestamp.newBuilder().setNanos(nanos).build();
    return PubsubClient.IncomingMessage.of(
        com.google.pubsub.v1.PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(bytes))
            .setPublishTime(timestamp)
            .build(),
        millisSinceEpoch,
        0,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
  }

  static TestCase testCase(String name, PubsubReadSchemaTransformConfiguration configuration) {
    return new TestCase(name, configuration);
  }

  private static class TestCase {

    private final String name;
    private final PubsubReadSchemaTransformConfiguration configuration;

    private Map<DisplayData.Identifier, DisplayData.Item> expectedDeadLetterQueue;

    private Map<DisplayData.Identifier, DisplayData.Item> expectedPubsubRead =
        DisplayData.from(PubsubIO.readMessages()).asMap();

    private boolean invalidConfigurationExpected = false;

    TestCase(String name, PubsubReadSchemaTransformConfiguration configuration) {
      this.name = name;
      this.configuration = configuration;
    }

    PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform pubsubReadSchemaTransform() {
      PubsubReadSchemaTransformProvider provider = new PubsubReadSchemaTransformProvider();
      Row configurationRow = toBeamRow();
      return (PubsubReadSchemaTransformProvider.PubsubReadSchemaTransform)
          provider.from(configurationRow);
    }

    private Row toBeamRow() {
      return ROW_SERIALIZABLE_FUNCTION.apply(configuration);
    }

    TestCase withExpectedDeadLetterQueue(PubsubIO.Write<PubsubMessage> value) {
      this.expectedDeadLetterQueue = DisplayData.from(value).asMap();
      return this;
    }

    TestCase withExpectedPubsubRead(PubsubIO.Read<PubsubMessage> value) {
      this.expectedPubsubRead = DisplayData.from(value).asMap();
      return this;
    }

    TestCase expectInvalidConfiguration() {
      this.invalidConfigurationExpected = true;
      return this;
    }
  }
}
