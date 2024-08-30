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
import static org.junit.Assert.assertThrows;

import com.google.api.client.util.Clock;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.io.gcp.pubsub.PubsubReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class PubsubReadSchemaTransformProviderTest {

  private static final Schema BEAM_SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("number", Schema.FieldType.INT64));
  private static final Schema BEAM_SCHEMA_WITH_ERROR =
      Schema.of(Schema.Field.of("error", Schema.FieldType.STRING));
  private static final String SCHEMA = AvroUtils.toAvroSchema(BEAM_SCHEMA).toString();
  private static final String SUBSCRIPTION = "projects/project/subscriptions/subscription";
  private static final String TOPIC = "projects/project/topics/topic";

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA)
              .withFieldValue("name", "a")
              .withFieldValue("number", 100L)
              .build(),
          Row.withSchema(BEAM_SCHEMA)
              .withFieldValue("name", "b")
              .withFieldValue("number", 200L)
              .build(),
          Row.withSchema(BEAM_SCHEMA)
              .withFieldValue("name", "c")
              .withFieldValue("number", 300L)
              .build());

  private static final List<Row> ROWSWITHERROR =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA_WITH_ERROR).withFieldValue("error", "a").build(),
          Row.withSchema(BEAM_SCHEMA_WITH_ERROR).withFieldValue("error", "b").build(),
          Row.withSchema(BEAM_SCHEMA_WITH_ERROR).withFieldValue("error", "c").build());

  private static final Clock CLOCK = (Clock & Serializable) () -> 1678988970000L;

  private static final AvroPayloadSerializerProvider AVRO_PAYLOAD_SERIALIZER_PROVIDER =
      new AvroPayloadSerializerProvider();
  private static final PayloadSerializer AVRO_PAYLOAD_SERIALIZER =
      AVRO_PAYLOAD_SERIALIZER_PROVIDER.getSerializer(BEAM_SCHEMA, new HashMap<>());
  private static final PayloadSerializer AVRO_PAYLOAD_SERIALIZER_WITH_ERROR =
      AVRO_PAYLOAD_SERIALIZER_PROVIDER.getSerializer(BEAM_SCHEMA_WITH_ERROR, new HashMap<>());

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testInvalidConfigNoTopicOrSubscription() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PubsubReadSchemaTransformProvider()
                .from(
                    PubsubReadSchemaTransformConfiguration.builder()
                        .setSchema(SCHEMA)
                        .setFormat("AVRO")
                        .build()));
  }

  @Test
  public void testInvalidConfigBothTopicAndSubscription() {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            begin.apply(
                new PubsubReadSchemaTransformProvider()
                    .from(
                        PubsubReadSchemaTransformConfiguration.builder()
                            .setSchema(SCHEMA)
                            .setFormat("AVRO")
                            .setTopic(TOPIC)
                            .setSubscription(SUBSCRIPTION)
                            .build())));
    p.run().waitUntilFinish();
  }

  @Test
  public void testInvalidConfigInvalidFormat() {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            begin.apply(
                new PubsubReadSchemaTransformProvider()
                    .from(
                        PubsubReadSchemaTransformConfiguration.builder()
                            .setSchema(SCHEMA)
                            .setFormat("BadFormat")
                            .setSubscription(SUBSCRIPTION)
                            .build())));
    p.run().waitUntilFinish();
  }

  @Test
  public void testNoSchema() {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);
    assertThrows(
        IllegalStateException.class,
        () ->
            begin.apply(
                new PubsubReadSchemaTransformProvider()
                    .from(
                        PubsubReadSchemaTransformConfiguration.builder()
                            .setSubscription(SUBSCRIPTION)
                            .setFormat("AVRO")
                            .build())));
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadRaw() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);

    Schema rawSchema = Schema.of(Schema.Field.of("payload", Schema.FieldType.BYTES));
    byte[] payload = "some payload".getBytes(StandardCharsets.UTF_8);

    try (PubsubTestClientFactory clientFactory =
        clientFactory(ImmutableList.of(incomingMessageOf(payload, CLOCK.currentTimeMillis())))) {
      PubsubReadSchemaTransformConfiguration config =
          PubsubReadSchemaTransformConfiguration.builder()
              .setFormat("RAW")
              .setSchema("")
              .setSubscription(SUBSCRIPTION)
              .setClientFactory(clientFactory)
              .setClock(CLOCK)
              .build();
      SchemaTransform transform = new PubsubReadSchemaTransformProvider().from(config);
      PCollectionRowTuple reads = begin.apply(transform);

      PAssert.that(reads.get("output"))
          .containsInAnyOrder(
              ImmutableList.of(Row.withSchema(rawSchema).addValue(payload).build()));

      p.run().waitUntilFinish();
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void testReadAttributes() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);

    Schema.builder()
        .addByteArrayField("payload")
        .addStringField("attr")
        .addMapField("attrMap", Schema.FieldType.STRING, Schema.FieldType.STRING)
        .build();

    Schema rawSchema =
        Schema.builder()
            .addByteArrayField("payload")
            .addStringField("attr")
            .addMapField("attrMap", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();
    byte[] payload = "some payload".getBytes(StandardCharsets.UTF_8);
    String attr = "attr value";

    try (PubsubTestClientFactory clientFactory =
        clientFactory(
            ImmutableList.of(
                incomingMessageOf(
                    payload, CLOCK.currentTimeMillis(), ImmutableMap.of("attr", attr))))) {
      PubsubReadSchemaTransformConfiguration config =
          PubsubReadSchemaTransformConfiguration.builder()
              .setFormat("RAW")
              .setSchema("")
              .setSubscription(SUBSCRIPTION)
              .setAttributes(ImmutableList.of("attr"))
              .setAttributesMap("attrMap")
              .setClientFactory(clientFactory)
              .setClock(CLOCK)
              .build();
      SchemaTransform transform = new PubsubReadSchemaTransformProvider().from(config);
      PCollectionRowTuple reads = begin.apply(transform);

      PAssert.that(reads.get("output"))
          .containsInAnyOrder(
              ImmutableList.of(
                  Row.withSchema(rawSchema)
                      .addValue(payload)
                      .addValue(attr)
                      .addValue(ImmutableMap.of("attr", attr))
                      .build()));

      p.run().waitUntilFinish();
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void testReadAvro() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);

    try (PubsubTestClientFactory clientFactory = clientFactory(beamRowToMessage())) {
      PubsubReadSchemaTransformConfiguration config =
          PubsubReadSchemaTransformConfiguration.builder()
              .setFormat("AVRO")
              .setSchema(SCHEMA)
              .setSubscription(SUBSCRIPTION)
              .setClientFactory(clientFactory)
              .setClock(CLOCK)
              .build();
      SchemaTransform transform = new PubsubReadSchemaTransformProvider().from(config);
      PCollectionRowTuple reads = begin.apply(transform);

      PAssert.that(reads.get("output")).containsInAnyOrder(ROWS);

      p.run().waitUntilFinish();
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void testReadAvroWithError() throws IOException {
    PCollectionRowTuple begin = PCollectionRowTuple.empty(p);

    try (PubsubTestClientFactory clientFactory = clientFactory(beamRowToMessageWithError())) {
      PubsubReadSchemaTransformConfiguration config =
          PubsubReadSchemaTransformConfiguration.builder()
              .setFormat("AVRO")
              .setSchema(SCHEMA)
              .setSubscription(SUBSCRIPTION)
              .setErrorHandling(
                  PubsubReadSchemaTransformConfiguration.ErrorHandling.builder()
                      .setOutput("errors")
                      .build())
              .setClientFactory(clientFactory)
              .setClock(CLOCK)
              .build();
      SchemaTransform transform = new PubsubReadSchemaTransformProvider().from(config);
      PCollectionRowTuple reads = begin.apply(transform);

      PAssert.that(reads.get("output")).empty();

      PipelineResult result = p.run();
      result.waitUntilFinish();

      MetricResults metrics = result.metrics();
      MetricQueryResults metricResults =
          metrics.queryMetrics(
              MetricsFilter.builder()
                  .addNameFilter(
                      MetricNameFilter.named(
                          PubsubReadSchemaTransformProvider.class, "PubSub-read-error-counter"))
                  .build());

      Iterable<MetricResult<Long>> counters = metricResults.getCounters();
      if (!counters.iterator().hasNext()) {
        throw new RuntimeException("no counters available ");
      }

      Long expectedCount = 3L;
      for (MetricResult<Long> count : counters) {
        assertEquals(expectedCount, count.getAttempted());
      }
    } catch (Exception e) {
      throw e;
    }
  }

  private static List<PubsubClient.IncomingMessage> beamRowToMessage() {
    long timestamp = CLOCK.currentTimeMillis();
    return ROWS.stream()
        .map(
            row -> {
              byte[] bytes = AVRO_PAYLOAD_SERIALIZER.serialize(row);
              return incomingMessageOf(bytes, timestamp);
            })
        .collect(Collectors.toList());
  }

  private static List<PubsubClient.IncomingMessage> beamRowToMessageWithError() {
    long timestamp = CLOCK.currentTimeMillis();
    return ROWSWITHERROR.stream()
        .map(
            row -> {
              byte[] bytes = AVRO_PAYLOAD_SERIALIZER_WITH_ERROR.serialize(row);
              return incomingMessageOf(bytes, timestamp);
            })
        .collect(Collectors.toList());
  }

  private static PubsubClient.IncomingMessage incomingMessageOf(
      byte[] bytes, long millisSinceEpoch) {
    return incomingMessageOf(bytes, millisSinceEpoch, ImmutableMap.of());
  }

  private static PubsubClient.IncomingMessage incomingMessageOf(
      byte[] bytes, long millisSinceEpoch, Map<String, String> attributes) {
    int nanos = Long.valueOf(millisSinceEpoch).intValue() * 1000;
    Timestamp timestamp = Timestamp.newBuilder().setNanos(nanos).build();
    return PubsubClient.IncomingMessage.of(
        com.google.pubsub.v1.PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(bytes))
            .setPublishTime(timestamp)
            .putAllAttributes(attributes)
            .build(),
        millisSinceEpoch,
        0,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
  }

  private static PubsubTestClient.PubsubTestClientFactory clientFactory(
      List<PubsubClient.IncomingMessage> messages) {
    return PubsubTestClient.createFactoryForPull(
        CLOCK, PubsubClient.subscriptionPathFromPath(SUBSCRIPTION), 60, messages);
  }
}
