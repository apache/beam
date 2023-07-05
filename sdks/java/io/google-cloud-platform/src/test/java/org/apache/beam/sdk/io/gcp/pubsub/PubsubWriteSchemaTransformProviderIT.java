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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubWriteSchemaTransformConfiguration.DEFAULT_TIMESTAMP_ATTRIBUTE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubWriteSchemaTransformProvider.INPUT_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Integration tests for {@link PubsubWriteSchemaTransformProvider}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubWriteSchemaTransformProviderIT {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final TestPubsubOptions TEST_PUBSUB_OPTIONS =
      TestPipeline.testingPipelineOptions().as(TestPubsubOptions.class);

  static {
    TEST_PUBSUB_OPTIONS.setBlockOnRun(false);
  }

  private static final String HAS_NO_SCHEMA = "has-no-schema";

  private static PubsubClient pubsubClient;

  private static PubsubClient.TopicPath hasNoSchemaTopic;

  private static PubsubClient.SubscriptionPath hasNoSchemaSubscription;

  private static final Instant TIMESTAMP = Instant.now();

  private static final String RESOURCE_NAME_POSTFIX = "-" + TIMESTAMP.getMillis();

  private static final int ACK_DEADLINE_SECONDS = 60;

  private static final int AWAIT_TERMINATED_SECONDS = 30;

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();

  private static final TypeDescriptor<PubsubWriteSchemaTransformConfiguration>
      CONFIGURATION_TYPE_DESCRIPTOR =
          TypeDescriptor.of(PubsubWriteSchemaTransformConfiguration.class);

  private static final SerializableFunction<PubsubWriteSchemaTransformConfiguration, Row>
      TO_ROW_FN = AUTO_VALUE_SCHEMA.toRowFunction(CONFIGURATION_TYPE_DESCRIPTOR);

  private final Field timestampField = Field.of("timestamp", FieldType.DATETIME);

  private final Field payloadBytesField = Field.of("payload", FieldType.BYTES);

  @BeforeClass
  public static void setUp() throws IOException {
    String project = TEST_PUBSUB_OPTIONS.as(PubsubOptions.class).getProject();
    pubsubClient = PubsubGrpcClient.FACTORY.newClient(null, null, TEST_PUBSUB_OPTIONS);
    hasNoSchemaTopic =
        PubsubClient.topicPathFromName(project, HAS_NO_SCHEMA + RESOURCE_NAME_POSTFIX);
    hasNoSchemaSubscription =
        PubsubClient.subscriptionPathFromName(project, HAS_NO_SCHEMA + RESOURCE_NAME_POSTFIX);

    pubsubClient.createTopic(hasNoSchemaTopic);
    pubsubClient.createSubscription(
        hasNoSchemaTopic, hasNoSchemaSubscription, ACK_DEADLINE_SECONDS);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    pubsubClient.deleteSubscription(hasNoSchemaSubscription);
    pubsubClient.deleteTopic(hasNoSchemaTopic);

    pubsubClient.close();
  }

  @Test
  public void testWritePayloadBytes() throws IOException {
    Instant timestamp = Instant.ofEpochMilli(100000L);
    Schema schema = Schema.of(payloadBytesField, timestampField);
    List<Row> input =
        Collections.singletonList(
            Row.withSchema(schema).attachValues("aaa".getBytes(StandardCharsets.UTF_8), timestamp));
    Row configuration =
        TO_ROW_FN.apply(
            PubsubWriteSchemaTransformConfiguration.builder()
                .setSource(
                    PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                        .setPayloadFieldName(payloadBytesField.getName())
                        .setTimestampFieldName(timestampField.getName())
                        .build())
                .setTopic(hasNoSchemaTopic.getPath())
                .setTarget(
                    PubsubWriteSchemaTransformConfiguration.targetConfigurationBuilder().build())
                .build());

    PCollectionRowTuple.of(INPUT_TAG, pipeline.apply(Create.of(input).withRowSchema(schema)))
        .apply(new PubsubWriteSchemaTransformProvider().from(configuration));

    PipelineResult job = pipeline.run(TEST_PUBSUB_OPTIONS);
    Instant now = Instant.now();
    Instant stop = Instant.ofEpochMilli(now.getMillis() + AWAIT_TERMINATED_SECONDS * 1000);
    List<Pair<String, Map<String, String>>> actualList = new ArrayList<>();
    while (now.isBefore(stop)) {
      List<IncomingMessage> received = pubsubClient.pull(0, hasNoSchemaSubscription, 1, true);
      for (IncomingMessage incoming : received) {
        actualList.add(
            Pair.of(
                incoming.message().getData().toStringUtf8(),
                ImmutableMap.of(
                    DEFAULT_TIMESTAMP_ATTRIBUTE,
                    incoming
                        .message()
                        .getAttributesMap()
                        .getOrDefault(DEFAULT_TIMESTAMP_ATTRIBUTE, ""))));
      }
      if (actualList.size() == input.size()) {
        break;
      }
      now = Instant.now();
    }
    job.cancel();
    assertFalse(
        String.format(
            "messages pulled from %s should not be empty", hasNoSchemaSubscription.getPath()),
        actualList.isEmpty());
    Pair<String, Map<String, String>> actual = actualList.get(0);
    Row expected = input.get(0);
    String payload =
        new String(
            Objects.requireNonNull(expected.getBytes(payloadBytesField.getName())),
            StandardCharsets.UTF_8);
    assertEquals(payload, actual.getLeft());
    assertEquals(
        ISODateTimeFormat.dateTime().print(timestamp),
        actual.getRight().get(DEFAULT_TIMESTAMP_ATTRIBUTE));
  }
}
