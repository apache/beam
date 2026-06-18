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

import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link PubsubIO.Write} operations. */
@RunWith(JUnit4.class)
public class PubsubWriteIT {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private PubsubClient pubsubClient;

  private TopicPath testTopic;
  private String project;

  @Before
  public void setup() throws IOException {
    PubsubOptions options = TestPipeline.testingPipelineOptions().as(PubsubOptions.class);
    project = options.getProject();
    pubsubClient = PubsubGrpcClient.FACTORY.newClient(null, null, options);
    testTopic =
        PubsubClient.topicPathFromName(project, "pubsub-write-" + Instant.now().getMillis());
    pubsubClient.createTopic(testTopic);
  }

  @After
  public void tearDown() throws IOException {
    pubsubClient.deleteTopic(testTopic);
    pubsubClient.close();
  }

  @Test
  public void testBoundedWriteSmallMessage() {
    String smallMessage = RandomStringUtils.randomAscii(100);
    pipeline.apply(Create.of(smallMessage)).apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteSequence() {
    pipeline
        .apply(GenerateSequence.from(0L).to(1000L))
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(sequence -> Objects.requireNonNull(sequence).toString()))
        .apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteLargeMessage() {
    String largeMessage = RandomStringUtils.randomAscii(1_000_000);
    pipeline.apply(Create.of(largeMessage)).apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteMessageWithAttributes() {
    byte[] payload = RandomStringUtils.randomAscii(1_000_000).getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes =
        ImmutableMap.<String, String>builder()
            .put("id", "1")
            .put("description", RandomStringUtils.randomAscii(100))
            .build();

    pipeline
        .apply(Create.of(new PubsubMessage(payload, attributes)))
        .apply(PubsubIO.writeMessages().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteMessageWithAttributesAndOrderingKey() throws IOException {
    TopicPath testTopicPath =
        PubsubClient.topicPathFromName(
            project, "pubsub-write-ordering-key-" + Instant.now().getMillis());
    pubsubClient.createTopic(testTopicPath);
    SubscriptionPath testSubscriptionPath =
        pubsubClient.createRandomSubscription(
            PubsubClient.projectPathFromId(project), testTopicPath, 10);

    byte[] payload = new byte[] {-16, -97, -89, -86}; // U+1F9EA

    // Messages with and without ordering keys can be mixed together in a collection. The writer
    // ensures that a publish request only sends message batches with the same ordering key,
    // otherwise the Pub/Sub service will reject the request.
    // Outgoing messages may specify either null or empty string to represent messages without
    // ordering keys, but Protobuf treats strings as primitives so we explicitly use empty string in
    // this test for the round trip assertion.
    Map<String, PubsubMessage> outgoingMessages = new HashMap<>();
    outgoingMessages.put(
        "0",
        new PubsubMessage(payload, ImmutableMap.of("id", "0"))
            .withOrderingKey("")); // No ordering key
    outgoingMessages.put(
        "1",
        new PubsubMessage(payload, ImmutableMap.of("id", "1"))
            .withOrderingKey("12")); // Repeated ordering key
    outgoingMessages.put(
        "2",
        new PubsubMessage(payload, ImmutableMap.of("id", "2"))
            .withOrderingKey("12")); // Repeated ordering key
    outgoingMessages.put(
        "3",
        new PubsubMessage(payload, ImmutableMap.of("id", "3"))
            .withOrderingKey("3")); // Distinct ordering key

    pipeline
        .apply(
            Create.of(ImmutableList.copyOf(outgoingMessages.values()))
                .withCoder(PubsubMessageSchemaCoder.getSchemaCoder()))
        .apply(PubsubIO.writeMessages().withOrderingKey().to(testTopicPath.getPath()));
    pipeline.run().waitUntilFinish();

    // sometimes the first pull comes up short. try 4 pulls to avoid flaky false-negatives
    int numPulls = 0;
    while (!outgoingMessages.isEmpty()) {
      boolean emptyOrDuplicatePull = true;
      List<IncomingMessage> incomingMessages =
          pubsubClient.pull(Instant.now().getMillis(), testSubscriptionPath, 4, true);

      for (IncomingMessage incomingMessage : incomingMessages) {
        com.google.pubsub.v1.PubsubMessage message = incomingMessage.message();
        @Nullable
        PubsubMessage outgoingMessage =
            outgoingMessages.remove(message.getAttributesMap().get("id"));
        if (outgoingMessage != null) {
          emptyOrDuplicatePull = false;
          assertEquals(
              UnsafeByteOperations.unsafeWrap(outgoingMessage.getPayload()), message.getData());
          assertEquals(outgoingMessage.getAttributeMap(), message.getAttributesMap());
          assertEquals(outgoingMessage.getOrderingKey(), message.getOrderingKey());
        }
      }

      if (emptyOrDuplicatePull && ++numPulls > 4) {
        throw new RuntimeException(
            String.format(
                "Pulled %s times from PubSub but retrieved no expected elements.", numPulls));
      }
    }

    pubsubClient.deleteSubscription(testSubscriptionPath);
    pubsubClient.deleteTopic(testTopicPath);
  }
}
