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

import com.google.api.client.util.Clock;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PubsubTestClient. */
@RunWith(JUnit4.class)
public class PubsubTestClientTest {
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final long REQ_TIME = 1234L;
  private static final long MESSAGE_TIME = 6789L;
  private static final String MESSAGE_ID = "testMessageId";
  private static final String DATA = "testData";
  private static final String ACK_ID = "testAckId";
  private static final int ACK_TIMEOUT_S = 60;

  @Test
  public void pullOneMessage() throws IOException {
    final AtomicLong now = new AtomicLong();
    Clock clock = now::get;
    IncomingMessage expectedIncomingMessage =
        IncomingMessage.of(
            PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(DATA)).build(),
            MESSAGE_TIME,
            REQ_TIME,
            ACK_ID,
            MESSAGE_ID);
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPull(
            clock, SUBSCRIPTION, ACK_TIMEOUT_S, Lists.newArrayList(expectedIncomingMessage))) {
      try (PubsubTestClient client = (PubsubTestClient) factory.newClient(null, null, null)) {
        now.set(REQ_TIME);
        client.advance();
        List<IncomingMessage> incomingMessages = client.pull(now.get(), SUBSCRIPTION, 1, true);
        assertEquals(1, incomingMessages.size());
        assertEquals(expectedIncomingMessage, incomingMessages.get(0));
        // Timeout on ACK.
        now.addAndGet((ACK_TIMEOUT_S + 10) * 1000);
        client.advance();
        incomingMessages = client.pull(now.get(), SUBSCRIPTION, 1, true);
        assertEquals(1, incomingMessages.size());
        assertEquals(
            IncomingMessage.of(
                expectedIncomingMessage.message(),
                expectedIncomingMessage.timestampMsSinceEpoch(),
                now.get(),
                expectedIncomingMessage.ackId(),
                expectedIncomingMessage.recordId()),
            incomingMessages.get(0));
        now.addAndGet(10 * 1000);
        client.advance();
        // Extend ack
        client.modifyAckDeadline(SUBSCRIPTION, ImmutableList.of(ACK_ID), 20);
        // Timeout on extended ACK
        now.addAndGet(30 * 1000);
        client.advance();
        incomingMessages = client.pull(now.get(), SUBSCRIPTION, 1, true);
        assertEquals(1, incomingMessages.size());
        assertEquals(
            IncomingMessage.of(
                expectedIncomingMessage.message(),
                expectedIncomingMessage.timestampMsSinceEpoch(),
                now.get(),
                expectedIncomingMessage.ackId(),
                expectedIncomingMessage.recordId()),
            incomingMessages.get(0));
        // Extend ack
        client.modifyAckDeadline(SUBSCRIPTION, ImmutableList.of(ACK_ID), 20);
        // Ack
        now.addAndGet(15 * 1000);
        client.advance();
        client.acknowledge(SUBSCRIPTION, ImmutableList.of(ACK_ID));
      }
    }
  }

  @Test
  public void publishOneMessage() throws IOException {
    OutgoingMessage expectedOutgoingMessage =
        OutgoingMessage.of(
            PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(DATA)).build(),
            MESSAGE_TIME,
            MESSAGE_ID,
            null);
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(
            TOPIC, Sets.newHashSet(expectedOutgoingMessage), ImmutableList.of())) {
      try (PubsubTestClient client = (PubsubTestClient) factory.newClient(null, null, null)) {
        client.publish(TOPIC, ImmutableList.of(expectedOutgoingMessage));
      }
    }
  }

  @Test
  public void testPullThenPublish() throws IOException {
    AtomicLong now = new AtomicLong();
    Clock clock = now::get;
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(DATA)).build();
    IncomingMessage expectedIncomingMessage =
        IncomingMessage.of(message, MESSAGE_TIME, REQ_TIME, ACK_ID, MESSAGE_ID);
    OutgoingMessage expectedOutgoingMessage =
        OutgoingMessage.of(message, MESSAGE_TIME, MESSAGE_ID, null);

    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPullAndPublish(
            SUBSCRIPTION,
            TOPIC,
            clock,
            ACK_TIMEOUT_S,
            ImmutableList.of(expectedIncomingMessage),
            ImmutableList.of(expectedOutgoingMessage),
            ImmutableList.of())) {
      try (PubsubTestClient client = (PubsubTestClient) factory.newClient(null, null, null)) {
        // Pull
        now.set(REQ_TIME);
        client.advance();
        List<IncomingMessage> actualIncomingMessages =
            client.pull(now.get(), SUBSCRIPTION, 1, true);
        now.addAndGet(ACK_TIMEOUT_S - 10);
        client.advance();
        client.acknowledge(SUBSCRIPTION, ImmutableList.of(ACK_ID));

        assertEquals(1, actualIncomingMessages.size());
        assertEquals(expectedIncomingMessage, actualIncomingMessages.get(0));

        // Publish
        IncomingMessage incomingMessage = actualIncomingMessages.get(0);
        OutgoingMessage actualOutgoingMessage =
            OutgoingMessage.of(
                incomingMessage.message(),
                incomingMessage.timestampMsSinceEpoch(),
                incomingMessage.recordId(),
                null);
        client.publish(TOPIC, ImmutableList.of(actualOutgoingMessage));
      }
    }
  }
}
