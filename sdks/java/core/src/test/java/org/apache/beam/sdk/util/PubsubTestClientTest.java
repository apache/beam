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

package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubClient.TopicPath;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Tests for PubsubTestClient.
 */
public class PubsubTestClientTest {
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final long REQ_TIME = 1234L;
  private static final long PUB_TIME = 3456L;
  private static final long MESSAGE_TIME = 6789L;
  private static final String MESSAGE_ID = "testMessageId";
  private static final String DATA = "testData";
  private static final String ACK_ID = "testAckId";
  private static final int ACK_TIMEOUT_S = 60;

  @Test
  public void pullOneMessage() throws IOException {
    IncomingMessage expectedIncomingMessage =
        new IncomingMessage(DATA.getBytes(), MESSAGE_TIME, REQ_TIME, ACK_ID, MESSAGE_ID.getBytes());
    try (PubsubTestClient client =
             new PubsubTestClient(null, SUBSCRIPTION, ACK_TIMEOUT_S, null,
                                  Lists.newArrayList(expectedIncomingMessage))) {
      long now = REQ_TIME;
      List<IncomingMessage> incomingMessages = client.pull(now, SUBSCRIPTION, 1, true);
      assertEquals(1, incomingMessages.size());
      assertEquals(expectedIncomingMessage, incomingMessages.get(0));
      // Timeout on ACK.
      now += (ACK_TIMEOUT_S + 10) * 1000;
      client.advanceTo(now);
      incomingMessages = client.pull(now, SUBSCRIPTION, 1, true);
      assertEquals(1, incomingMessages.size());
      assertEquals(expectedIncomingMessage.withRequestTime(now), incomingMessages.get(0));
      now += 10 * 1000;
      client.advanceTo(now);
      // Extend ack
      client.modifyAckDeadline(SUBSCRIPTION, ImmutableList.of(ACK_ID), 20);
      // Timeout on extended ACK
      now += 30 * 1000;
      client.advanceTo(now);
      incomingMessages = client.pull(now, SUBSCRIPTION, 1, true);
      assertEquals(1, incomingMessages.size());
      assertEquals(expectedIncomingMessage.withRequestTime(now), incomingMessages.get(0));
      // Extend ack
      client.modifyAckDeadline(SUBSCRIPTION, ImmutableList.of(ACK_ID), 20);
      // Ack
      now += 15 * 1000;
      client.advanceTo(now);
      client.acknowledge(SUBSCRIPTION, ImmutableList.of(ACK_ID));
    }
  }

  @Test
  public void publishOneMessage() throws IOException {
    OutgoingMessage expectedOutgoingMessage = new OutgoingMessage(DATA.getBytes(), MESSAGE_TIME);
    try (PubsubTestClient client =
             new PubsubTestClient(TOPIC, null, ACK_TIMEOUT_S,
                                  Sets.newHashSet(expectedOutgoingMessage), null)) {
      client.publish(TOPIC, ImmutableList.of(expectedOutgoingMessage));
    }
  }
}
