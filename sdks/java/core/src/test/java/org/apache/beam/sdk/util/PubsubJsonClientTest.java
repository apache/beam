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

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubClient.TopicPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Tests for PubsubJsonClient.
 */
@RunWith(JUnit4.class)
public class PubsubJsonClientTest {
  private Pubsub mockPubsub;
  private PubsubClient client;

  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final long REQ_TIME = 1234L;
  private static final long PUB_TIME = 3456L;
  private static final long MESSAGE_TIME = 6789L;
  private static final String TIMESTAMP_LABEL = "timestamp";
  private static final String ID_LABEL = "id";
  private static final String MESSAGE_ID = "testMessageId";
  private static final String DATA = "testData";
  private static final String RECORD_ID = "testRecordId";
  private static final String ACK_ID = "testAckId";

  @Before
  public void setup() throws IOException {
    mockPubsub = Mockito.mock(Pubsub.class, Mockito.RETURNS_DEEP_STUBS);
    client = new PubsubJsonClient(TIMESTAMP_LABEL, ID_LABEL, mockPubsub);
  }

  @After
  public void teardown() throws IOException {
    client.close();
    client = null;
    mockPubsub = null;
  }

  @Test
  public void pullOneMessage() throws IOException {
    String expectedSubscription = SUBSCRIPTION.getPath();
    PullRequest expectedRequest =
        new PullRequest().setReturnImmediately(true).setMaxMessages(10);
    PubsubMessage expectedPubsubMessage = new PubsubMessage()
        .setMessageId(MESSAGE_ID)
        .encodeData(DATA.getBytes())
        .setPublishTime(String.valueOf(PUB_TIME))
        .setAttributes(
            ImmutableMap.of(TIMESTAMP_LABEL, String.valueOf(MESSAGE_TIME),
                            ID_LABEL, RECORD_ID));
    ReceivedMessage expectedReceivedMessage =
        new ReceivedMessage().setMessage(expectedPubsubMessage)
                             .setAckId(ACK_ID);
    PullResponse expectedResponse =
        new PullResponse().setReceivedMessages(ImmutableList.of(expectedReceivedMessage));
    Mockito.when((Object) (mockPubsub.projects()
                               .subscriptions()
                               .pull(expectedSubscription, expectedRequest)
                               .execute()))
           .thenReturn(expectedResponse);
    List<IncomingMessage> acutalMessages = client.pull(REQ_TIME, SUBSCRIPTION, 10, true);
    assertEquals(1, acutalMessages.size());
    IncomingMessage actualMessage = acutalMessages.get(0);
    assertEquals(ACK_ID, actualMessage.ackId);
    assertEquals(DATA, new String(actualMessage.elementBytes));
    assertEquals(RECORD_ID, actualMessage.recordId);
    assertEquals(REQ_TIME, actualMessage.requestTimeMsSinceEpoch);
    assertEquals(MESSAGE_TIME, actualMessage.timestampMsSinceEpoch);
  }

  @Test
  public void publishOneMessage() throws IOException {
    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage = new PubsubMessage()
        .encodeData(DATA.getBytes())
        .setAttributes(
            ImmutableMap.<String, String> builder()
                    .put(TIMESTAMP_LABEL, String.valueOf(MESSAGE_TIME))
                    .put(ID_LABEL, RECORD_ID)
                    .put("k", "v").build());
    PublishRequest expectedRequest = new PublishRequest()
        .setMessages(ImmutableList.of(expectedPubsubMessage));
    PublishResponse expectedResponse = new PublishResponse()
        .setMessageIds(ImmutableList.of(MESSAGE_ID));
    Mockito.when((Object) (mockPubsub.projects()
                                .topics()
                                .publish(expectedTopic, expectedRequest)
                                .execute()))
           .thenReturn(expectedResponse);
    Map<String, String> attrs = new HashMap<>();
    attrs.put("k", "v");
    OutgoingMessage actualMessage = new OutgoingMessage(
            DATA.getBytes(), attrs, MESSAGE_TIME, RECORD_ID);
    int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
    assertEquals(1, n);
  }
}
