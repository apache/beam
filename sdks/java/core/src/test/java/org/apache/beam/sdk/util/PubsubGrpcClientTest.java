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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.List;
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
 * Tests for PubsubGrpcClient.
 */
@RunWith(JUnit4.class)
public class PubsubGrpcClientTest {
  private ManagedChannel mockChannel;
  private GoogleCredentials mockCredentials;
  private PublisherGrpc.PublisherBlockingStub mockPublisherStub;
  private SubscriberGrpc.SubscriberBlockingStub mockSubscriberStub;

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
    mockChannel = Mockito.mock(ManagedChannel.class);
    mockCredentials = Mockito.mock(GoogleCredentials.class);
    mockPublisherStub =
        Mockito.mock(PublisherGrpc.PublisherBlockingStub.class, Mockito.RETURNS_DEEP_STUBS);
    mockSubscriberStub =
        Mockito.mock(SubscriberGrpc.SubscriberBlockingStub.class, Mockito.RETURNS_DEEP_STUBS);
    client = new PubsubGrpcClient(TIMESTAMP_LABEL, ID_LABEL, 0, mockChannel,
                                  mockCredentials, mockPublisherStub, mockSubscriberStub);
  }

  @After
  public void teardown() throws IOException {
    client.close();
    client = null;
    mockChannel = null;
    mockCredentials = null;
    mockPublisherStub = null;
    mockSubscriberStub = null;
  }

  @Test
  public void pullOneMessage() throws IOException {
    String expectedSubscription = SUBSCRIPTION.getPath();
    PullRequest expectedRequest =
        PullRequest.newBuilder()
                   .setSubscription(expectedSubscription)
                   .setReturnImmediately(true)
                   .setMaxMessages(10)
                   .build();
    Timestamp timestamp = Timestamp.newBuilder()
                                   .setSeconds(PUB_TIME / 1000)
                                   .setNanos((int) (PUB_TIME % 1000) * 1000)
                                   .build();
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
                     .setMessageId(MESSAGE_ID)
                     .setData(
                         ByteString.copyFrom(DATA.getBytes()))
                     .setPublishTime(timestamp)
                     .putAllAttributes(
                         ImmutableMap.of(TIMESTAMP_LABEL,
                                         String.valueOf(MESSAGE_TIME),
                                         ID_LABEL, RECORD_ID))
                     .build();
    ReceivedMessage expectedReceivedMessage =
        ReceivedMessage.newBuilder()
                       .setMessage(expectedPubsubMessage)
                       .setAckId(ACK_ID)
                       .build();
    PullResponse expectedResponse =
        PullResponse.newBuilder()
                    .addAllReceivedMessages(ImmutableList.of(expectedReceivedMessage))
                    .build();
    Mockito.when(mockSubscriberStub.pull(expectedRequest))
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
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
                     .setData(ByteString.copyFrom(DATA.getBytes()))
                     .putAllAttributes(
                         ImmutableMap.of(TIMESTAMP_LABEL, String.valueOf(MESSAGE_TIME),
                                         ID_LABEL, RECORD_ID))
                     .build();
    PublishRequest expectedRequest =
        PublishRequest.newBuilder()
                      .setTopic(expectedTopic)
                      .addAllMessages(
                          ImmutableList.of(expectedPubsubMessage))
                      .build();
    PublishResponse expectedResponse =
        PublishResponse.newBuilder()
                       .addAllMessageIds(ImmutableList.of(MESSAGE_ID))
                       .build();
    Mockito.when(mockPublisherStub.publish(expectedRequest))
           .thenReturn(expectedResponse);
    OutgoingMessage actualMessage = new OutgoingMessage(DATA.getBytes(), MESSAGE_TIME, RECORD_ID);
    int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
    assertEquals(1, n);
  }
}
