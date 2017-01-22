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

import com.google.auth.Credentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubClient.TopicPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PubsubGrpcClient.
 */
@RunWith(JUnit4.class)
public class PubsubGrpcClientTest {
  private ManagedChannel inProcessChannel;
  private Credentials testCredentials;

  private PubsubClient client;
  private String channelName;

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
  private static final Map<String, String> ATTRIBUTES =
          ImmutableMap.<String, String>builder().put("a", "b").put("c", "d").build();

  @Before
  public void setup() {
    channelName = String.format("%s-%s",
        PubsubGrpcClientTest.class.getName(), ThreadLocalRandom.current().nextInt());
    inProcessChannel = InProcessChannelBuilder.forName(channelName).directExecutor().build();
    testCredentials = new TestCredential();
    client = new PubsubGrpcClient(TIMESTAMP_LABEL, ID_LABEL, 10, inProcessChannel, testCredentials);
  }

  @After
  public void teardown() throws IOException {
    client.close();
    inProcessChannel.shutdownNow();
  }

  @Test
  public void pullOneMessage() throws IOException {
    String expectedSubscription = SUBSCRIPTION.getPath();
    final PullRequest expectedRequest =
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
                     .putAllAttributes(ATTRIBUTES)
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
    final PullResponse response =
        PullResponse.newBuilder()
                    .addAllReceivedMessages(ImmutableList.of(expectedReceivedMessage))
                    .build();

    final List<PullRequest> requestsReceived = new ArrayList<>();
    SubscriberImplBase subscriberImplBase = new SubscriberImplBase() {
      @Override
      public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
        requestsReceived.add(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    };
    Server server = InProcessServerBuilder.forName(channelName)
        .addService(subscriberImplBase)
        .build()
        .start();
    try {
      List<IncomingMessage> acutalMessages = client.pull(REQ_TIME, SUBSCRIPTION, 10, true);
      assertEquals(1, acutalMessages.size());
      IncomingMessage actualMessage = acutalMessages.get(0);
      assertEquals(ACK_ID, actualMessage.ackId);
      assertEquals(DATA, new String(actualMessage.elementBytes));
      assertEquals(RECORD_ID, actualMessage.recordId);
      assertEquals(REQ_TIME, actualMessage.requestTimeMsSinceEpoch);
      assertEquals(MESSAGE_TIME, actualMessage.timestampMsSinceEpoch);
      assertEquals(expectedRequest, Iterables.getOnlyElement(requestsReceived));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void publishOneMessage() throws IOException {
    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
                     .setData(ByteString.copyFrom(DATA.getBytes()))
                     .putAllAttributes(ATTRIBUTES)
                     .putAllAttributes(
                         ImmutableMap.of(TIMESTAMP_LABEL, String.valueOf(MESSAGE_TIME),
                                         ID_LABEL, RECORD_ID))
                     .build();
    final PublishRequest expectedRequest =
        PublishRequest.newBuilder()
                      .setTopic(expectedTopic)
                      .addAllMessages(
                          ImmutableList.of(expectedPubsubMessage))
                      .build();
    final PublishResponse response =
        PublishResponse.newBuilder()
                       .addAllMessageIds(ImmutableList.of(MESSAGE_ID))
                       .build();

    final List<PublishRequest> requestsReceived = new ArrayList<>();
    PublisherImplBase publisherImplBase = new PublisherImplBase() {
      @Override
      public void publish(
          PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
        requestsReceived.add(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    };
    Server server = InProcessServerBuilder.forName(channelName)
        .addService(publisherImplBase)
        .build()
        .start();
    try {
      OutgoingMessage actualMessage = new OutgoingMessage(
              DATA.getBytes(), ATTRIBUTES, MESSAGE_TIME, RECORD_ID);
      int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
      assertEquals(1, n);
      assertEquals(expectedRequest, Iterables.getOnlyElement(requestsReceived));
    } finally {
      server.shutdownNow();
    }
  }
}
