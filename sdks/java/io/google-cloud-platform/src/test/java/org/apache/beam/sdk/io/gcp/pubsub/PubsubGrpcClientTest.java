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

import com.google.auth.Credentials;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.GetSchemaRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaServiceGrpc.SchemaServiceImplBase;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Topic;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SchemaPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PubsubGrpcClient. */
@RunWith(JUnit4.class)
public class PubsubGrpcClientTest {
  private ManagedChannel inProcessChannel;

  private PubsubClient client;
  private String channelName;

  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final SchemaPath SCHEMA =
      PubsubClient.schemaPathFromId("testProject", "testSchemaId");
  private static final long REQ_TIME_MS = 1234L;
  private static final long PUB_TIME_MS = 3456L;
  private static final long MESSAGE_TIME_MS = 6789L;
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final String MESSAGE_ID = "testMessageId";
  private static final String DATA = "testData";
  private static final String RECORD_ID = "testRecordId";
  private static final String ACK_ID = "testAckId";
  private static final ImmutableMap<String, String> ATTRIBUTES =
      ImmutableMap.<String, String>builder().put("a", "b").put("c", "d").build();

  @Before
  public void setup() {
    channelName =
        String.format(
            "%s-%s", PubsubGrpcClientTest.class.getName(), ThreadLocalRandom.current().nextInt());
    inProcessChannel = InProcessChannelBuilder.forName(channelName).directExecutor().build();
  }

  protected void initializeClient(
      @Nullable String timestampAttribute, @Nullable String idAttribute) {
    Credentials testCredentials = new TestCredential();
    client =
        new PubsubGrpcClient(
            timestampAttribute, idAttribute, 10, inProcessChannel, testCredentials);
  }

  @After
  public void teardown() throws IOException {
    client.close();
    inProcessChannel.shutdownNow();
  }

  @Test
  public void pullOneMessage() throws IOException {
    initializeClient(null, null);
    String expectedSubscription = SUBSCRIPTION.getPath();
    final PullRequest expectedRequest =
        PullRequest.newBuilder()
            .setSubscription(expectedSubscription)
            .setReturnImmediately(true)
            .setMaxMessages(10)
            .build();
    Timestamp timestamp =
        Timestamp.newBuilder()
            .setSeconds(PUB_TIME_MS / 1000)
            .setNanos((int) (PUB_TIME_MS % 1000) * 1000 * 1000)
            .build();
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
            .setMessageId(MESSAGE_ID)
            .setData(ByteString.copyFrom(DATA.getBytes(StandardCharsets.UTF_8)))
            .setPublishTime(timestamp)
            .build();
    ReceivedMessage expectedReceivedMessage =
        ReceivedMessage.newBuilder().setMessage(expectedPubsubMessage).setAckId(ACK_ID).build();
    final PullResponse response =
        PullResponse.newBuilder()
            .addAllReceivedMessages(ImmutableList.of(expectedReceivedMessage))
            .build();

    final List<PullRequest> requestsReceived = new ArrayList<>();
    SubscriberImplBase subscriberImplBase =
        new SubscriberImplBase() {
          @Override
          public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
            requestsReceived.add(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(subscriberImplBase).build().start();
    try {
      List<IncomingMessage> actualMessages = client.pull(REQ_TIME_MS, SUBSCRIPTION, 10, true);
      assertEquals(1, actualMessages.size());
      IncomingMessage actualMessage = actualMessages.get(0);
      assertEquals(ACK_ID, actualMessage.ackId());
      assertEquals(DATA, actualMessage.message().getData().toStringUtf8());
      assertEquals(MESSAGE_ID, actualMessage.recordId());
      assertEquals(REQ_TIME_MS, actualMessage.requestTimeMsSinceEpoch());
      assertEquals(PUB_TIME_MS, actualMessage.timestampMsSinceEpoch());
      assertEquals(expectedRequest, Iterables.getOnlyElement(requestsReceived));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void pullOneMessageUsingAttributes() throws IOException {
    initializeClient(TIMESTAMP_ATTRIBUTE, ID_ATTRIBUTE);
    String expectedSubscription = SUBSCRIPTION.getPath();
    final PullRequest expectedRequest =
        PullRequest.newBuilder()
            .setSubscription(expectedSubscription)
            .setReturnImmediately(true)
            .setMaxMessages(10)
            .build();
    Timestamp timestamp =
        Timestamp.newBuilder()
            .setSeconds(PUB_TIME_MS / 1000)
            .setNanos((int) (PUB_TIME_MS % 1000) * 1000 * 1000)
            .build();
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
            .setMessageId(MESSAGE_ID)
            .setData(ByteString.copyFrom(DATA.getBytes(StandardCharsets.UTF_8)))
            .setPublishTime(timestamp)
            .putAllAttributes(ATTRIBUTES)
            .putAllAttributes(
                ImmutableMap.of(
                    TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME_MS), ID_ATTRIBUTE, RECORD_ID))
            .build();
    ReceivedMessage expectedReceivedMessage =
        ReceivedMessage.newBuilder().setMessage(expectedPubsubMessage).setAckId(ACK_ID).build();
    final PullResponse response =
        PullResponse.newBuilder()
            .addAllReceivedMessages(ImmutableList.of(expectedReceivedMessage))
            .build();

    final List<PullRequest> requestsReceived = new ArrayList<>();
    SubscriberImplBase subscriberImplBase =
        new SubscriberImplBase() {
          @Override
          public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
            requestsReceived.add(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(subscriberImplBase).build().start();
    try {
      List<IncomingMessage> actualMessages = client.pull(REQ_TIME_MS, SUBSCRIPTION, 10, true);
      assertEquals(1, actualMessages.size());
      IncomingMessage actualMessage = actualMessages.get(0);
      assertEquals(ACK_ID, actualMessage.ackId());
      assertEquals(DATA, actualMessage.message().getData().toStringUtf8());
      assertEquals(RECORD_ID, actualMessage.recordId());
      assertEquals(REQ_TIME_MS, actualMessage.requestTimeMsSinceEpoch());
      assertEquals(MESSAGE_TIME_MS, actualMessage.timestampMsSinceEpoch());
      assertEquals(expectedRequest, Iterables.getOnlyElement(requestsReceived));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void publishOneMessage() throws IOException {
    initializeClient(TIMESTAMP_ATTRIBUTE, ID_ATTRIBUTE);
    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(DATA.getBytes(StandardCharsets.UTF_8)))
            .putAllAttributes(ATTRIBUTES)
            .putAllAttributes(
                ImmutableMap.of(
                    TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME_MS), ID_ATTRIBUTE, RECORD_ID))
            .build();
    final PublishRequest expectedRequest =
        PublishRequest.newBuilder()
            .setTopic(expectedTopic)
            .addAllMessages(ImmutableList.of(expectedPubsubMessage))
            .build();
    final PublishResponse response =
        PublishResponse.newBuilder().addAllMessageIds(ImmutableList.of(MESSAGE_ID)).build();

    final List<PublishRequest> requestsReceived = new ArrayList<>();
    PublisherImplBase publisherImplBase =
        new PublisherImplBase() {
          @Override
          public void publish(
              PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
            requestsReceived.add(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(publisherImplBase).build().start();
    try {
      OutgoingMessage actualMessage =
          OutgoingMessage.of(
              com.google.pubsub.v1.PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(DATA))
                  .putAllAttributes(ATTRIBUTES)
                  .build(),
              MESSAGE_TIME_MS,
              RECORD_ID,
              null);
      int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
      assertEquals(1, n);
      assertEquals(expectedRequest, Iterables.getOnlyElement(requestsReceived));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void getSchemaPath() throws IOException {
    initializeClient(null, null);
    TopicPath topicDoesNotExist =
        PubsubClient.topicPathFromPath("projects/testProject/topics/idontexist");
    TopicPath topicExistsDeletedSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/deletedSchema");
    TopicPath topicExistsNoSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/noSchema");
    TopicPath topicExistsSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/topicWithSchema");
    PublisherImplBase publisherImplBase =
        new PublisherImplBase() {
          @Override
          public void getTopic(GetTopicRequest request, StreamObserver<Topic> responseObserver) {
            String topicPath = request.getTopic();
            if (topicPath.equals(topicDoesNotExist.getPath())) {
              responseObserver.onError(
                  new IOException(String.format("%s does not exist", topicPath)));
            }
            if (topicPath.equals(topicExistsDeletedSchema.getPath())) {
              responseObserver.onNext(
                  Topic.newBuilder()
                      .setName(topicPath)
                      .setSchemaSettings(
                          SchemaSettings.newBuilder()
                              .setSchema(SchemaPath.DELETED_SCHEMA_PATH)
                              .build())
                      .build());
              responseObserver.onCompleted();
            }
            if (topicPath.equals(topicExistsNoSchema.getPath())) {
              responseObserver.onNext(Topic.newBuilder().setName(topicPath).build());
              responseObserver.onCompleted();
            }
            if (topicPath.equals(topicExistsSchema.getPath())) {
              responseObserver.onNext(
                  Topic.newBuilder()
                      .setName(topicPath)
                      .setSchemaSettings(
                          SchemaSettings.newBuilder().setSchema(SCHEMA.getPath()).build())
                      .build());
              responseObserver.onCompleted();
            }
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(publisherImplBase).build().start();
    try {
      assertThrows(
          "topic does not exist",
          StatusRuntimeException.class,
          () -> client.getSchemaPath(topicDoesNotExist));

      assertNull(
          "topic with deleted Schema should return null SchemaPath",
          client.getSchemaPath(topicExistsDeletedSchema));

      assertNull(
          "topic without Schema should return null SchemaPath",
          client.getSchemaPath(topicExistsNoSchema));

      assertEquals(SCHEMA.getPath(), client.getSchemaPath(topicExistsSchema).getPath());

    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void getAvroSchema() throws IOException {
    String schemaDefinition =
        "{"
            + " \"type\" : \"record\","
            + " \"name\" : \"Avro\","
            + " \"fields\" : ["
            + "   {"
            + "     \"name\" : \"StringField\","
            + "     \"type\" : \"string\""
            + "   },"
            + "   {"
            + "     \"name\" : \"FloatField\","
            + "     \"type\" : \"float\""
            + "   },"
            + "   {"
            + "     \"name\" : \"BooleanField\","
            + "     \"type\" : \"boolean\""
            + "   }"
            + " ]"
            + "}";
    initializeClient(null, null);
    final Schema schema =
        com.google.pubsub.v1.Schema.newBuilder()
            .setName(SCHEMA.getPath())
            .setType(Schema.Type.AVRO)
            .setDefinition(schemaDefinition)
            .build();
    SchemaServiceImplBase schemaImplBase =
        new SchemaServiceImplBase() {
          @Override
          public void getSchema(GetSchemaRequest request, StreamObserver<Schema> responseObserver) {
            if (request.getName().equals(SCHEMA.getPath())) {
              responseObserver.onNext(schema);
              responseObserver.onCompleted();
            }
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(schemaImplBase).build().start();
    try {
      assertEquals(
          org.apache.beam.sdk.schemas.Schema.of(
              Field.of("StringField", FieldType.STRING),
              Field.of("FloatField", FieldType.FLOAT),
              Field.of("BooleanField", FieldType.BOOLEAN)),
          client.getSchema(SCHEMA));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void getProtoSchema() throws IOException {
    String schemaDefinition =
        "syntax = \"proto3\"; message ProtocolBuffer { string string_field = 1; int32 int_field = 2; }";
    initializeClient(null, null);
    final Schema schema =
        com.google.pubsub.v1.Schema.newBuilder()
            .setName(SCHEMA.getPath())
            .setType(Schema.Type.PROTOCOL_BUFFER)
            .setDefinition(schemaDefinition)
            .build();
    SchemaServiceImplBase schemaImplBase =
        new SchemaServiceImplBase() {
          @Override
          public void getSchema(GetSchemaRequest request, StreamObserver<Schema> responseObserver) {
            if (request.getName().equals(SCHEMA.getPath())) {
              responseObserver.onNext(schema);
              responseObserver.onCompleted();
            }
          }
        };
    Server server =
        InProcessServerBuilder.forName(channelName).addService(schemaImplBase).build().start();
    try {
      assertThrows(
          "Pub/Sub Schema type PROTOCOL_BUFFER is not supported at this time",
          IllegalArgumentException.class,
          () -> client.getSchema(SCHEMA));
    } finally {
      server.shutdownNow();
    }
  }
}
