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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.Pubsub.Projects.Subscriptions;
import com.google.api.services.pubsub.Pubsub.Projects.Topics;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Schema;
import com.google.api.services.pubsub.model.SchemaSettings;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for PubsubJsonClient. */
@RunWith(JUnit4.class)
public class PubsubJsonClientTest {
  private Pubsub mockPubsub;
  private PubsubClient client;

  private static final ProjectPath PROJECT = PubsubClient.projectPathFromId("testProject");
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");

  private static final PubsubClient.SchemaPath SCHEMA =
      PubsubClient.schemaPathFromId("testProject", "testSchemaId");
  private static final long REQ_TIME = 1234L;
  private static final long PUB_TIME = 3456L;
  private static final long MESSAGE_TIME = 6789L;
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final String MESSAGE_ID = "testMessageId";
  private static final String DATA = "testData";
  private static final String RECORD_ID = "testRecordId";
  private static final String ACK_ID = "testAckId";
  private static final String ORDERING_KEY = "testOrderingKey";

  @Before
  public void setup() {
    mockPubsub = Mockito.mock(Pubsub.class, Mockito.RETURNS_DEEP_STUBS);
    client = new PubsubJsonClient(TIMESTAMP_ATTRIBUTE, ID_ATTRIBUTE, mockPubsub);
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
    PullRequest expectedRequest = new PullRequest().setReturnImmediately(true).setMaxMessages(10);
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .setMessageId(MESSAGE_ID)
            .encodeData(DATA.getBytes(StandardCharsets.UTF_8))
            .setPublishTime(String.valueOf(PUB_TIME))
            .setAttributes(
                ImmutableMap.of(
                    TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME), ID_ATTRIBUTE, RECORD_ID))
            .setOrderingKey(ORDERING_KEY);
    ReceivedMessage expectedReceivedMessage =
        new ReceivedMessage().setMessage(expectedPubsubMessage).setAckId(ACK_ID);
    PullResponse expectedResponse =
        new PullResponse().setReceivedMessages(ImmutableList.of(expectedReceivedMessage));
    when((Object)
            mockPubsub
                .projects()
                .subscriptions()
                .pull(expectedSubscription, expectedRequest)
                .execute())
        .thenReturn(expectedResponse);
    List<IncomingMessage> acutalMessages = client.pull(REQ_TIME, SUBSCRIPTION, 10, true);
    assertEquals(1, acutalMessages.size());
    IncomingMessage actualMessage = acutalMessages.get(0);
    assertEquals(ACK_ID, actualMessage.ackId());
    assertEquals(DATA, actualMessage.message().getData().toStringUtf8());
    assertEquals(RECORD_ID, actualMessage.recordId());
    assertEquals(REQ_TIME, actualMessage.requestTimeMsSinceEpoch());
    assertEquals(MESSAGE_TIME, actualMessage.timestampMsSinceEpoch());
    assertEquals(ORDERING_KEY, actualMessage.message().getOrderingKey());
  }

  @Test
  public void pullOneMessageEmptyAttributes() throws IOException {
    client = new PubsubJsonClient(null, null, mockPubsub);
    String expectedSubscription = SUBSCRIPTION.getPath();
    PullRequest expectedRequest = new PullRequest().setReturnImmediately(true).setMaxMessages(10);
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .setMessageId(MESSAGE_ID)
            .encodeData(DATA.getBytes(StandardCharsets.UTF_8))
            .setPublishTime(String.valueOf(PUB_TIME));
    ReceivedMessage expectedReceivedMessage =
        new ReceivedMessage().setMessage(expectedPubsubMessage).setAckId(ACK_ID);
    PullResponse expectedResponse =
        new PullResponse().setReceivedMessages(ImmutableList.of(expectedReceivedMessage));
    when((Object)
            mockPubsub
                .projects()
                .subscriptions()
                .pull(expectedSubscription, expectedRequest)
                .execute())
        .thenReturn(expectedResponse);
    List<IncomingMessage> acutalMessages = client.pull(REQ_TIME, SUBSCRIPTION, 10, true);
    assertEquals(1, acutalMessages.size());
    IncomingMessage actualMessage = acutalMessages.get(0);
    assertEquals(ACK_ID, actualMessage.ackId());
    assertEquals(DATA, actualMessage.message().getData().toStringUtf8());
    assertEquals(REQ_TIME, actualMessage.requestTimeMsSinceEpoch());
    assertEquals(PUB_TIME, actualMessage.timestampMsSinceEpoch());
  }

  @Test
  public void pullOneMessageWithNoData() throws IOException {
    String expectedSubscription = SUBSCRIPTION.getPath();
    PullRequest expectedRequest = new PullRequest().setReturnImmediately(true).setMaxMessages(10);
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .setMessageId(MESSAGE_ID)
            .setPublishTime(String.valueOf(PUB_TIME))
            .setAttributes(
                ImmutableMap.of(
                    TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME), ID_ATTRIBUTE, RECORD_ID));
    ReceivedMessage expectedReceivedMessage =
        new ReceivedMessage().setMessage(expectedPubsubMessage).setAckId(ACK_ID);
    PullResponse expectedResponse =
        new PullResponse().setReceivedMessages(ImmutableList.of(expectedReceivedMessage));
    Mockito.when(
            (Object)
                mockPubsub
                    .projects()
                    .subscriptions()
                    .pull(expectedSubscription, expectedRequest)
                    .execute())
        .thenReturn(expectedResponse);
    List<IncomingMessage> acutalMessages = client.pull(REQ_TIME, SUBSCRIPTION, 10, true);
    assertEquals(1, acutalMessages.size());
    IncomingMessage actualMessage = acutalMessages.get(0);
    assertArrayEquals(new byte[0], actualMessage.message().getData().toByteArray());
  }

  @Test
  public void publishOneMessage() throws IOException {
    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .encodeData(DATA.getBytes(StandardCharsets.UTF_8))
            .setAttributes(
                ImmutableMap.<String, String>builder()
                    .put(TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME))
                    .put(ID_ATTRIBUTE, RECORD_ID)
                    .put("k", "v")
                    .build())
            .set("orderingKey", ORDERING_KEY);
    PublishRequest expectedRequest =
        new PublishRequest().setMessages(ImmutableList.of(expectedPubsubMessage));
    PublishResponse expectedResponse =
        new PublishResponse().setMessageIds(ImmutableList.of(MESSAGE_ID));
    when((Object) mockPubsub.projects().topics().publish(expectedTopic, expectedRequest).execute())
        .thenReturn(expectedResponse);
    Map<String, String> attrs = new HashMap<>();
    attrs.put("k", "v");
    OutgoingMessage actualMessage =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(DATA))
                .putAllAttributes(attrs)
                .setOrderingKey(ORDERING_KEY)
                .build(),
            MESSAGE_TIME,
            RECORD_ID,
            null);
    int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
    assertEquals(1, n);
  }

  @Test
  public void publishOneMessageWithOnlyTimestampAndIdAttributes() throws IOException {
    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .encodeData(DATA.getBytes(StandardCharsets.UTF_8))
            .setAttributes(
                ImmutableMap.<String, String>builder()
                    .put(TIMESTAMP_ATTRIBUTE, String.valueOf(MESSAGE_TIME))
                    .put(ID_ATTRIBUTE, RECORD_ID)
                    .build());
    PublishRequest expectedRequest =
        new PublishRequest().setMessages(ImmutableList.of(expectedPubsubMessage));
    PublishResponse expectedResponse =
        new PublishResponse().setMessageIds(ImmutableList.of(MESSAGE_ID));
    when((Object) mockPubsub.projects().topics().publish(expectedTopic, expectedRequest).execute())
        .thenReturn(expectedResponse);
    OutgoingMessage actualMessage =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(DATA))
                .build(),
            MESSAGE_TIME,
            RECORD_ID,
            null);
    int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
    assertEquals(1, n);
  }

  @Test
  public void publishOneMessageWithNoTimestampOrIdAttribute() throws IOException {
    // For this test, create a new PubsubJsonClient without the timestamp attribute
    // or id attribute set.
    client = new PubsubJsonClient(null, null, mockPubsub);

    String expectedTopic = TOPIC.getPath();
    PubsubMessage expectedPubsubMessage =
        new PubsubMessage()
            .encodeData(DATA.getBytes(StandardCharsets.UTF_8))
            .setAttributes(ImmutableMap.<String, String>builder().put("k", "v").build());
    PublishRequest expectedRequest =
        new PublishRequest().setMessages(ImmutableList.of(expectedPubsubMessage));
    PublishResponse expectedResponse =
        new PublishResponse().setMessageIds(ImmutableList.of(MESSAGE_ID));
    when((Object) mockPubsub.projects().topics().publish(expectedTopic, expectedRequest).execute())
        .thenReturn(expectedResponse);
    Map<String, String> attrs = new HashMap<>();
    attrs.put("k", "v");
    OutgoingMessage actualMessage =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(DATA))
                .putAllAttributes(attrs)
                .build(),
            MESSAGE_TIME,
            RECORD_ID,
            null);
    int n = client.publish(TOPIC, ImmutableList.of(actualMessage));
    assertEquals(1, n);
  }

  @Test
  public void listTopics() throws Exception {
    ListTopicsResponse expectedResponse1 = new ListTopicsResponse();
    expectedResponse1.setTopics(Collections.singletonList(buildTopic(1)));
    expectedResponse1.setNextPageToken("AVgJH3Z7aHxiDBs");

    ListTopicsResponse expectedResponse2 = new ListTopicsResponse();
    expectedResponse2.setTopics(Collections.singletonList(buildTopic(2)));

    Topics.List request = mockPubsub.projects().topics().list(PROJECT.getPath());
    when((Object) request.execute()).thenReturn(expectedResponse1, expectedResponse2);

    List<TopicPath> topicPaths = client.listTopics(PROJECT);
    assertEquals(2, topicPaths.size());
  }

  private static Topic buildTopic(int i) {
    Topic topic = new Topic();
    topic.setName(PubsubClient.topicPathFromName(PROJECT.getId(), "Topic" + i).getPath());
    return topic;
  }

  @Test
  public void listSubscriptions() throws Exception {
    ListSubscriptionsResponse expectedResponse1 = new ListSubscriptionsResponse();
    expectedResponse1.setSubscriptions(Collections.singletonList(buildSubscription(1)));
    expectedResponse1.setNextPageToken("AVgJH3Z7aHxiDBs");

    ListSubscriptionsResponse expectedResponse2 = new ListSubscriptionsResponse();
    expectedResponse2.setSubscriptions(Collections.singletonList(buildSubscription(2)));

    Subscriptions.List request = mockPubsub.projects().subscriptions().list(PROJECT.getPath());
    when((Object) request.execute()).thenReturn(expectedResponse1, expectedResponse2);

    final TopicPath topic101 = PubsubClient.topicPathFromName("testProject", "Topic2");
    List<SubscriptionPath> subscriptionPaths = client.listSubscriptions(PROJECT, topic101);
    assertEquals(1, subscriptionPaths.size());
  }

  private static Subscription buildSubscription(int i) {
    Subscription subscription = new Subscription();
    subscription.setName(
        PubsubClient.subscriptionPathFromName(PROJECT.getId(), "Subscription" + i).getPath());
    subscription.setTopic(PubsubClient.topicPathFromName(PROJECT.getId(), "Topic" + i).getPath());
    return subscription;
  }

  @Test
  public void testGetSchemaPath() throws IOException {
    TopicPath topicDoesNotExist =
        PubsubClient.topicPathFromPath("projects/testProject/topics/idontexist");
    TopicPath topicExistsDeletedSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/deletedSchema");
    TopicPath topicExistsNoSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/noSchema");
    TopicPath topicExistsSchema =
        PubsubClient.topicPathFromPath("projects/testProject/topics/topicWithSchema");
    when(mockPubsub.projects().topics().get(topicDoesNotExist.getPath()).execute())
        .thenThrow(
            new IOException(
                String.format("topic does not exist: %s", topicDoesNotExist.getPath())));
    when(mockPubsub.projects().topics().get(topicExistsDeletedSchema.getPath()).execute())
        .thenReturn(
            new Topic()
                .setName(topicExistsDeletedSchema.getName())
                .setSchemaSettings(
                    new SchemaSettings().setSchema(PubsubClient.SchemaPath.DELETED_SCHEMA_PATH)));
    when(mockPubsub.projects().topics().get(topicExistsNoSchema.getPath()).execute())
        .thenReturn(new Topic().setName(topicExistsNoSchema.getName()));
    when(mockPubsub.projects().topics().get(topicExistsSchema.getPath()).execute())
        .thenReturn(
            new Topic()
                .setName(topicExistsSchema.getName())
                .setSchemaSettings(new SchemaSettings().setSchema(SCHEMA.getPath())));

    client = new PubsubJsonClient(null, null, mockPubsub);

    assertThrows(
        "topic does not exist", IOException.class, () -> client.getSchemaPath(topicDoesNotExist));

    assertNull("schema for topic is deleted", client.getSchemaPath(topicExistsDeletedSchema));

    assertNull("topic has no schema", client.getSchemaPath(topicExistsNoSchema));

    assertEquals(SCHEMA.getPath(), client.getSchemaPath(topicExistsSchema).getPath());
  }

  @Test
  public void testAvroSchema() throws IOException {
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
    Schema schema =
        new Schema().setName(SCHEMA.getPath()).setType("AVRO").setDefinition(schemaDefinition);
    when(mockPubsub.projects().schemas().get(SCHEMA.getPath()).execute()).thenReturn(schema);
    client = new PubsubJsonClient(null, null, mockPubsub);
    assertEquals(
        org.apache.beam.sdk.schemas.Schema.of(
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "StringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "FloatField", org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "BooleanField", org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN)),
        client.getSchema(SCHEMA));
  }

  @Test
  public void getProtoSchema() throws IOException {
    String schemaDefinition =
        "syntax = \"proto3\"; message ProtocolBuffer { string string_field = 1; int32 int_field = 2; }";
    Schema schema =
        new Schema()
            .setName(SCHEMA.getPath())
            .setType("PROTOCOL_BUFFER")
            .setDefinition(schemaDefinition);
    when(mockPubsub.projects().schemas().get(SCHEMA.getPath()).execute()).thenReturn(schema);
    client = new PubsubJsonClient(null, null, mockPubsub);
    assertThrows(
        "Pub/Sub Schema type PROTOCOL_BUFFER is not supported at this time",
        IllegalArgumentException.class,
        () -> client.getSchema(SCHEMA));
  }
}
