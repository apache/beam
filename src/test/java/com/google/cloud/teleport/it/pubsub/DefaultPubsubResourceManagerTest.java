/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.pubsub;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DefaultPubsubResourceManager}. */
@RunWith(JUnit4.class)
public final class DefaultPubsubResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String TEST_ID = "test-id";
  private static final String PROJECT_ID = "test-project";
  private static final String TOPIC_NAME = "test-topic-name";
  private static final String SUBSCRIPTION_NAME = "test-topic-name-sub0";
  private static final TopicName TOPIC_REFERENCE = TopicName.of("test-project", "test-topic");
  private static final String VALID_MESSAGE_ID = "abcdef";

  @Mock private TopicAdminClient topicAdminClient;
  @Mock private SubscriptionAdminClient subscriptionAdminClient;
  @Mock private Topic topic;
  @Mock private Subscription subscription;
  @Mock private Publisher publisher;
  @Mock private PubsubPublisherFactory publisherFactory;

  private DefaultPubsubResourceManager testManager;

  @Captor private ArgumentCaptor<TopicName> topicNameCaptor;
  @Captor private ArgumentCaptor<SubscriptionName> subscriptionNameCaptor;
  @Captor private ArgumentCaptor<PubsubMessage> pubsubMessageCaptor;

  @Before
  public void setUp() throws IOException {
    // Using spy to inject our mocked publisher at getPublisher(topic)
    testManager =
        new DefaultPubsubResourceManager(
            TEST_ID, PROJECT_ID, publisherFactory, topicAdminClient, subscriptionAdminClient);

    when(topic.getName()).thenReturn("projects/" + PROJECT_ID + "/topics/" + TOPIC_NAME);
    when(subscription.getName())
        .thenReturn("projects/" + PROJECT_ID + "/subscriptions/" + SUBSCRIPTION_NAME);
    when(publisherFactory.createPublisher(any())).thenReturn(publisher);
  }

  @Test
  public void testBuilderWithInvalidProjectShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DefaultPubsubResourceManager.builder("test-a", ""));
    assertThat(exception).hasMessageThat().contains("projectId can not be empty");
  }

  @Test
  public void testCreateTopicWithInvalidNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> testManager.createTopic(""));
    assertThat(exception).hasMessageThat().contains("topicName can not be empty");
  }

  @Test
  public void testCreateSubscriptionWithInvalidNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> testManager.createSubscription(TopicName.of(PROJECT_ID, "topic-a"), ""));
    assertThat(exception).hasMessageThat().contains("subscriptionName can not be empty");
  }

  @Test
  public void testCreateTopicShouldCreate() {
    when(topicAdminClient.createTopic(any(TopicName.class))).thenReturn(topic);

    TopicName createTopic = testManager.createTopic("topic-name");

    assertThat(createTopic).isNotNull();
    verify(topicAdminClient).createTopic(topicNameCaptor.capture());
    TopicName actualTopicName = topicNameCaptor.getValue();
    assertThat(actualTopicName.getProject()).isEqualTo(PROJECT_ID);
    assertThat(actualTopicName.getTopic()).matches(TEST_ID + "-\\d{17}-topic-name");
  }

  @Test
  public void testCreateSubscriptionShouldCreate() {
    when(topicAdminClient.createTopic(any(TopicName.class))).thenReturn(topic);
    when(subscriptionAdminClient.createSubscription(
            any(SubscriptionName.class), any(TopicName.class), any(), anyInt()))
        .thenReturn(subscription);

    TopicName createTopic = testManager.createTopic("topic-name");
    SubscriptionName createSub = testManager.createSubscription(createTopic, "subscription-name");

    assertThat(createSub).isNotNull();
    verify(subscriptionAdminClient)
        .createSubscription(
            subscriptionNameCaptor.capture(), topicNameCaptor.capture(), any(), anyInt());
    SubscriptionName subscriptionName = subscriptionNameCaptor.getValue();
    TopicName actualTopicName = topicNameCaptor.getValue();
    assertThat(subscriptionName.getProject()).isEqualTo(PROJECT_ID);
    assertThat(subscriptionName.getSubscription()).matches(TEST_ID + "-\\d{17}-subscription-name");
    assertThat(actualTopicName.getProject()).isEqualTo(PROJECT_ID);
    assertThat(actualTopicName.getTopic()).matches(createTopic.getTopic());
  }

  @Test
  public void testCreateSubscriptionUnmanagedTopicShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                testManager.createSubscription(
                    TopicName.of(PROJECT_ID, "topic-name"), "subscription-name"));
    assertThat(exception).hasMessageThat().contains("topic not managed");
  }

  @Test
  public void testPublishMessageShouldPublish() {
    when(topicAdminClient.createTopic(any(TopicName.class))).thenReturn(topic);
    when(publisher.publish(any())).thenReturn(ApiFutures.immediateFuture(VALID_MESSAGE_ID));
    Map<String, String> attributes = ImmutableMap.of("key1", "value1");
    ByteString data = ByteString.copyFromUtf8("valid message");

    TopicName topic = testManager.createTopic(TOPIC_NAME);
    String publishMessage = testManager.publish(topic, attributes, data);

    assertThat(publishMessage).isEqualTo(VALID_MESSAGE_ID);
    verify(publisher).publish(pubsubMessageCaptor.capture());
    PubsubMessage actualMessage = pubsubMessageCaptor.getValue();
    assertThat(actualMessage.getAttributesMap()).isEqualTo(attributes);
    assertThat(actualMessage.getData()).isEqualTo(data);
  }

  @Test
  public void testPublishMessageUnmanagedTopicShouldFail() {
    Map<String, String> attributes = ImmutableMap.of("key1", "value1");
    ByteString data = ByteString.copyFromUtf8("valid message");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> testManager.publish(TOPIC_REFERENCE, attributes, data));
    assertThat(exception).hasMessageThat().contains("topic not managed");
  }

  @Test
  public void testCleanupTopicsShouldDeleteTopics() {
    TopicName topicName1 = testManager.getTopicName("topic1");
    TopicName topicName2 = testManager.getTopicName("topic2");
    when(topic.getName()).thenReturn(topicName1.toString(), topicName2.toString());
    when(topicAdminClient.createTopic(any(TopicName.class))).thenReturn(topic);

    testManager.createTopic("topic1");
    testManager.createTopic("topic2");
    testManager.cleanupAll();

    verify(topicAdminClient, times(2)).deleteTopic(topicNameCaptor.capture());
    assertThat(topicNameCaptor.getAllValues()).hasSize(2);
    assertThat(topicNameCaptor.getAllValues()).containsExactly(topicName1, topicName2);
  }

  @Test
  public void testCleanupSubscriptionsShouldDeleteResources() {
    SubscriptionName subscriptionName1 = testManager.getSubscriptionName("topic1-sub0");
    SubscriptionName subscriptionName2 = testManager.getSubscriptionName("topic1-sub1");
    SubscriptionName subscriptionName3 = testManager.getSubscriptionName("topic2-sub0");
    when(topicAdminClient.createTopic(any(TopicName.class))).thenReturn(topic);
    when(subscription.getName())
        .thenReturn(
            subscriptionName1.toString(),
            subscriptionName2.toString(),
            subscriptionName3.toString());
    when(subscriptionAdminClient.createSubscription(
            any(SubscriptionName.class), any(TopicName.class), any(), anyInt()))
        .thenReturn(subscription);

    TopicName topic1 = testManager.createTopic("topic1");
    TopicName topic2 = testManager.createTopic("topic2");
    testManager.createSubscription(topic1, "topic1-sub0");
    testManager.createSubscription(topic1, "topic1-sub1");
    testManager.createSubscription(topic2, "topic2-sub0");
    testManager.cleanupAll();

    verify(subscriptionAdminClient, times(3)).deleteSubscription(subscriptionNameCaptor.capture());
    assertThat(subscriptionNameCaptor.getAllValues()).hasSize(3);
    assertThat(subscriptionNameCaptor.getAllValues())
        .containsExactly(subscriptionName1, subscriptionName2, subscriptionName3);
  }
}
