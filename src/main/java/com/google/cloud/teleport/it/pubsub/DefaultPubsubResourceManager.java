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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link PubsubResourceManager} interface.
 *
 * <p>The class provides an interaction with the real Pub/Sub client, with operations related to
 * management of topics and subscriptions.
 */
public final class DefaultPubsubResourceManager implements PubsubResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPubsubResourceManager.class);

  private static final int DEFAULT_ACK_DEADLINE_SECONDS = 600;
  private static final String RESOURCE_NAME_SEPARATOR = "-";

  private final String testId;
  private final String projectId;
  private final PubsubPublisherFactory publisherFactory;
  private final TopicAdminClient topicAdminClient;
  private final SubscriptionAdminClient subscriptionAdminClient;

  private final Set<TopicName> createdTopics;
  private final Set<SubscriptionName> createdSubscriptions;

  private DefaultPubsubResourceManager(Builder builder) throws IOException {
    this(
        builder.testName,
        builder.projectId,
        new DefaultPubsubPublisherFactory(builder.credentialsProvider),
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()),
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()));
  }

  @VisibleForTesting
  DefaultPubsubResourceManager(
      String testName,
      String projectId,
      PubsubPublisherFactory publisherFactory,
      TopicAdminClient topicAdminClient,
      SubscriptionAdminClient subscriptionAdminClient) {
    this.projectId = projectId;
    this.testId = PubsubUtils.createTestId(testName);
    this.publisherFactory = publisherFactory;
    this.topicAdminClient = topicAdminClient;
    this.subscriptionAdminClient = subscriptionAdminClient;
    this.createdTopics = Collections.synchronizedSet(new HashSet<>());
    this.createdSubscriptions = Collections.synchronizedSet(new HashSet<>());
  }

  public static Builder builder(String testName, String projectId) {
    checkArgument(!Strings.isNullOrEmpty(testName), "testName can not be null or empty");
    checkArgument(!projectId.isEmpty(), "projectId can not be empty");
    return new Builder(testName, projectId);
  }

  /**
   * Create a topic based on the given topic name. However, it adds a prefix based on the test id
   * and timestamp, to uniquely identify the current execution instance.
   *
   * @see PubsubResourceManager#createTopic(String)
   */
  @Override
  public TopicName createTopic(String topicName) {
    checkArgument(!topicName.isEmpty(), "topicName can not be empty");
    checkIsUsable();

    LOG.info("Creating topic '{}'...", topicName);

    Topic topic = topicAdminClient.createTopic(getTopicName(topicName));
    TopicName reference = PubsubUtils.toTopicName(topic);
    createdTopics.add(reference);

    LOG.info("Topic '{}' was created successfully!", topicName);

    return reference;
  }

  /**
   * Create a subscription based on the given name. However, it adds a prefix based on the test id
   * and timestamp, to uniquely identify the current execution instance.
   *
   * @see PubsubResourceManager#createSubscription(TopicName, String)
   */
  @Override
  public SubscriptionName createSubscription(TopicName topicName, String subscriptionName) {
    checkArgument(!subscriptionName.isEmpty(), "subscriptionName can not be empty");
    checkIsUsable();

    if (!createdTopics.contains(topicName)) {
      throw new IllegalArgumentException(
          "Can not create a subscription for a topic not managed by this instance.");
    }

    LOG.info("Creating subscription '{}' for topic '{}'", subscriptionName, topicName);

    Subscription subscription =
        subscriptionAdminClient.createSubscription(
            getSubscriptionName(subscriptionName),
            topicName,
            PushConfig.getDefaultInstance(),
            DEFAULT_ACK_DEADLINE_SECONDS);
    SubscriptionName reference = PubsubUtils.toSubscriptionName(subscription);
    createdSubscriptions.add(reference);

    LOG.info(
        "Subscription '{}' for topic '{}' was created successfully!", topicName, subscriptionName);

    return reference;
  }

  @Override
  public String publish(TopicName topic, Map<String, String> attributes, ByteString data)
      throws PubsubResourceManagerException {
    checkIsUsable();

    if (!createdTopics.contains(topic)) {
      throw new IllegalArgumentException(
          "Can not publish to a topic not managed by this instance.");
    }

    LOG.info("Publishing message with {} bytes to topic '{}'", data.size(), topic);

    PubsubMessage pubsubMessage =
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(data).build();

    try {
      Publisher publisher = publisherFactory.createPublisher(topic);
      String messageId = publisher.publish(pubsubMessage).get();
      LOG.info("Message published with id '{}'", messageId);
      publisher.shutdown();
      return messageId;
    } catch (Exception e) {
      throw new PubsubResourceManagerException("Error publishing message to Pubsub", e);
    }
  }

  @Override
  public PullResponse pull(SubscriptionName subscriptionName, int maxMessages) {
    LOG.info("Pulling messages from subscription '{}'", subscriptionName);
    PullResponse response = subscriptionAdminClient.pull(subscriptionName, maxMessages);
    LOG.info(
        "Received {} messages from subscription '{}'",
        response.getReceivedMessagesCount(),
        subscriptionName);
    return response;
  }

  @Override
  public synchronized void cleanupAll() {
    // Ignore call if it was cleaned up before
    if (isNotUsable()) {
      return;
    }

    LOG.info("Attempting to cleanup manager.");

    try {
      for (SubscriptionName subscription : createdSubscriptions) {
        LOG.info("Deleting subscription '{}'", subscription);
        subscriptionAdminClient.deleteSubscription(subscription);
      }

      for (TopicName topic : createdTopics) {
        LOG.info("Deleting topic '{}'", topic);
        topicAdminClient.deleteTopic(topic);
      }
    } finally {
      subscriptionAdminClient.close();
      topicAdminClient.close();
    }

    LOG.info("Manager successfully cleaned up.");
  }

  SubscriptionName getSubscriptionName(String subscriptionName) {
    return SubscriptionName.of(projectId, testId + RESOURCE_NAME_SEPARATOR + subscriptionName);
  }

  TopicName getTopicName(String topicName) {
    return TopicName.of(projectId, testId + RESOURCE_NAME_SEPARATOR + topicName);
  }

  /**
   * Check if the clients started by this instance are still usable, and throwing {@link
   * IllegalStateException} otherwise.
   */
  private void checkIsUsable() throws IllegalStateException {
    if (isNotUsable()) {
      throw new IllegalStateException("Manager has cleaned up resources and is unusable.");
    }
  }

  private boolean isNotUsable() {
    return topicAdminClient.isShutdown() || subscriptionAdminClient.isShutdown();
  }

  /** Builder for {@link DefaultPubsubResourceManager}. */
  public static final class Builder {

    private final String projectId;
    private final String testName;
    private CredentialsProvider credentialsProvider;

    private Builder(String testName, String projectId) {
      this.testName = testName;
      this.projectId = projectId;
    }

    public Builder credentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public DefaultPubsubResourceManager build() throws IOException {
      return new DefaultPubsubResourceManager(this);
    }
  }
}
