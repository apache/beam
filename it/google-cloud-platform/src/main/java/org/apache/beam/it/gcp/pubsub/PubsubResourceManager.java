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
package org.apache.beam.it.gcp.pubsub;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SchemaServiceSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.UpdateTopicRequest;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Pub/Sub resources.
 *
 * <p>The class provides an interaction with the real Pub/Sub client, with operations related to
 * management of topics and subscriptions.
 */
public final class PubsubResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubResourceManager.class);

  private static final int DEFAULT_ACK_DEADLINE_SECONDS = 600;
  private static final String RESOURCE_NAME_SEPARATOR = "-";

  // Retry settings for client operations
  private static final int FAILSAFE_MAX_RETRIES = 5;
  private static final Duration FAILSAFE_RETRY_DELAY = Duration.ofSeconds(10);
  private static final Duration FAILSAFE_RETRY_MAX_DELAY = Duration.ofSeconds(60);
  private static final double FAILSAFE_RETRY_JITTER = 0.1;

  private final String testId;
  private final String projectId;
  private final PubsubPublisherFactory publisherFactory;
  private final TopicAdminClient topicAdminClient;
  private final SubscriptionAdminClient subscriptionAdminClient;

  private final SchemaServiceClient schemaServiceClient;

  private final Set<TopicName> createdTopics;
  private final Set<SubscriptionName> createdSubscriptions;

  private final Set<SchemaName> createdSchemas;

  private PubsubResourceManager(Builder builder) throws IOException {
    this(
        builder.testName,
        builder.projectId,
        new PubsubPublisherFactory(builder.credentialsProvider),
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()),
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()),
        SchemaServiceClient.create(
            SchemaServiceSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()));
  }

  @VisibleForTesting
  PubsubResourceManager(
      String testName,
      String projectId,
      PubsubPublisherFactory publisherFactory,
      TopicAdminClient topicAdminClient,
      SubscriptionAdminClient subscriptionAdminClient,
      SchemaServiceClient schemaServiceClient) {
    this.projectId = projectId;
    this.testId = PubsubUtils.createTestId(testName);
    this.publisherFactory = publisherFactory;
    this.topicAdminClient = topicAdminClient;
    this.subscriptionAdminClient = subscriptionAdminClient;
    this.createdTopics = Collections.synchronizedSet(new HashSet<>());
    this.createdSubscriptions = Collections.synchronizedSet(new HashSet<>());
    this.createdSchemas = Collections.synchronizedSet(new HashSet<>());
    this.schemaServiceClient = schemaServiceClient;
  }

  public static Builder builder(
      String testName, String projectId, CredentialsProvider credentialsProvider) {
    checkArgument(!Strings.isNullOrEmpty(testName), "testName can not be null or empty");
    checkArgument(!projectId.isEmpty(), "projectId can not be empty");
    return new Builder(testName, projectId, credentialsProvider);
  }

  /**
   * Return the test ID this Resource Manager uses to manage Pub/Sub instances.
   *
   * @return the test ID.
   */
  public String getTestId() {
    return testId;
  }

  /**
   * Creates a topic with the given name on Pub/Sub.
   *
   * @param topicName Topic name to create. The underlying implementation may not use the topic name
   *     directly, and can add a prefix or a suffix to identify specific executions.
   * @return The instance of the TopicName that was just created.
   */
  public TopicName createTopic(String topicName) {
    checkArgument(!topicName.isEmpty(), "topicName can not be empty");
    checkIsUsable();

    TopicName name = getTopicName(topicName);
    return createTopicInternal(name);
  }

  /**
   * Creates a topic with the given name on Pub/Sub.
   *
   * @param topicName Topic name to create. The underlying implementation will use the topic name
   *     directly.
   * @return The instance of the TopicName that was just created.
   */
  public TopicName createTopicWithoutPrefix(String topicName) {
    checkArgument(!topicName.isEmpty(), "topicName can not be empty");
    checkIsUsable();

    TopicName name = TopicName.of(projectId, topicName);
    return createTopicInternal(name);
  }

  /**
   * Creates a subscription at the specific topic, with a given name.
   *
   * @param topicName Topic Name reference to add the subscription.
   * @param subscriptionName Name of the subscription to use. Note that the underlying
   *     implementation may not use the subscription name literally, and can use a prefix or a
   *     suffix to identify specific executions.
   * @return The instance of the SubscriptionName that was just created.
   */
  public SubscriptionName createSubscription(TopicName topicName, String subscriptionName) {
    checkArgument(!subscriptionName.isEmpty(), "subscriptionName can not be empty");
    checkIsUsable();

    if (!createdTopics.contains(topicName)) {
      throw new IllegalArgumentException(
          "Can not create a subscription for a topic not managed by this instance.");
    }

    LOG.info("Creating subscription '{}' for topic '{}'", subscriptionName, topicName);

    Subscription subscription =
        Failsafe.with(retryOnDeadlineExceeded())
            .get(
                () ->
                    subscriptionAdminClient.createSubscription(
                        getSubscriptionName(subscriptionName),
                        topicName,
                        PushConfig.getDefaultInstance(),
                        DEFAULT_ACK_DEADLINE_SECONDS));
    SubscriptionName reference = PubsubUtils.toSubscriptionName(subscription);
    createdSubscriptions.add(getSubscriptionName(subscriptionName));

    LOG.info(
        "Subscription '{}' for topic '{}' was created successfully!",
        subscription.getName(),
        topicName);

    return reference;
  }

  /**
   * Publishes a message with the given data to the publisher context's topic.
   *
   * @param topic Reference to the topic to send the message.
   * @param attributes Attributes to send with the message.
   * @param data Byte data to send.
   * @return The message id that was generated.
   */
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

  /**
   * Pulls messages from the given subscription.
   *
   * @param subscriptionName Name of the subscription to use.
   * @return The message id that was generated.
   */
  public PullResponse pull(SubscriptionName subscriptionName, int maxMessages) {
    LOG.info("Pulling messages from subscription '{}'", subscriptionName);
    PullResponse response = subscriptionAdminClient.pull(subscriptionName, maxMessages);
    LOG.info(
        "Received {} messages from subscription '{}'",
        response.getReceivedMessagesCount(),
        subscriptionName);
    return response;
  }

  /**
   * Registers a new schema of the given type and definition, then assigns it to the specified
   * topic.
   *
   * @param schemaType the type of schema to create (e.g. AVRO, PROTOBUF)
   * @param schemaDefinition the definition of the schema to create in AVRO or Protobuf syntax.
   * @param dataEncoding the encoding of the data in pubsub (e.g. BINARY_ENCODING, JSON)
   * @param schemaTopic the name of the topic to which assign the schema.
   * @return the name of the newly created schema
   */
  public String createSchema(
      Schema.Type schemaType,
      String schemaDefinition,
      Encoding dataEncoding,
      TopicName schemaTopic) {
    Schema schema =
        schemaServiceClient.createSchema(
            ProjectName.newBuilder().setProject(projectId).build(),
            Schema.newBuilder().setType(schemaType).setDefinition(schemaDefinition).build(),
            "schema-" + testId + "-" + schemaTopic.getTopic());
    createdSchemas.add(SchemaName.parse(schema.getName()));
    topicAdminClient.updateTopic(
        UpdateTopicRequest.newBuilder()
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_settings"))
            .setTopic(
                Topic.newBuilder()
                    .setName(schemaTopic.toString())
                    .setSchemaSettings(
                        SchemaSettings.newBuilder()
                            .setSchema(schema.getName())
                            .setEncoding(dataEncoding)
                            .build())
                    .build())
            .build());
    return schema.getName();
  }

  /** Delete any topics or subscriptions created by this manager. */
  @Override
  public synchronized void cleanupAll() {
    // Ignore call if it was cleaned up before
    if (isNotUsable()) {
      return;
    }

    LOG.info("Attempting to cleanup Pub/Sub resource manager.");

    try {
      for (SubscriptionName subscription : createdSubscriptions) {
        LOG.info("Deleting subscription '{}'", subscription);
        Failsafe.with(retryOnDeadlineExceeded())
            .run(() -> subscriptionAdminClient.deleteSubscription(subscription));
      }

      for (TopicName topic : createdTopics) {
        LOG.info("Deleting topic '{}'", topic);
        Failsafe.with(retryOnDeadlineExceeded())
            .run(
                () -> {

                  // Delete subscriptions that would be orphaned.
                  for (String topicSub :
                      topicAdminClient.listTopicSubscriptions(topic).iterateAll()) {
                    LOG.info("Deleting subscription '{}'", topicSub);
                    subscriptionAdminClient.deleteSubscription(topicSub);
                  }

                  topicAdminClient.deleteTopic(topic);
                });
      }

      for (SchemaName schemaName : createdSchemas) {
        LOG.info("Deleting schema '{}'", schemaName);
        Failsafe.with(retryOnDeadlineExceeded())
            .run(() -> schemaServiceClient.deleteSchema(schemaName));
      }
    } finally {
      subscriptionAdminClient.close();
      topicAdminClient.close();
      schemaServiceClient.close();
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

  /** Internal method to create a Pub/Sub topic used by this resource manager. */
  private TopicName createTopicInternal(TopicName topicName) {
    LOG.info("Creating topic '{}'...", topicName.toString());

    Topic topic =
        Failsafe.with(retryOnDeadlineExceeded()).get(() -> topicAdminClient.createTopic(topicName));
    TopicName reference = PubsubUtils.toTopicName(topic);
    createdTopics.add(reference);

    LOG.info("Topic '{}' was created successfully!", reference);

    return reference;
  }

  private boolean isNotUsable() {
    return topicAdminClient.isShutdown() || subscriptionAdminClient.isShutdown();
  }

  private static <T> RetryPolicy<T> retryOnDeadlineExceeded() {
    return RetryPolicy.<T>builder()
        .handleIf(
            exception -> ExceptionUtils.containsType(exception, DeadlineExceededException.class))
        .withMaxRetries(FAILSAFE_MAX_RETRIES)
        .withBackoff(FAILSAFE_RETRY_DELAY, FAILSAFE_RETRY_MAX_DELAY)
        .withJitter(FAILSAFE_RETRY_JITTER)
        .build();
  }

  /** Builder for {@link PubsubResourceManager}. */
  public static final class Builder {

    private final String projectId;
    private final String testName;
    private CredentialsProvider credentialsProvider;

    private Builder(String testName, String projectId, CredentialsProvider credentialsProvider) {
      this.testName = testName;
      this.projectId = projectId;
      this.credentialsProvider = credentialsProvider;
    }

    public Builder credentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public PubsubResourceManager build() throws IOException {
      if (credentialsProvider == null) {
        throw new IllegalArgumentException(
            "Unable to find credentials. Please provide credentials to authenticate to GCP");
      }
      return new PubsubResourceManager(this);
    }
  }
}
