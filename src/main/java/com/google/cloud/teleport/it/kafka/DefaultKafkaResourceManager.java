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
package com.google.cloud.teleport.it.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.it.testcontainers.TestContainerResourceManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for implementation of {@link KafkaResourceManager} interface.
 *
 * <p>The class supports multiple topic names per server object. A topic is created if one has not
 * been created already.
 *
 * <p>The topic name is formed using testId. The topic name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class DefaultKafkaResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements KafkaResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaResourceManager.class);

  private static final String DEFAULT_KAFKA_CONTAINER_NAME = "confluentinc/cp-kafka";

  // A list of available Kafka Docker image tags can be found at
  // https://hub.docker.com/r/confluentinc/cp-kafka/tags
  private static final String DEFAULT_KAFKA_CONTAINER_TAG = "7.3.1";

  // 9092 is the default port that kafka is configured to listen on.
  private static final int KAFKA_INTERNAL_PORT = 9092;

  private final AdminClient kafkaClient;
  private final Set<String> topicNames;
  private final String connectionString;
  private final boolean usingStaticTopic;

  private DefaultKafkaResourceManager(DefaultKafkaResourceManager.Builder builder) {
    this(
        null,
        new KafkaContainer(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DefaultKafkaResourceManager(
      AdminClient client, KafkaContainer container, DefaultKafkaResourceManager.Builder builder) {
    super(container, builder);

    this.usingStaticTopic = builder.topicNames.size() > 0;
    if (this.usingStaticTopic) {
      this.topicNames = ImmutableSet.copyOf(builder.topicNames);
    } else {
      Set<String> setOfTopicNames = new HashSet<String>();
      for (int index = 0; index < builder.numTopics; ++index) {
        setOfTopicNames.add(
            KafkaResourceManagerUtils.generateTopicName(
                String.format("%s-%d", builder.testId, index)));
      }
      this.topicNames = ImmutableSet.copyOf(setOfTopicNames);
    }

    this.connectionString =
        String.format("PLAINTEXT://%s:%d", this.getHost(), this.getPort(KAFKA_INTERNAL_PORT));

    this.kafkaClient =
        (client == null)
            ? AdminClient.create(ImmutableMap.of("bootstrap.servers", this.connectionString))
            : client;
  }

  public static DefaultKafkaResourceManager.Builder builder(String testId) throws IOException {
    return new DefaultKafkaResourceManager.Builder(testId);
  }

  @Override
  public synchronized String getBootstrapServers() {
    return connectionString;
  }

  @Override
  public synchronized Set<String> getTopicNames() {
    return topicNames;
  }

  @Override
  public synchronized boolean createTopic(String topicName) {
    try {
      Set<String> currentTopics = kafkaClient.listTopics().names().get();
      if (currentTopics.contains(topicName)) {
        return false;
      } else {
        kafkaClient
            .createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)))
            .all()
            .get();
      }
    } catch (Exception e) {
      throw new KafkaResourceManagerException("Error creating topics.", e);
    }

    LOG.info("Successfully created topic {}.{}", topicName);

    return true;
  }

  @Override
  public synchronized boolean cleanupAll() {
    LOG.info("Attempting to cleanup Kafka manager.");

    boolean producedError = false;

    // First, delete kafka topics if it was not given as a static argument
    try {
      if (!usingStaticTopic) {
        kafkaClient.deleteTopics(topicNames).all().get();
      }
    } catch (Exception e) {
      LOG.error("Failed to delete kafka topic.", e);
      producedError = true;
    }

    // Throw Exception at the end if there were any errors
    if (producedError || !super.cleanupAll()) {
      throw new KafkaResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    LOG.info("Kafka manager successfully cleaned up.");

    return true;
  }

  /** Builder for {@link DefaultKafkaResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<DefaultKafkaResourceManager> {

    private Set<String> topicNames;
    int numTopics;

    private Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_KAFKA_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_KAFKA_CONTAINER_TAG;
      this.topicNames = new HashSet<String>();
      this.numTopics = 1;
    }

    /**
     * Sets names of topics to be used to that of a static kafka instance. Use this method only when
     * attempting to operate on pre-existing kafka topics.
     *
     * <p>Note: if a topic name is set, and a static kafka server is being used
     * (useStaticContainer() is also called on the builder), then a topic will be created on the
     * static server if it does not exist, and it will NOT be removed when cleanupAll() is called on
     * the KafkaResourceManager.
     *
     * @param topicNames A set of topic names.
     * @return this builder object with the topic name set.
     */
    public Builder setTopicNames(Set<String> topicNames) {
      this.topicNames.clear();
      this.topicNames.addAll(topicNames);
      return this;
    }

    /**
     * Sets the number of topics to be used instead of explicitly assign the topic names.
     *
     * <p>Note: if number of topics is set, and a static kafka server is being used
     * (useStaticContainer() is also called on the builder), then the assigned number of topics will
     * be created on the static server, and it will be removed when cleanupAll() is called on the
     * KafkaResourceManager.
     *
     * @param numTopics number of topics to be created. Default is 1.
     * @return this builder object with numTopics set.
     */
    public Builder setNumTopics(int numTopics) {
      checkArgument(numTopics > 0, "numTopics must be positive.");
      checkArgument(
          this.topicNames.size() == 0,
          "setTopicNames and setNumTopics cannot be set at the same time.");
      this.numTopics = numTopics;
      return this;
    }

    @Override
    public DefaultKafkaResourceManager build() {
      return new DefaultKafkaResourceManager(this);
    }
  }
}
