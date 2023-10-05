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
package org.apache.beam.it.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Kafka resources.
 *
 * <p>The class supports multiple topic names per server object. A topic is created if one has not
 * been created already.
 *
 * <p>The topic name is formed using testId. The topic name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class KafkaResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaResourceManager.class);

  private static final String DEFAULT_KAFKA_CONTAINER_NAME = "confluentinc/cp-kafka";

  // A list of available Kafka Docker image tags can be found at
  // https://hub.docker.com/r/confluentinc/cp-kafka/tags
  private static final String DEFAULT_KAFKA_CONTAINER_TAG = "7.3.1";

  // 9093 is the default port that kafka broker is configured to listen on.
  private static final int KAFKA_BROKER_PORT = 9093;

  private final AdminClient kafkaClient;
  private final Set<String> topicNames;
  private final String connectionString;
  private final boolean usingStaticTopic;

  private KafkaResourceManager(KafkaResourceManager.Builder builder) {
    this(null, new DefaultKafkaContainer(builder), builder);
  }

  @VisibleForTesting
  @SuppressWarnings("nullness")
  KafkaResourceManager(
      @Nullable AdminClient client,
      KafkaContainer container,
      KafkaResourceManager.Builder builder) {
    super(container, builder);

    this.usingStaticTopic = builder.topicNames.size() > 0;
    if (this.usingStaticTopic) {
      this.topicNames = new HashSet<>(builder.topicNames);
    } else {
      Set<String> setOfTopicNames = new HashSet<>();
      for (int index = 0; index < builder.numTopics; ++index) {
        setOfTopicNames.add(
            KafkaResourceManagerUtils.generateTopicName(
                String.format("%s-%d", builder.testId, index)));
      }
      this.topicNames = new HashSet<>(setOfTopicNames);
    }

    this.connectionString =
        String.format("PLAINTEXT://%s:%d", this.getHost(), this.getPort(KAFKA_BROKER_PORT));

    this.kafkaClient =
        client != null
            ? client
            : AdminClient.create(ImmutableMap.of("bootstrap.servers", this.connectionString));
  }

  public static KafkaResourceManager.Builder builder(String testId) {
    return new KafkaResourceManager.Builder(testId);
  }

  /** Returns the kafka bootstrap server connection string. */
  public synchronized String getBootstrapServers() {
    return connectionString;
  }

  /**
   * Returns a list of names of the topics that this kafka manager will operate in.
   *
   * @return names of the kafka topics.
   */
  public synchronized Set<String> getTopicNames() {
    return topicNames;
  }

  /**
   * Creates a kafka topic.
   *
   * <p>Note: Implementations may do topic creation here, if one does not already exist.
   *
   * @param topicName Topic name to associate with the given kafka instance.
   * @param partitions Number of partitions on the topic.
   * @return The name of the topic that was created.
   * @throws KafkaResourceManagerException if there is an error creating the kafka topic.
   */
  public synchronized String createTopic(String topicName, int partitions)
      throws KafkaResourceManagerException {
    checkArgument(partitions > 0, "partitions must be positive.");

    String uniqueName = KafkaResourceManagerUtils.generateTopicName(topicName);
    try {
      Set<String> currentTopics = kafkaClient.listTopics().names().get();
      if (!currentTopics.contains(uniqueName)) {
        kafkaClient
            .createTopics(
                Collections.singletonList(new NewTopic(uniqueName, partitions, (short) 1)))
            .all()
            .get();
        topicNames.add(uniqueName);
      }
    } catch (Exception e) {
      throw new KafkaResourceManagerException("Error creating topics.", e);
    }

    LOG.info("Successfully created topic {}.", uniqueName);

    return uniqueName;
  }

  /** Build a {@link KafkaProducer} for the given serializer and deserializers. */
  public synchronized <K, V> KafkaProducer<K, V> buildProducer(
      Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return new KafkaProducer<>(
        ImmutableMap.of("bootstrap.servers", getBootstrapServers()),
        keySerializer,
        valueSerializer);
  }

  /** Build a {@link KafkaConsumer} for the given serializer and deserializers. */
  public synchronized <K, V> KafkaConsumer<K, V> buildConsumer(
      Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {

    return new KafkaConsumer<>(
        ImmutableMap.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            getBootstrapServers().replace("PLAINTEXT://", ""),
            ConsumerConfig.GROUP_ID_CONFIG,
            "cg-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"),
        keyDeserializer,
        valueDeserializer);
  }

  /**
   * Deletes all created resources and cleans up the Kafka client, making the manager object
   * unusable.
   *
   * @throws KafkaResourceManagerException if there is an error deleting the Kafka resources.
   */
  @Override
  public synchronized void cleanupAll() throws KafkaResourceManagerException {
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
    if (producedError) {
      throw new KafkaResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    super.cleanupAll();

    LOG.info("Kafka manager successfully cleaned up.");
  }

  /** Builder for {@link KafkaResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<KafkaResourceManager> {

    private final Set<String> topicNames;
    int numTopics;

    private Builder(String testId) {
      super(testId, DEFAULT_KAFKA_CONTAINER_NAME, DEFAULT_KAFKA_CONTAINER_TAG);
      this.topicNames = new HashSet<>();
      this.numTopics = 0;
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
      checkArgument(numTopics >= 0, "numTopics must be non-negative.");
      checkArgument(
          this.topicNames.size() == 0,
          "setTopicNames and setNumTopics cannot be set at the same time.");
      this.numTopics = numTopics;
      return this;
    }

    @Override
    public KafkaResourceManager build() {
      return new KafkaResourceManager(this);
    }
  }

  static class DefaultKafkaContainer extends KafkaContainer {

    private final @Nullable String host;

    public DefaultKafkaContainer(Builder builder) {
      super(DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag));
      this.host = builder.host;
    }

    @Override
    public String getHost() {
      if (this.host == null) {
        return super.getHost();
      }
      return this.host;
    }

    @Override
    @SuppressWarnings("nullness")
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
