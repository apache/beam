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
package org.apache.beam.runners.kafka.streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the topics a translated pipeline needs before the Kafka Streams application starts.
 *
 * <p>The runner shuffles data through topics it names itself: a bootstrap topic per Impulse and per
 * primitive Read, and a repartition topic per GroupByKey. Kafka Streams does create the internal
 * topics it manages on its own, but these are declared with explicit names through {@code
 * addSource} and {@code addSink}, so to Kafka Streams they are ordinary user topics — it will not
 * create them, and refuses to start with {@code MissingSourceTopicException} if a source topic is
 * absent. Relying on the broker's {@code auto.create.topics.enable} is not an option either: it is
 * off on many clusters, and a topic auto-created on first fetch gets the broker's default partition
 * count rather than the pipeline's.
 *
 * <p>Only topics carrying one of the runner's own prefixes are created. Any other topic in the
 * topology belongs to the user (a source or sink they named), and creating those implicitly would
 * hide a misconfiguration behind an empty topic.
 */
class KafkaStreamsTopicManager {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsTopicManager.class);

  /**
   * Prefixes of the bootstrap topics, which must have exactly one partition.
   *
   * <p>An Impulse or a primitive Read emits its elements once per task, gated by a state store that
   * is itself per task. Kafka Streams creates one task per partition of the source topic, so a
   * bootstrap topic with several partitions would make the same Impulse fire once per partition and
   * the same source be read once per partition.
   */
  private static final List<String> SINGLE_PARTITION_TOPIC_PREFIXES =
      java.util.Arrays.asList("__beam_impulse_", "__beam_read_");

  /**
   * Prefixes of the topics whose partition count sets the pipeline's parallelism — the repartition
   * topic a GroupByKey shuffles through.
   */
  private static final List<String> PARTITIONED_TOPIC_PREFIXES =
      java.util.Arrays.asList("__beam_gbk_");

  private KafkaStreamsTopicManager() {}

  /**
   * Creates any runner-owned topic in {@code topology} that does not exist yet.
   *
   * <p>Safe to run concurrently with another instance of the same job: a topic that appears between
   * the existence check and the create request surfaces as {@link TopicExistsException}, which is
   * treated as success.
   */
  static void createMissingTopics(Topology topology, KafkaStreamsPipelineOptions options) {
    Set<String> runnerTopics = runnerOwnedTopics(topology);
    if (runnerTopics.isEmpty()) {
      return;
    }
    Properties adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
    try (Admin admin = Admin.create(adminConfig)) {
      Set<String> existing = admin.listTopics().names().get();
      List<NewTopic> toCreate = new ArrayList<>();
      for (String topic : runnerTopics) {
        if (!existing.contains(topic)) {
          toCreate.add(
              new NewTopic(
                  topic, partitionsFor(topic, options), options.getTopicReplicationFactor()));
        }
      }
      if (toCreate.isEmpty()) {
        return;
      }
      LOG.info("Creating {} runner-owned topic(s): {}", toCreate.size(), toCreate);
      createAll(admin, toCreate);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while creating the pipeline's Kafka topics", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create the pipeline's Kafka topics", e);
    }
  }

  private static void createAll(Admin admin, Collection<NewTopic> topics)
      throws InterruptedException, ExecutionException {
    try {
      admin.createTopics(topics).all().get();
    } catch (ExecutionException e) {
      // Another instance of the same application may have created them first, which is fine.
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw e;
      }
      LOG.debug("Some topics already existed; another instance created them first", e);
    }
  }

  /** The topics in the topology that the runner named, and so is responsible for creating. */
  private static Set<String> runnerOwnedTopics(Topology topology) {
    Set<String> topics = new HashSet<>();
    for (TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
      for (TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof TopologyDescription.Source) {
          Set<String> sourceTopics = ((TopologyDescription.Source) node).topicSet();
          if (sourceTopics != null) {
            topics.addAll(sourceTopics);
          }
        } else if (node instanceof TopologyDescription.Sink) {
          String topic = ((TopologyDescription.Sink) node).topic();
          if (topic != null) {
            topics.add(topic);
          }
        }
      }
    }
    topics.removeIf(topic -> !isRunnerOwned(topic));
    return topics;
  }

  /**
   * The partition count a runner-owned topic is created with: one for a bootstrap topic, and the
   * configured parallelism for a shuffle topic.
   */
  private static int partitionsFor(String topic, KafkaStreamsPipelineOptions options) {
    return hasAnyPrefix(topic, SINGLE_PARTITION_TOPIC_PREFIXES)
        ? 1
        : options.getInternalParallelism();
  }

  private static boolean isRunnerOwned(String topic) {
    return hasAnyPrefix(topic, SINGLE_PARTITION_TOPIC_PREFIXES)
        || hasAnyPrefix(topic, PARTITIONED_TOPIC_PREFIXES);
  }

  private static boolean hasAnyPrefix(String topic, List<String> prefixes) {
    for (String prefix : prefixes) {
      if (topic.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
