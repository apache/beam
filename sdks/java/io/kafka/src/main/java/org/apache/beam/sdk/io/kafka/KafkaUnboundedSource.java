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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link UnboundedSource} to read from Kafka, used by {@link Read} transform in KafkaIO. See
 * {@link KafkaIO} for user visible documentation and example usage.
 */
class KafkaUnboundedSource<K, V> extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {

  /**
   * The partitions are evenly distributed among the splits. The number of splits returned is {@code
   * min(desiredNumSplits, totalNumPartitions)}, though better not to depend on the exact count.
   *
   * <p>It is important to assign the partitions deterministically so that we can support resuming a
   * split from last checkpoint. The Kafka partitions are sorted by {@code <topic, partition>} and
   * then assigned to splits in round-robin order.
   */
  @Override
  public List<KafkaUnboundedSource<K, V>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {

    List<TopicPartition> partitions =
        new ArrayList<>(Preconditions.checkStateNotNull(spec.getTopicPartitions()));

    // (a) fetch partitions for each topic
    // (b) sort by <topic, partition>
    // (c) round-robin assign the partitions to splits

    String bootStrapServers =
        (String)
            Preconditions.checkArgumentNotNull(
                spec.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    if (partitions.isEmpty()) {
      try (Consumer<?, ?> consumer = spec.getConsumerFactoryFn().apply(spec.getConsumerConfig())) {
        List<String> topics = Preconditions.checkStateNotNull(spec.getTopics());
        if (topics.isEmpty()) {
          Pattern pattern = Preconditions.checkStateNotNull(spec.getTopicPattern());
          for (Map.Entry<String, List<PartitionInfo>> entry : consumer.listTopics().entrySet()) {
            if (pattern.matcher(entry.getKey()).matches()) {
              for (PartitionInfo p : entry.getValue()) {
                partitions.add(new TopicPartition(p.topic(), p.partition()));
                Lineage.getSources().add("kafka", ImmutableList.of(bootStrapServers, p.topic()));
              }
            }
          }
        } else {
          for (String topic : topics) {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            if (spec.getLogTopicVerification() == null || !spec.getLogTopicVerification()) {
              checkState(
                  partitionInfoList != null && !partitionInfoList.isEmpty(),
                  "Could not find any partitions info. Please check Kafka configuration and make sure "
                      + "that provided topics exist.");
            } else {
              LOG.warn(
                  "Could not find any partitions info. Please check Kafka configuration and make sure that the "
                      + "provided topics exist.");
            }
            for (PartitionInfo p : partitionInfoList) {
              partitions.add(new TopicPartition(p.topic(), p.partition()));
            }
            Lineage.getSources().add("kafka", ImmutableList.of(bootStrapServers, topic));
          }
        }
      }
    } else {
      final Map<String, List<Integer>> topicsAndPartitions = new HashMap<>();
      for (TopicPartition p : partitions) {
        topicsAndPartitions.computeIfAbsent(p.topic(), k -> new ArrayList<>()).add(p.partition());
      }

      try (Consumer<?, ?> consumer = spec.getConsumerFactoryFn().apply(spec.getConsumerConfig())) {
        for (Map.Entry<String, List<Integer>> e : topicsAndPartitions.entrySet()) {
          final String providedTopic = e.getKey();
          final List<Integer> providedPartitions = e.getValue();
          final Set<Integer> partitionsForTopic;
          try {
            partitionsForTopic =
                consumer.partitionsFor(providedTopic).stream()
                    .map(PartitionInfo::partition)
                    .collect(Collectors.toSet());
            if (spec.getLogTopicVerification() == null || !spec.getLogTopicVerification()) {
              for (Integer p : providedPartitions) {
                checkState(
                    partitionsForTopic.contains(p),
                    "Partition "
                        + p
                        + " does not exist for topic "
                        + providedTopic
                        + ". Please check Kafka configuration.");
              }
            } else {
              for (Integer p : providedPartitions) {
                if (!partitionsForTopic.contains(p)) {
                  LOG.warn(
                      "Partition {} does not exist for topic {}. Please check Kafka configuration.",
                      p,
                      providedTopic);
                }
              }
            }
          } catch (KafkaException exception) {
            LOG.warn("Unable to access cluster. Skipping fail fast checks.");
          }
          Lineage.getSources().add("kafka", ImmutableList.of(bootStrapServers, providedTopic));
        }
      } catch (KafkaException exception) {
        LOG.warn(
            "WARN: Failed to connect to kafka for running pre-submit validation of kafka "
                + "topic and partition configuration. This may be due to local permissions or "
                + "connectivity to the kafka bootstrap server, or due to misconfiguration of "
                + "KafkaIO. This validation is not required, and this warning may be ignored "
                + "if the Beam job runs successfully.");
      }
    }

    partitions.sort(
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

    checkArgument(desiredNumSplits > 0);
    checkState(
        partitions.size() > 0,
        "Could not find any partitions. Please check Kafka configuration and topic names");

    int numSplits;
    if (offsetBasedDeduplicationSupported()) {
      // Enforce 1:1 split to partition ratio for offset deduplication.
      numSplits = partitions.size();
      LOG.info(
          "Offset-based deduplication is enabled for KafkaUnboundedSource. "
              + "Forcing the number of splits to equal the number of total partitions: {}.",
          numSplits);
    } else {
      numSplits = Math.min(desiredNumSplits, partitions.size());
      // Make all splits have the same # of partitions.
      while (partitions.size() % numSplits > 0) {
        ++numSplits;
      }
    }
    List<List<TopicPartition>> assignments = new ArrayList<>(numSplits);

    for (int i = 0; i < numSplits; i++) {
      assignments.add(new ArrayList<>());
    }
    for (int i = 0; i < partitions.size(); i++) {
      assignments.get(i % numSplits).add(partitions.get(i));
    }

    List<KafkaUnboundedSource<K, V>> result = new ArrayList<>(numSplits);

    for (int i = 0; i < numSplits; i++) {
      List<TopicPartition> assignedToSplit = assignments.get(i);

      LOG.info(
          "Partitions assigned to split {} (total {}): {}",
          i,
          assignedToSplit.size(),
          Joiner.on(",").join(assignedToSplit));

      result.add(
          new KafkaUnboundedSource<>(
              spec.toBuilder()
                  .setTopics(Collections.emptyList())
                  .setTopicPartitions(assignedToSplit)
                  .build(),
              i));
    }

    return result;
  }

  @Override
  public KafkaUnboundedReader<K, V> createReader(
      PipelineOptions options, @Nullable KafkaCheckpointMark checkpointMark) {
    Preconditions.checkStateNotNull(spec.getTopicPartitions());
    if (spec.getTopicPartitions().isEmpty()) {
      LOG.warn("Looks like generateSplits() is not called. Generate single split.");
      try {
        return new KafkaUnboundedReader<>(split(1, options).get(0), checkpointMark);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new KafkaUnboundedReader<>(this, checkpointMark);
  }

  @Override
  public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
    return AvroCoder.of(KafkaCheckpointMark.class);
  }

  @Override
  public boolean requiresDeduping() {
    // Kafka records are ordered with in partitions. In addition checkpoint guarantees
    // records are not consumed twice.
    return false;
  }

  @Override
  public boolean offsetBasedDeduplicationSupported() {
    return spec.getOffsetDeduplication() != null && spec.getOffsetDeduplication();
  }

  @Override
  public Coder<KafkaRecord<K, V>> getOutputCoder() {
    Coder<K> keyCoder = Preconditions.checkStateNotNull(spec.getKeyCoder());
    Coder<V> valueCoder = Preconditions.checkStateNotNull(spec.getValueCoder());
    return KafkaRecordCoder.of(keyCoder, valueCoder);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUnboundedSource.class);

  private final Read<K, V> spec; // Contains all the relevant configuration of the source.
  private final int id; // split id, mainly for debugging

  public KafkaUnboundedSource(Read<K, V> spec, int id) {
    this.spec = spec;
    this.id = id;
  }

  Read<K, V> getSpec() {
    return spec;
  }

  int getId() {
    return id;
  }
}
