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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
  public List<KafkaUnboundedSource<K, V>> split(int desiredNumSplits, PipelineOptions options) {
    int numSplits = spec.getNumSplits() > 0 ? spec.getNumSplits() : desiredNumSplits;
    return IntStream.range(0, numSplits)
        .mapToObj(i -> new KafkaUnboundedSource<>(spec.withNumSplits(numSplits), i))
        .collect(Collectors.toList());
  }

  /**
   * Creates a new source spec with assigned partitions and updated consumer config before starting
   * the reader at runtime. It fetches partitions from the Kafka if partitions are not explicitly
   * set by the user.
   */
  private KafkaIO.Read<K, V> updatedSpecWithAssignedPartitions() {

    // Set bootstrap servers config.
    KafkaIO.Read<K, V> updatedSpec =
        spec.updateConsumerProperties(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.getBootstrapServers().get()));

    // (a) fetch partitions for each topic
    // (b) sort by <topic, partition>
    // (c) round-robin assign the partitions to splits
    List<TopicPartition> partitions = new ArrayList<>(spec.getTopicPartitions());

    if (partitions.isEmpty()) {
      try (Consumer<?, ?> consumer =
          spec.getConsumerFactoryFn().apply(updatedSpec.getConsumerConfig())) {
        for (String topic : spec.getTopics().get()) {
          for (PartitionInfo p : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(p.topic(), p.partition()));
          }
        }
      }
    }

    int numSplits = spec.getNumSplits();

    checkState(
        partitions.size() > 0,
        "Could not find any partitions. Please check Kafka configuration and topic names");
    checkState(
        numSplits <= partitions.size(),
        "Number of splits %s is larger than number of partitions %s.  "
            + "Empty splits are not supported yet. Please set number of partitions explicitly "
            + "using 'withNumSplits() option",
        numSplits,
        partitions.size());

    partitions.sort(
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

    List<TopicPartition> assignedPartitions =
        partitions
            .stream()
            .filter(p -> p.partition() % numSplits == id) // round robin assignment
            .collect(Collectors.toList());

    LOG.info(
        "Partitions assigned to split {} (total {}): {}",
        id,
        assignedPartitions.size(),
        Joiner.on(",").join(assignedPartitions));

    return updatedSpec.toBuilder().setTopics(null).setTopicPartitions(assignedPartitions).build();
  }

  @Override
  public KafkaUnboundedReader<K, V> createReader(
      PipelineOptions options, KafkaCheckpointMark checkpointMark) {
    return new KafkaUnboundedReader<>(
        new KafkaUnboundedSource<>(updatedSpecWithAssignedPartitions(), id), checkpointMark);
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
  public Coder<KafkaRecord<K, V>> getOutputCoder() {
    return KafkaRecordCoder.of(spec.getKeyCoder(), spec.getValueCoder());
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUnboundedSource.class);

  private final Read<K, V> spec; // Contains all the relevant configuration of the source.
  private final int id; // split id

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
