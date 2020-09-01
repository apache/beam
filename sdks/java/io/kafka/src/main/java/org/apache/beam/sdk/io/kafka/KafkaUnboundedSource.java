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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.Consumer;
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
  public List<KafkaUnboundedSource<K, V>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {

    List<TopicPartition> partitions = new ArrayList<>(spec.getTopicPartitions());

    // (a) fetch partitions for each topic
    // (b) sort by <topic, partition>
    // (c) round-robin assign the partitions to splits

    if (partitions.isEmpty()) {
      try (Consumer<?, ?> consumer = spec.getConsumerFactoryFn().apply(spec.getConsumerConfig())) {
        for (String topic : spec.getTopics()) {
          for (PartitionInfo p : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(p.topic(), p.partition()));
          }
        }
      }
    }

    partitions.sort(
        Comparator.comparing(TopicPartition::topic)
            .thenComparing(Comparator.comparingInt(TopicPartition::partition)));

    checkArgument(desiredNumSplits > 0);
    checkState(
        partitions.size() > 0,
        "Could not find any partitions. Please check Kafka configuration and topic names");

    int numSplits = Math.min(desiredNumSplits, partitions.size());
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
      PipelineOptions options, KafkaCheckpointMark checkpointMark) {
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
  public Coder<KafkaRecord<K, V>> getOutputCoder() {
    return KafkaRecordCoder.of(spec.getKeyCoder(), spec.getValueCoder());
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUnboundedSource.class);

  private final Read<K, V> spec; // Contains all the relevant configuratiton of the source.
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
