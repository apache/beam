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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Extracts Kafka topics from {@code KafkaIO.Read} and {@code KafkaIO.WriteRecords}, mirroring the
 * Spark integration's {@code KafkaRelationVisitor}. Naming follows the OpenLineage spec: namespace
 * {@code kafka://{host}:{port}} of the first bootstrap server, name {@code {topic}}. Topic patterns
 * are only resolved by the reader at runtime and are therefore not reported here.
 */
class KafkaLineageVisitor extends PipelineLineageVisitor {

  @Override
  boolean isDefinedAt(PTransform<?, ?> transform) {
    return transform instanceof KafkaIO.Read || transform instanceof KafkaIO.WriteRecords;
  }

  @Override
  List<DatasetIdentifier> applyInputs(PTransform<?, ?> transform) {
    if (!(transform instanceof KafkaIO.Read)) {
      return Collections.emptyList();
    }
    KafkaIO.Read<?, ?> read = (KafkaIO.Read<?, ?>) transform;
    Set<String> topics = new LinkedHashSet<>();
    List<String> names = read.getTopics();
    if (names != null) {
      topics.addAll(names);
    }
    List<TopicPartition> partitions = read.getTopicPartitions();
    if (partitions != null) {
      for (TopicPartition partition : partitions) {
        topics.add(partition.topic());
      }
    }
    return datasets(read.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), topics);
  }

  @Override
  List<DatasetIdentifier> applyOutputs(PTransform<?, ?> transform) {
    if (!(transform instanceof KafkaIO.WriteRecords)) {
      return Collections.emptyList();
    }
    KafkaIO.WriteRecords<?, ?> write = (KafkaIO.WriteRecords<?, ?>) transform;
    String topic = write.getTopic();
    return datasets(
        write.getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
        topic == null ? Collections.emptySet() : Collections.singleton(topic));
  }

  private static List<DatasetIdentifier> datasets(
      @Nullable Object bootstrapServers, Collection<String> topics) {
    String server = firstServer(bootstrapServers);
    if (server == null || topics.isEmpty()) {
      return Collections.emptyList();
    }
    List<DatasetIdentifier> result = new ArrayList<>();
    for (String topic : topics) {
      result.add(new DatasetIdentifier(topic, "kafka://" + server));
    }
    return result;
  }

  /** The spec namespace is a single host:port; Kafka accepts a list or a comma-separated string. */
  private static @Nullable String firstServer(@Nullable Object bootstrapServers) {
    Object value = bootstrapServers;
    if (value instanceof List) {
      value = ((List<?>) value).isEmpty() ? null : ((List<?>) value).get(0);
    }
    if (value == null) {
      return null;
    }
    String first = value.toString().split(",", -1)[0].trim();
    return first.isEmpty() ? null : first;
  }
}
