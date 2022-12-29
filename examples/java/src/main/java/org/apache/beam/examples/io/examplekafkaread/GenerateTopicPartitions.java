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
package org.apache.beam.examples.io.examplekafkaread;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * GenerateTopicPartitions is a straightforward DoFn designed to take the list of topics provided in
 * its configuration, and yield all the current {@link TopicPartition}s for those topics
 *
 * <p>It consumes a byte[], but this is from an Impulse, and is just used to start the processing.
 *
 * <p>Because this process happens once and is bounded by the number of current TopicPartitions,
 * there is no need to leverage advanced features for performance, stability, or correctness.
 */
public class GenerateTopicPartitions extends DoFn<byte[], TopicPartition> {
  final Map<String, Object> consumerConfig;
  final List<String> topics;

  GenerateTopicPartitions(
      @NonNull Map<String, Object> consumerConfig, @NonNull List<String> topics) {
    this.consumerConfig = consumerConfig;
    this.topics = topics;
  }

  @ProcessElement
  public void processElement(OutputReceiver<TopicPartition> receiver) {
    try (Consumer<?, ?> consumer = new KafkaConsumer<Object, Object>(consumerConfig)) {
      for (String topic : Preconditions.checkStateNotNull(topics)) {
        for (PartitionInfo p : consumer.partitionsFor(topic)) {
          receiver.output(new TopicPartition(p.topic(), p.partition()));
        }
      }
    }
  }
}
