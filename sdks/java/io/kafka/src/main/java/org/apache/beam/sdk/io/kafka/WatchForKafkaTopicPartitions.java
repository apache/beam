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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WatchForKafkaTopicPartitions
    extends PTransform<PBegin, PCollection<KafkaSourceDescriptor>> {

  private static final Duration DEFAULT_CHECK_DURATION = Duration.standardHours(1);
  private static final String COUNTER_NAMESPACE = "watch_kafka_topic_partition";

  private final Duration checkDuration;
  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      kafkaConsumerFactoryFn;
  private final Map<String, Object> kafkaConsumerConfig;
  private final SerializableFunction<TopicPartition, Boolean> checkStopReadingFn;
  private final List<String> topics;
  private final Instant startReadTime;
  private final Instant stopReadTime;

  public WatchForKafkaTopicPartitions(
      @Nullable Duration checkDuration,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
      Map<String, Object> kafkaConsumerConfig,
      SerializableFunction<TopicPartition, Boolean> checkStopReadingFn,
      List<String> topics,
      Instant startReadTime,
      Instant stopReadTime) {
    this.checkDuration = checkDuration == null ? DEFAULT_CHECK_DURATION : checkDuration;
    this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.checkStopReadingFn = checkStopReadingFn;
    this.topics = topics;
    this.startReadTime = startReadTime;
    this.stopReadTime = stopReadTime;
  }

  @Override
  public PCollection<KafkaSourceDescriptor> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            "Match new TopicPartitions",
            Watch.growthOf(new WatchPartitionFn()).withPollInterval(checkDuration))
        .apply(ParDo.of(new ConvertToDescriptor()));
  }

  private class ConvertToDescriptor
      extends DoFn<KV<byte[], TopicPartition>, KafkaSourceDescriptor> {
    @ProcessElement
    public void processElement(
        @Element KV<byte[], TopicPartition> partition,
        OutputReceiver<KafkaSourceDescriptor> receiver) {
      TopicPartition topicPartition = Objects.requireNonNull(partition.getValue());
      if (checkStopReadingFn == null || !checkStopReadingFn.apply(topicPartition)) {
        Counter foundedTopicPartition =
            Metrics.counter(COUNTER_NAMESPACE, topicPartition.toString());
        foundedTopicPartition.inc();
        receiver.output(
            KafkaSourceDescriptor.of(
                topicPartition, null, startReadTime, null, stopReadTime, null));
      }
    }
  }

  private class WatchPartitionFn extends PollFn<byte[], TopicPartition> {
    @Override
    public Watch.Growth.PollResult<TopicPartition> apply(byte[] element, Context c)
        throws Exception {
      Instant now = Instant.now();
      return Watch.Growth.PollResult.incomplete(now, getAllTopicPartitions()).withWatermark(now);
    }
  }

  @VisibleForTesting
  List<TopicPartition> getAllTopicPartitions() {
    List<TopicPartition> current = new ArrayList<>();
    try (Consumer<byte[], byte[]> kafkaConsumer =
        kafkaConsumerFactoryFn.apply(kafkaConsumerConfig)) {
      if (topics != null && !topics.isEmpty()) {
        for (String topic : topics) {
          for (PartitionInfo partition : kafkaConsumer.partitionsFor(topic)) {
            current.add(new TopicPartition(topic, partition.partition()));
          }
        }

      } else {
        for (Map.Entry<String, List<PartitionInfo>> topicInfo :
            kafkaConsumer.listTopics().entrySet()) {
          for (PartitionInfo partition : topicInfo.getValue()) {
            current.add(new TopicPartition(topicInfo.getKey(), partition.partition()));
          }
        }
      }
    }
    return current;
  }
}
