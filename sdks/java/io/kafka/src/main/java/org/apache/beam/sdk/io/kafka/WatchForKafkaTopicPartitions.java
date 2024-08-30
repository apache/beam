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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} for continuously querying Kafka for new partitions, and emitting those
 * topics as {@link KafkaSourceDescriptor} This transform is implemented using the {@link Watch}
 * transform, and modifications to this transform should keep that in mind.
 *
 * <p>Please see
 * https://docs.google.com/document/d/1Io49s5LBs29HJyppKG3AlR-gHz5m5PC6CqO0CCoSqLs/edit?usp=sharing
 * for design details
 */
class WatchForKafkaTopicPartitions extends PTransform<PBegin, PCollection<KafkaSourceDescriptor>> {

  private static final Duration DEFAULT_CHECK_DURATION = Duration.standardHours(1);
  private static final String COUNTER_NAMESPACE = "watch_kafka_topic_partition";

  private final Duration checkDuration;
  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      kafkaConsumerFactoryFn;
  private final Map<String, Object> kafkaConsumerConfig;
  private final @Nullable CheckStopReadingFn checkStopReadingFn;
  private final Set<String> topics;
  private final @Nullable Pattern topicPattern;
  private final @Nullable Instant startReadTime;
  private final @Nullable Instant stopReadTime;

  public WatchForKafkaTopicPartitions(
      @Nullable Duration checkDuration,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
      Map<String, Object> kafkaConsumerConfig,
      @Nullable CheckStopReadingFn checkStopReadingFn,
      Set<String> topics,
      @Nullable Pattern topicPattern,
      @Nullable Instant startReadTime,
      @Nullable Instant stopReadTime) {
    this.checkDuration = firstNonNull(checkDuration, DEFAULT_CHECK_DURATION);
    this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.checkStopReadingFn = checkStopReadingFn;
    this.topics = topics;
    this.topicPattern = topicPattern;
    this.startReadTime = startReadTime;
    this.stopReadTime = stopReadTime;
  }

  @Override
  public PCollection<KafkaSourceDescriptor> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            "Match new TopicPartitions",
            Watch.growthOf(
                    new WatchPartitionFn(
                        kafkaConsumerFactoryFn, kafkaConsumerConfig, topics, topicPattern))
                .withPollInterval(checkDuration))
        .apply(ParDo.of(new ConvertToDescriptor(checkStopReadingFn, startReadTime, stopReadTime)));
  }

  private static class ConvertToDescriptor
      extends DoFn<KV<byte[], TopicPartition>, KafkaSourceDescriptor> {

    private final @Nullable CheckStopReadingFn checkStopReadingFn;
    private final @Nullable Instant startReadTime;
    private final @Nullable Instant stopReadTime;

    private ConvertToDescriptor(
        @Nullable CheckStopReadingFn checkStopReadingFn,
        @Nullable Instant startReadTime,
        @Nullable Instant stopReadTime) {
      this.checkStopReadingFn = checkStopReadingFn;
      this.startReadTime = startReadTime;
      this.stopReadTime = stopReadTime;
    }

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

    @Setup
    public void setup() throws Exception {
      if (checkStopReadingFn != null) {
        checkStopReadingFn.setup();
      }
    }

    @Teardown
    public void teardown() throws Exception {
      if (checkStopReadingFn != null) {
        checkStopReadingFn.teardown();
      }
    }
  }

  private static class WatchPartitionFn extends PollFn<byte[], TopicPartition> {

    private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        kafkaConsumerFactoryFn;
    private final Map<String, Object> kafkaConsumerConfig;
    private final Set<String> topics;
    private final @Nullable Pattern topicPattern;

    private WatchPartitionFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
        Map<String, Object> kafkaConsumerConfig,
        Set<String> topics,
        @Nullable Pattern topicPattern) {
      this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
      this.kafkaConsumerConfig = kafkaConsumerConfig;
      this.topics = topics;
      this.topicPattern = topicPattern;
    }

    @Override
    public Watch.Growth.PollResult<TopicPartition> apply(byte[] element, Context c)
        throws Exception {
      Instant now = Instant.now();
      return Watch.Growth.PollResult.incomplete(
              now,
              getAllTopicPartitions(
                  kafkaConsumerFactoryFn, kafkaConsumerConfig, topics, topicPattern))
          .withWatermark(now);
    }
  }

  @VisibleForTesting
  static List<TopicPartition> getAllTopicPartitions(
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
      Map<String, Object> kafkaConsumerConfig,
      Set<String> topics,
      @Nullable Pattern topicPattern) {
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
          if (topicPattern == null || topicPattern.matcher(topicInfo.getKey()).matches()) {
            for (PartitionInfo partition : topicInfo.getValue()) {
              current.add(new TopicPartition(partition.topic(), partition.partition()));
            }
          }
        }
      }
    }
    return current;
  }
}
