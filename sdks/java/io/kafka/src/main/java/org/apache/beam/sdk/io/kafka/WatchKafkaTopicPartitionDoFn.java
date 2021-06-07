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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A stateful {@linkl DoFn} that emits new available {@link TopicPartition} regularly.
 *
 * <p>Please refer to
 * https://docs.google.com/document/d/1FU3GxVRetHPLVizP3Mdv6mP5tpjZ3fd99qNjUI5DT5k/edit# for more
 * details.
 */
@SuppressWarnings({"nullness"})
@Experimental
class WatchKafkaTopicPartitionDoFn extends DoFn<KV<byte[], byte[]>, KafkaSourceDescriptor> {

  private static final Duration DEFAULT_CHECK_DURATION = Duration.standardHours(1);
  private static final String TIMER_ID = "watch_timer";
  private static final String STATE_ID = "topic_partition_set";
  private final Duration checkDuration;

  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      kafkaConsumerFactoryFn;
  private final SerializableFunction<TopicPartition, Boolean> checkStopReadingFn;
  private final Map<String, Object> kafkaConsumerConfig;
  private final Instant startReadTime;

  private static final String COUNTER_NAMESPACE = "watch_kafka_topic_partition";

  private final List<String> topics;

  WatchKafkaTopicPartitionDoFn(
      Duration checkDuration,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
      SerializableFunction<TopicPartition, Boolean> checkStopReadingFn,
      Map<String, Object> kafkaConsumerConfig,
      Instant startReadTime,
      List<String> topics) {
    this.checkDuration = checkDuration == null ? DEFAULT_CHECK_DURATION : checkDuration;
    this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
    this.checkStopReadingFn = checkStopReadingFn;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.startReadTime = startReadTime;
    this.topics = topics;
  }

  @TimerId(TIMER_ID)
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @StateId(STATE_ID)
  private final StateSpec<BagState<TopicPartition>> bagStateSpec =
      StateSpecs.bag(new TopicPartitionCoder());

  @VisibleForTesting
  Set<TopicPartition> getAllTopicPartitions() {
    Set<TopicPartition> current = new HashSet<>();
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

  @ProcessElement
  public void processElement(
      @TimerId(TIMER_ID) Timer timer,
      @StateId(STATE_ID) BagState<TopicPartition> existingTopicPartitions,
      OutputReceiver<KafkaSourceDescriptor> outputReceiver) {
    // For the first time, we emit all available TopicPartition and write them into State.
    Set<TopicPartition> current = getAllTopicPartitions();
    current.forEach(
        topicPartition -> {
          if (checkStopReadingFn == null || !checkStopReadingFn.apply(topicPartition)) {
            Counter foundedTopicPartition =
                Metrics.counter(COUNTER_NAMESPACE, topicPartition.toString());
            foundedTopicPartition.inc();
            existingTopicPartitions.add(topicPartition);
            outputReceiver.output(
                KafkaSourceDescriptor.of(topicPartition, null, startReadTime, null));
          }
        });

    timer.offset(checkDuration).setRelative();
  }

  @OnTimer(TIMER_ID)
  public void onTimer(
      @TimerId(TIMER_ID) Timer timer,
      @StateId(STATE_ID) BagState<TopicPartition> existingTopicPartitions,
      OutputReceiver<KafkaSourceDescriptor> outputReceiver) {
    Set<TopicPartition> readingTopicPartitions = new HashSet<>();
    existingTopicPartitions
        .read()
        .forEach(
            topicPartition -> {
              readingTopicPartitions.add(topicPartition);
            });
    existingTopicPartitions.clear();

    Set<TopicPartition> currentAll = this.getAllTopicPartitions();

    // Emit new added TopicPartitions.
    Set<TopicPartition> newAdded = Sets.difference(currentAll, readingTopicPartitions);
    newAdded.forEach(
        topicPartition -> {
          if (checkStopReadingFn == null || !checkStopReadingFn.apply(topicPartition)) {
            Counter foundedTopicPartition =
                Metrics.counter(COUNTER_NAMESPACE, topicPartition.toString());
            foundedTopicPartition.inc();
            outputReceiver.output(
                KafkaSourceDescriptor.of(topicPartition, null, startReadTime, null));
          }
        });

    // Update the State.
    currentAll.forEach(
        topicPartition -> {
          if (checkStopReadingFn == null || !checkStopReadingFn.apply(topicPartition)) {
            existingTopicPartitions.add(topicPartition);
          }
        });

    // Reset the timer.
    timer.set(Instant.now().plus(checkDuration.getMillis()));
  }
}
