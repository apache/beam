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

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

/**
 * Because the element here is a topic partition, there can be any number of KV pairs per element.
 * As such, we mark this as {@link UnboundedPerElement}.
 *
 * <p>Note, although base {@link org.apache.beam.sdk.io.kafka.KafkaIO} does provide an @GetSize
 * method, it is not required for a Kafka based IO to function properly. Because both IOs use an
 * OffsetRangeTracker, the size of work remaining can be computed using that trackers built-in
 * getProgress function. In KafkaIO, this is enhanced providing a @GetSize method that improves the
 * estimate by keeping track of the size of the Kafka elements, but that is not strictly necessary.
 */
@UnboundedPerElement
public class ReadFromKafka extends DoFn<TopicPartition, KV<byte[], byte[]>> {
  private static final java.time.Duration KAFKA_POLL_TIMEOUT = java.time.Duration.ofSeconds(1);

  final Map<String, Object> consumerConfig;

  public ReadFromKafka(Map<String, Object> consumerConfig) {
    this.consumerConfig = consumerConfig;
  }

  /**
   * Our initial restriction from Kafka is the range from the current consumer position until
   * Long.MAX_VALUE. The reason we do not go from 0 to max value, is that Kafka may have cleaned up
   * old elements, such that the minimum offset is not 0, or is that our consumer could be using a
   * group identifier that has already consumed from this topic partition.
   */
  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element TopicPartition topicPartition) {
    try (Consumer<byte[], byte[]> offsetConsumer =
        new KafkaConsumer<byte[], byte[]>(consumerConfig)) {
      offsetConsumer.assign(ImmutableList.of(topicPartition));
      long startOffset = offsetConsumer.position(topicPartition);

      return new OffsetRange(startOffset, Long.MAX_VALUE);
    }
  }

  /**
   * Because elements can be added to Kafka continuously, we want our offset range tracker to
   * reflect this. As such, we use a {@link GrowableOffsetRangeTracker} to poll Kafka and see how
   * far out the end of the range is.
   *
   * <p>If the restriction provided by the runner has a getTo() of Long.MAX_VALUE, we don't need to
   * query Kafka to know how far out to grab our restriction.
   */
  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element TopicPartition topicPartition, @Restriction OffsetRange restriction) {
    if (restriction.getTo() < Long.MAX_VALUE) {
      return new OffsetRangeTracker(restriction);
    }
    ExampleKafkaReadOffsetEstimator offsetPoller =
        new ExampleKafkaReadOffsetEstimator(
            new KafkaConsumer<byte[], byte[]>(
                ExampleKafkaReadIOUtils.getOffsetConsumerConfig(
                    "tracker-" + topicPartition, consumerConfig)),
            topicPartition);
    return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetPoller);
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  /**
   * For this example, we estimate the watermark based on the clock time we receive elements from
   * Kafka. As such, we use a manual estimator that we update during ProcessElement.
   */
  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  /**
   * @param topicPartition is required to know what topic this bundle is being consumed from
   * @param tracker is required to claim records as we receive them
   * @param watermarkEstimator is required to be updated as we receive records
   * @param receiver for outputting the KV Pairs
   */
  @ProcessElement
  public ProcessContinuation processElement(
      @Element TopicPartition topicPartition,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      OutputReceiver<KV<byte[], byte[]>> receiver) {
    try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerConfig)) {
      consumer.assign(ImmutableList.of(topicPartition));
      long startOffset = tracker.currentRestriction().getFrom();
      // Seek to the lowest element for the restriction before we poll
      consumer.seek(topicPartition, startOffset);
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();

      while (true) {
        rawRecords = consumer.poll(KAFKA_POLL_TIMEOUT);
        // When there are no records available for the current TopicPartition, self-checkpoint
        // and move to process the next element.
        if (rawRecords.isEmpty()) {
          // If we receive no records, we still should update the watermark. This ensures that
          // watermark driven logic continues to work downstream, even if the data on Kafka is
          // sparse
          ((ManualWatermarkEstimator<Instant>) watermarkEstimator).setWatermark(Instant.now());

          // We continue processing, as there is still work to be done in the range of the tracker
          return ProcessContinuation.resume();
        }
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
          if (!tracker.tryClaim(rawRecord.offset())) {
            // Because we poll from Kafka, as opposed to querying for specific rows, we may receive
            // records that are beyond the scope of the tracker. If we do, we need to not process
            // those records, and we return stop() to indicate that we are done with that tracker
            return ProcessContinuation.stop();
          }
          // If we are able to claim the record, update the watermark, and output the KV pair.
          Instant now = Instant.now();
          ((ManualWatermarkEstimator<Instant>) watermarkEstimator).setWatermark(now);
          receiver.outputWithTimestamp(KV.of(rawRecord.key(), rawRecord.value()), now);
        }
      }
    }
  }
}
