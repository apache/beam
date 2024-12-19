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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.function.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.AtomicLongMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConcurrentConsumer<K, V> implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentConsumer.class);

  private final ConsumerPhaser phaser;
  private final Consumer<K, V> consumer;
  private final Duration pollDuration;
  private final AtomicLongMap<TopicPartition> times;
  private final AtomicLongMap<TopicPartition> positions;
  private final Supplier<Metric> recordsLagMax;
  private final Map<TopicPartition, Supplier<Metric>> partitionRecordsLag;
  private ConsumerRecords<K, V> pollResult;
  private Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesResult;

  private final class ConsumerPhaser extends Phaser {
    @Override
    protected boolean onAdvance(final int phase, final int registeredParties) {
      try {
        final Map<TopicPartition, Long> positionsView = positions.asMap();
        final Set<TopicPartition> prevAssignment = consumer.assignment();
        final Set<TopicPartition> nextAssignment = positionsView.keySet();

        if (!times.isEmpty()) {
          offsetsForTimesResult = consumer.offsetsForTimes(times.asMap());
          times.clear();
        }

        if (!prevAssignment.equals(nextAssignment)) {
          consumer.assign(nextAssignment);
        }

        positionsView.forEach(
            (tp, o) -> {
              if (o == Long.MIN_VALUE) {
                consumer.pause(Collections.singleton(tp));
              } else if (!prevAssignment.contains(tp)) {
                consumer.seek(tp, o);
              }
            });

        if (consumer.paused().size() != nextAssignment.size()) {
          pollResult = consumer.poll(pollDuration.toMillis());
        }

        nextAssignment.forEach(tp -> positions.put(tp, consumer.position(tp)));
        return false;
      } catch (WakeupException e) {
        if (!this.isTerminated()) {
          LOG.error("Unexpected wakeup while running", e);
        }
      } catch (Exception e) {
        LOG.error("Exception while reading from Kafka", e);
      }
      return true;
    }
  }

  ConcurrentConsumer(final Consumer<K, V> consumer, final Duration pollDuration) {
    this.phaser = new ConsumerPhaser();
    this.consumer = consumer;
    this.pollDuration = pollDuration;
    this.times = AtomicLongMap.create();
    this.positions = AtomicLongMap.create();
    this.recordsLagMax =
        Suppliers.memoize(
            () ->
                this.consumer.metrics().values().stream()
                    .filter(
                        m ->
                            "consumer-fetch-manager-metrics".equals(m.metricName().group())
                                && "records-lag-max".equals(m.metricName().name())
                                && !m.metricName().tags().containsKey("topic")
                                && !m.metricName().tags().containsKey("partition"))
                    .findAny()
                    .get());
    this.partitionRecordsLag = new ConcurrentHashMap<>();
    this.pollResult = ConsumerRecords.empty();
    this.offsetsForTimesResult = Collections.emptyMap();
  }

  @Override
  public void close() {
    this.phaser.forceTermination();
    try {
      this.consumer.wakeup();
      this.consumer.close();
    } catch (Exception e) {
      LOG.error("Exception while closing Kafka consumer", e);
    }
    this.times.clear();
    this.positions.clear();
    this.pollResult = ConsumerRecords.empty();
    this.offsetsForTimesResult = Collections.emptyMap();
  }

  boolean isClosed() {
    return this.phaser.isTerminated();
  }

  Map<MetricName, ? extends Metric> metrics() {
    return this.consumer.metrics();
  }

  long currentLagOrMaxLag(final TopicPartition topicPartition) {
    final Supplier<Metric> metric =
        this.partitionRecordsLag.getOrDefault(topicPartition, this.recordsLagMax);
    try {
      return ((Number) metric.get().metricValue()).longValue();
    } catch (Exception e) {
      return 0;
    }
  }

  long position(final TopicPartition topicPartition) {
    return this.positions.get(topicPartition) & Long.MAX_VALUE;
  }

  long initialOffsetForPartition(final TopicPartition topicPartition) {
    // Offsets start at 0 and there is no position to advance to beyond Long.MAX_VALUE.
    // The sign bit indicates that an assignment is paused.
    checkState(this.phaser.register() >= 0);
    this.positions.put(topicPartition, Long.MIN_VALUE);

    // Synchronize and throw if the consumer was terminated in between.
    checkState(this.phaser.arriveAndAwaitAdvance() >= 0);

    // Removal will revoke the assignment when the phase advances.
    final long result = this.positions.remove(topicPartition);

    // Synchronize and ignore the consumer status since the result is already known.
    this.phaser.arriveAndDeregister();

    // Since Long.MIN_VALUE only has the sign bit set, this will return 0 as a default value if no
    // position could be determined.
    return result & Long.MAX_VALUE;
  }

  @Nullable
  OffsetAndTimestamp initialOffsetForTime(final TopicPartition topicPartition, final long time) {
    // Request the offset closest to the provided time.
    checkState(this.phaser.register() >= 0);
    this.times.put(topicPartition, time);

    // Synchronize and throw if the consumer was terminated in between.
    checkState(this.phaser.arriveAndAwaitAdvance() >= 0);
    final Map<TopicPartition, OffsetAndTimestamp> result = this.offsetsForTimesResult;

    // Synchronize and ignore the consumer status since the result is already known.
    this.phaser.arriveAndDeregister();

    return result.get(topicPartition);
  }

  void assignAndSeek(final TopicPartition topicPartition, final long offset) {
    checkState(this.phaser.register() >= 0);

    this.positions.put(topicPartition, offset);
    this.partitionRecordsLag.computeIfAbsent(
        topicPartition,
        k ->
            Suppliers.memoize(
                () ->
                    this.consumer.metrics().values().stream()
                        .filter(
                            m ->
                                "consumer-fetch-manager-metrics".equals(m.metricName().group())
                                    && "records-lag-max".equals(m.metricName().name())
                                    && k.topic()
                                        .replace('.', '_')
                                        .equals(m.metricName().tags().get("topic"))
                                    && Integer.toString(k.partition())
                                        .equals(m.metricName().tags().get("partition")))
                        .findAny()
                        .get()));
  }

  List<ConsumerRecord<K, V>> poll(final TopicPartition topicPartition) {
    checkState(this.phaser.arriveAndAwaitAdvance() >= 0);

    return this.pollResult.records(topicPartition);
  }

  void unassign(final TopicPartition topicPartition) {
    this.positions.remove(topicPartition);
    this.partitionRecordsLag.remove(topicPartition);

    this.phaser.arriveAndDeregister();
  }
}
