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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetConsumerPollThread {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetConsumerPollThread.class);
  private @Nullable Consumer<byte[], byte[]> consumer;
  private @Nullable TopicPartition topicPartition;
  private final AtomicLong collectedEndOffset = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private @Nullable Future<?> offsetTrackerFuture;

  KafkaOffsetConsumerPollThread() {
    consumer = null;
    offsetTrackerFuture = null;
    topicPartition = null;
  }

  void startOnExecutor(
      ScheduledExecutorService executorService,
      Consumer<byte[], byte[]> consumer,
      TopicPartition topicPartition) {
    this.consumer = consumer;
    this.topicPartition = topicPartition;
    this.offsetTrackerFuture =
        executorService.scheduleAtFixedRate(
            this::fetchEndOffsetFromKafka, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void fetchEndOffsetFromKafka() {
    Consumer<byte[], byte[]> consumer = checkStateNotNull(this.consumer);
    TopicPartition topicPartition = checkStateNotNull(this.topicPartition);

    if (closed.get()) {
      return;
    }

    Long currentEndOffset =
        consumer.endOffsets(ImmutableList.of(topicPartition)).get(topicPartition);
    if (currentEndOffset != null) {
      collectedEndOffset.set(currentEndOffset);
    } else {
      LOG.warn("Unable to get an end offset for {}", topicPartition);
    }
  }

  void close() throws IOException {
    if (consumer == null) {
      LOG.debug("Closing consumer poll thread that was never started.");
      return;
    }
    closed.set(true);
    Closeables.close(consumer, true); // todo not sure about the order
    checkStateNotNull(offsetTrackerFuture).cancel(true); // todo if or checkStateNotNull
  }

  long readEndOffset() {
    return collectedEndOffset.get();
  }

  boolean isClosed() {
    return closed.get();
  }
}
