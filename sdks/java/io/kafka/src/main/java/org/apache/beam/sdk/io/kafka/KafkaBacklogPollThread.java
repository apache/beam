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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.kafka.ReadFromKafkaDoFn.KafkaLatestOffsetEstimator;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

public class KafkaBacklogPollThread {

  @VisibleForTesting static final String METRIC_NAMESPACE =
KafkaUnboundedReader.METRIC_NAMESPACE;
  @VisibleForTesting
  static final String RAW_SIZE_METRIC_PREFIX = KafkaUnboundedReader.RAW_SIZE_METRIC_PREFIX;

  private transient @Nullable Map<TopicPartition, KafkaLatestOffsetEstimator>
offsetEstimatorCache;

  KafkaBacklogPollThread() {
    consumer = null;
  }

  private @Nullable Consumer<byte[], byte[]> consumer;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaBacklogPollThread.class);

  private @Nullable Future<?> pollFuture;

  private static Map<TopicPartition, Long> backlogMap = new HashMap<>();

  void startOnExecutor(ExecutorService executorService, Consumer<byte[], byte[]> consumer) {
    this.consumer = consumer;
    // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
    // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
    // like 100 milliseconds does not work well. This along with large receive buffer for
    // consumer achieved best throughput in tests (see `defaultConsumerProperties`).
    pollFuture = executorService.submit(this::backlogPollLoop);
  }

  void close() throws IOException {
    if (consumer == null) {
      LOG.debug("Closing backlog poll thread that was never started.");
      return;
    }
    Preconditions.checkStateNotNull(pollFuture);
  }

  private static Supplier<Long> backlogQuery(
    Consumer<byte[], byte[]> offsetConsumer, TopicPartition topicPartition) {
    return Suppliers.memoizeWithExpiration(
        () -> {
          synchronized (offsetConsumer) {
            ConsumerSpEL.evaluateSeek2End(offsetConsumer, topicPartition);
            return offsetConsumer.position(topicPartition);
          }
        },
        1,
        TimeUnit.SECONDS);
  }

  // should there be a separate thread per partition?
  private void backlogPollLoop() {
    // OffsetEstimators are cached for each topic-partition because they hold a stateful connection,
    // so we want to minimize the amount of connections that we start and track with Kafka. Another
    // point is that it has a memoized backlog, and this should make that more reusable estimations.
    Preconditions.checkStateNotNull(this.offsetEstimatorCache);
    for (Map.Entry<TopicPartition, KafkaLatestOffsetEstimator> tp :
        offsetEstimatorCache.entrySet()) {
      TopicPartition topicPartition = tp.getKey();
      KafkaLatestOffsetEstimator offsetEstimator = tp.getValue();

      // Insert into backlog map. and update the tracker if the previous one is now closed.
      if (backlogMap.get(topicPartition) == null || offsetEstimator.isClosed()) {
        Preconditions.checkStateNotNull(this.consumer);
        java.util.function.Supplier<Long> memoizedBacklog = backlogQuery(this.consumer, topicPartition);
        LOG.info("xxx the backlog is {}", memoizedBacklog.get());
        backlogMap.put(topicPartition, memoizedBacklog.get());
      }
    }

    // Update backlog metrics.
    Preconditions.checkStateNotNull(this.offsetEstimatorCache);
    for (TopicPartition topicPartition : offsetEstimatorCache.keySet()) {
      Preconditions.checkStateNotNull(backlogMap.get(topicPartition));
      Long backlogValue = backlogMap.get(topicPartition);
      Gauge backlog =
          Metrics.gauge(
              METRIC_NAMESPACE,
              RAW_SIZE_METRIC_PREFIX + "backlogBytes_" + topicPartition.toString());
      backlog.set(backlogValue);
    }
  }
}
