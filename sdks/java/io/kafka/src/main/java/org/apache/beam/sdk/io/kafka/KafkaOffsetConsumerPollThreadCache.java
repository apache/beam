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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetConsumerPollThreadCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(KafkaOffsetConsumerPollThreadCache.class);
  private static final Duration OFFSET_THREAD_ALLOWED_NOT_ACCESSED_DURATION =
      Duration.ofMinutes(10);

  private final ExecutorService invalidationExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaOffsetConsumerPollCache-invalidation-%d")
              .build());

  private final ScheduledExecutorService backgroundThread =
      Executors.newScheduledThreadPool(
          0,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaOffsetConsumerPollCache-poll-%d")
              .build());

  private final Cache<KafkaSourceDescriptor, KafkaOffsetConsumerPollThread> offsetThreadCache;

  @SuppressWarnings("method.invocation")
  KafkaOffsetConsumerPollThreadCache() {
    this.offsetThreadCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(
                OFFSET_THREAD_ALLOWED_NOT_ACCESSED_DURATION.toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<KafkaSourceDescriptor, KafkaOffsetConsumerPollThread>
                        notification) -> {
                  LOG.info(
                      "Asynchronously closing offset reader for {}. Reason: {}",
                      notification.getKey(),
                      notification.getCause());
                  asyncCloseOffsetConsumer(
                      checkNotNull(notification.getKey()), checkNotNull(notification.getValue()));
                })
            .build();
  }

  void invalidate(KafkaSourceDescriptor kafkaSourceDescriptor) {
    this.offsetThreadCache.invalidate(kafkaSourceDescriptor);
  }

  KafkaOffsetConsumerPollThread acquireOffsetTrackerThread(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor) {
    try {
      return offsetThreadCache.get(
          kafkaSourceDescriptor,
          () -> {
            Consumer<byte[], byte[]> consumer =
                createAndSetupKafkaEndOffsetConsumer(
                    consumerConfig, consumerFactoryFn, kafkaSourceDescriptor);
            KafkaOffsetConsumerPollThread pollThread = new KafkaOffsetConsumerPollThread();
            pollThread.startOnExecutor(
                backgroundThread, consumer, kafkaSourceDescriptor.getTopicPartition());
            return pollThread;
          });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Consumer<byte[], byte[]> createAndSetupKafkaEndOffsetConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor) {
    LOG.info(
        "Creating a new kafka consumer for tracking backlog for {}",
        kafkaSourceDescriptor.getTopicPartition());
    Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(consumerConfig);
    ConsumerSpEL.evaluateAssign(
        consumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
    return consumer;
  }

  /** Close the reader and log a warning if close fails. */
  private void asyncCloseOffsetConsumer(
      KafkaSourceDescriptor kafkaSourceDescriptor, KafkaOffsetConsumerPollThread offsetPollThread) {
    invalidationExecutor.execute(
        () -> {
          try {
            offsetPollThread.close();
            LOG.info("Finished closing consumer for {}", kafkaSourceDescriptor);
          } catch (IOException e) {
            LOG.warn("Failed to close consumer for {}", kafkaSourceDescriptor, e);
          }
        });
  }
}
