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

import static
org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static
org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import
org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.beam.sdk.io.kafka.KafkaIOUtils;

public class KafkaBacklogPollThreadCache {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaBacklogPollThreadCache.class);
  private final ExecutorService invalidationExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaBacklogPollCache-invalidation-%d")
              .build());
  private final ExecutorService backgroundThreads =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaBacklogPollCache-poll-%d")
              .build());

  // Note on thread safety. This class is thread safe because:
  //   - Guava Cache is thread safe.
  //   - There is no state other than Cache.
  //   - API is strictly a 1:1 wrapper over Cache API (not counting cache.cleanUp() calls).
  //       - i.e. it does not invoke more than one call, which could make it inconsistent.
  // If any of these conditions changes, please test ensure and test thread safety.

  private static class CacheKey {
    final Map<String, Object> consumerConfig;
    final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn;
    final KafkaSourceDescriptor descriptor;

    CacheKey(
        Map<String, Object> consumerConfig,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
        KafkaSourceDescriptor descriptor) {
      this.consumerConfig = consumerConfig;
      this.consumerFactoryFn = consumerFactoryFn;
      this.descriptor = descriptor;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null) {
        return false;
      }
      if (!(other instanceof CacheKey)) {
        return false;
      }
      CacheKey otherKey = (CacheKey) other;
      return descriptor.equals(otherKey.descriptor)
          && consumerFactoryFn.equals(otherKey.consumerFactoryFn)
          && consumerConfig.equals(otherKey.consumerConfig) ;
    }

    @Override
    public int hashCode() {
      return Objects.hash(descriptor, consumerFactoryFn, consumerConfig);
    }
  }

  private static class CacheEntry {

    final KafkaBacklogPollThread pollThread;

    CacheEntry(KafkaBacklogPollThread pollThread) {
      this.pollThread = pollThread;
    }
  }

  private final Duration cacheDuration = Duration.ofMinutes(1);
  private final Cache<CacheKey, CacheEntry> cache;

  @SuppressWarnings("method.invocation")
  KafkaBacklogPollThreadCache() {
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration.toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<CacheKey, CacheEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info(
                        "Asynchronously closing backlog poll for {} as it has been idle for over {}",
                        notification.getKey(),
                        cacheDuration);
                    asyncCloseConsumer(
                        checkNotNull(notification.getKey()),
checkNotNull(notification.getValue()));
                  }
                })
            .build();
  }

  KafkaBacklogPollThread acquireConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor  // needed to get updated consumer config
      ) {
    cache.cleanUp();

    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    LOG.info(
        "Creating Kafka consumer for process continuation for {}",
        kafkaSourceDescriptor.getTopicPartition());

    TopicPartition topicPartition = kafkaSourceDescriptor.getTopicPartition();
    Consumer<byte[], byte[]> offsetConsumer =
    consumerFactoryFn.apply(
        KafkaIOUtils.getOffsetConsumerConfig(
            "tracker-" + topicPartition, consumerConfig, updatedConsumerConfig));

    // This should create whatever is needed to poll the backlog
    KafkaBacklogPollThread pollThread = new KafkaBacklogPollThread();
    pollThread.startOnExecutor(backgroundThreads, offsetConsumer);
    return pollThread;
  }

  /** Close the reader and log a warning if close fails. */
  private void asyncCloseConsumer(CacheKey key, CacheEntry entry) {
    invalidationExecutor.execute(
        () -> {
          try {
            entry.pollThread.close();
            LOG.info("Finished closing consumer for {}", key);
          } catch (IOException e) {
            LOG.warn("Failed to close consumer for {}", key, e);
          }
        });
  }

  void releaseConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor,
      KafkaBacklogPollThread pollThread
      ) {
    CacheKey key = new CacheKey(consumerConfig, consumerFactoryFn, kafkaSourceDescriptor);
    CacheEntry existing = cache.asMap().putIfAbsent(key, new CacheEntry(pollThread));
    if (existing != null) {
      LOG.warn("Topic {} and partition {} combination does not exist in cache", kafkaSourceDescriptor.getTopic(), kafkaSourceDescriptor.getPartition());
      asyncCloseConsumer(key, existing);
    }
    cache.cleanUp();
  }

  private Map<String, Object> overrideBootstrapServersConfig(
      Map<String, Object> currentConfig, KafkaSourceDescriptor description) {
    checkState(
        currentConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
            || description.getBootStrapServers() != null);
    Map<String, Object> config = new HashMap<>(currentConfig);
    if (description.getBootStrapServers() != null &&
!description.getBootStrapServers().isEmpty()) {
      config.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          String.join(",", description.getBootStrapServers()));
    }
    return config;
  }
}
