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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerPollThreadCache {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPollThreadCache.class);
  private final ExecutorService invalidationExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaConsumerPollCache-invalidation-%d")
              .build());
  private final ExecutorService backgroundThreads =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaConsumerPollCache-poll-%d")
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
          && consumerConfig.equals(otherKey.consumerConfig);
    }

    @Override
    public int hashCode() {
      return Objects.hash(descriptor, consumerFactoryFn, consumerConfig);
    }
  }

  private static class CacheEntry {

    final KafkaConsumerPollThread pollThread;
    final long offset;

    CacheEntry(KafkaConsumerPollThread pollThread, long offset) {
      this.pollThread = pollThread;
      this.offset = offset;
    }
  }

  private final Duration cacheDuration = Duration.ofMinutes(1);
  private final Cache<CacheKey, CacheEntry> cache;

  @SuppressWarnings("method.invocation")
  KafkaConsumerPollThreadCache() {
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration.toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<CacheKey, CacheEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info(
                        "Asynchronously closing reader for {} as it has been idle for over {}",
                        notification.getKey(),
                        cacheDuration);
                    asyncCloseConsumer(
                        checkNotNull(notification.getKey()), checkNotNull(notification.getValue()));
                  }
                })
            .build();
  }

  KafkaConsumerPollThread acquireConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor,
      long offset) {
    CacheKey key = new CacheKey(consumerConfig, consumerFactoryFn, kafkaSourceDescriptor);
    CacheEntry entry = cache.asMap().remove(key);
    cache.cleanUp();
    if (entry != null) {
      if (entry.offset == offset) {
        return entry.pollThread;
      } else {
        // Offset doesn't match, close.
        LOG.info("Closing consumer as it is no longer valid {}", kafkaSourceDescriptor);
        asyncCloseConsumer(key, entry);
      }
    }

    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    LOG.info(
        "Creating Kafka consumer for process continuation for {}",
        kafkaSourceDescriptor.getTopicPartition());
    Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(updatedConsumerConfig);
    ConsumerSpEL.evaluateAssign(
        consumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
    consumer.seek(kafkaSourceDescriptor.getTopicPartition(), offset);
    KafkaConsumerPollThread pollThread = new KafkaConsumerPollThread();
    pollThread.startOnExecutor(backgroundThreads, consumer);
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
      KafkaConsumerPollThread pollThread,
      long offset) {
    CacheKey key = new CacheKey(consumerConfig, consumerFactoryFn, kafkaSourceDescriptor);
    CacheEntry existing = cache.asMap().putIfAbsent(key, new CacheEntry(pollThread, offset));
    if (existing != null) {
      LOG.warn("Unexpected collision of topic and partition");
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
    if (description.getBootStrapServers() != null && !description.getBootStrapServers().isEmpty()) {
      config.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          String.join(",", description.getBootStrapServers()));
    }
    return config;
  }
}
