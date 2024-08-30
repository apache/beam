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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Common utility functions and default configurations for {@link KafkaIO.Read} and {@link
 * KafkaIO.ReadSourceDescriptors}.
 */
public final class KafkaIOUtils {
  // A set of config defaults.
  static final Map<String, Object> DEFAULT_CONSUMER_PROPERTIES =
      ImmutableMap.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getName(),
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getName(),

          // Use large receive buffer. Once KAFKA-3135 is fixed, this _may_ not be required.
          // with default value of 32K, It takes multiple seconds between successful polls.
          // All the consumer work is done inside poll(), with smaller send buffer size, it
          // takes many polls before a 1MB chunk from the server is fully read. In my testing
          // about half of the time select() inside kafka consumer waited for 20-30ms, though
          // the server had lots of data in tcp send buffers on its side. Compared to default,
          // this setting increased throughput by many fold (3-4x).
          ConsumerConfig.RECEIVE_BUFFER_CONFIG,
          512 * 1024,

          // default to latest offset when we are not resuming.
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          "latest",
          // disable auto commit of offsets. we don't require group_id. could be enabled by user.
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          false);

  // A set of properties that are not required or don't make sense for our consumer.
  public static final Map<String, String> DISALLOWED_CONSUMER_PROPERTIES =
      ImmutableMap.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyDeserializer instead",
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueDeserializer instead"
          // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
          //     lets allow these, applications can have better resume point for restarts.
          );

  // default Kafka 0.9 Consumer supplier.
  static final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      KAFKA_CONSUMER_FACTORY_FN = KafkaConsumer::new;

  /**
   * Returns a new config map which is merge of current config and updates. Verifies the updates do
   * not includes ignored properties.
   */
  static Map<String, Object> updateKafkaProperties(
      Map<String, Object> currentConfig, Map<String, Object> updates) {

    for (String key : updates.keySet()) {
      checkArgument(
          !DISALLOWED_CONSUMER_PROPERTIES.containsKey(key),
          "No need to configure '%s'. %s",
          key,
          DISALLOWED_CONSUMER_PROPERTIES.get(key));
    }

    Map<String, Object> config = new HashMap<>(currentConfig);
    config.putAll(updates);

    return config;
  }

  static Map<String, Object> getOffsetConsumerConfig(
      String name, @Nullable Map<String, Object> offsetConfig, Map<String, Object> consumerConfig) {
    Map<String, Object> offsetConsumerConfig = new HashMap<>(consumerConfig);
    offsetConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Object groupId = consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
    // override group_id and disable auto_commit so that it does not interfere with main consumer
    String offsetGroupId =
        String.format(
            "%s_offset_consumer_%d_%s",
            name, new Random().nextInt(Integer.MAX_VALUE), (groupId == null ? "none" : groupId));
    offsetConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, offsetGroupId);

    if (offsetConfig != null) {
      offsetConsumerConfig.putAll(offsetConfig);
    }

    // Force read isolation level to 'read_uncommitted' for offset consumer. This consumer
    // fetches latest offset for two reasons : (a) to calculate backlog (number of records
    // yet to be consumed) (b) to advance watermark if the backlog is zero. The right thing to do
    // for (a) is to leave this config unchanged from the main config (i.e. if there are records
    // that can't be read because of uncommitted records before them, they shouldn't
    // ideally count towards backlog when "read_committed" is enabled. But (b)
    // requires finding out if there are any records left to be read (committed or uncommitted).
    // Rather than using two separate consumers we will go with better support for (b). If we do
    // hit a case where a lot of records are not readable (due to some stuck transactions), the
    // pipeline would report more backlog, but would not be able to consume it. It might be ok
    // since CPU consumed on the workers would be low and will likely avoid unnecessary upscale.
    offsetConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");

    return offsetConsumerConfig;
  }

  // Maintains approximate average over last 1000 elements
  static class MovingAvg {
    private static final int MOVING_AVG_WINDOW = 1000;
    private double avg = 0;
    private long numUpdates = 0;

    void update(double quantity) {
      numUpdates++;
      avg += (quantity - avg) / Math.min(MOVING_AVG_WINDOW, numUpdates);
    }

    double get() {
      return avg;
    }
  }
}
