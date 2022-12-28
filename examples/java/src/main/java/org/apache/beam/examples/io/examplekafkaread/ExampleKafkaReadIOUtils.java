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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ExampleKafkaReadIOUtils {
  static final Map<String, Object> DEFAULT_CONSUMER_PROPERTIES =
      ImmutableMap.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
          ConsumerConfig.RECEIVE_BUFFER_CONFIG, 512 * 1024,
          // default to latest offset when we are not resuming.
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
          // disable auto commit of offsets. we don't require group_id. could be enabled by user.
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);


  static Map<String, Object> getOffsetConsumerConfig(
      String name, Map<String, Object> consumerConfig) {
    Map<String, Object> offsetConsumerConfig = new HashMap<>(consumerConfig);
    offsetConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Object groupId = consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
    // override group_id and disable auto_commit so that it does not interfere with main consumer
    String offsetGroupId =
        String.format(
            "%s_offset_consumer_%d_%s",
            name, new Random().nextInt(Integer.MAX_VALUE), (groupId == null ? "none" : groupId));
    offsetConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, offsetGroupId);

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


  /**
   * Returns a new config map which is merge of current config and updates. Verifies the updates do
   * not includes ignored properties.
   */
  static Map<String, Object> updateKafkaProperties(
      Map<String, Object> currentConfig, Map<String, Object> updates) {

    Map<String, Object> config = new HashMap<>(currentConfig);
    config.putAll(updates);

    return config;
  }

}
