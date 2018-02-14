/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.kafka;

import cz.seznam.euphoria.core.util.Settings;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  private static Properties toProperties(@Nullable Settings properties) {
    final Properties ps = new Properties();
    if (properties != null) {
      for (Map.Entry<String, String> e : properties.getAll().entrySet()) {
        ps.setProperty(e.getKey(), e.getValue());
      }
    }
    return ps;
  }

  static Producer<byte [], byte []> newProducer(String brokerList, Settings config) {
    final Properties ps = toProperties(config);
    ps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    ps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    ps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    if (ps.getProperty(ProducerConfig.ACKS_CONFIG) == null) {
      ps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    }
    return new KafkaProducer<>(ps);
  }

  static Consumer<byte[], byte[]> newConsumer(
      String brokerList, @Nullable String groupId, @Nullable Settings config) {
    Properties ps = toProperties(config);
    ps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    ps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    ps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    if (groupId != null) {
      ps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    if (ps.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null) {
      final String name = "euphoria.client-id-" + UUID.randomUUID().toString();
      LOG.warn("Autogenerating name of consumer's {} to {}",
          ConsumerConfig.CLIENT_ID_CONFIG, name);
      ps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, name);
    }
    return new KafkaConsumer<>(ps);
  }

}
