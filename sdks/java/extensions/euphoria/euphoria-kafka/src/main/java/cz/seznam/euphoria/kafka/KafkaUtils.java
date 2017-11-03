/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

  public static Producer<byte [], byte []>
  newProducer(String brokerList, Settings config)
  {
    final Properties ps = toProperties(config);
    ps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    ps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    ps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    if (ps.getProperty(ProducerConfig.ACKS_CONFIG) == null) {
      ps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    }
    return new KafkaProducer<>(ps);
  }

  public static Consumer<byte[], byte[]>
  newConsumer(String brokerList, @Nullable String groupId, @Nullable Settings config)
  {
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

  static Map<Integer, Long> getOffsetsBeforeTimestamp(
      String brokers, String topic, long timestamp)
      throws IOException
  {
    String[] servers = brokers.split(",");

    Map<Integer, Long> partitions = new HashMap<>();

    for (String seed : servers) {

      String[] parsedServer = seed.split(":");
      int port = Integer.parseInt(parsedServer[1]);
      String server = parsedServer[0];
      SimpleConsumer consumer = null;

      try {

        consumer = new SimpleConsumer(server, port, 100000, 64 * 1024, "leaderLookup");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {

            String clientName = "Client_" + topic + "_" + part.partitionId();
            SimpleConsumer offsetsConsumer =
                new SimpleConsumer(part.leader().host(), port, 100000, 64 * 1024, clientName);
            try {
              long offsets = getOffsetsForPartition(
                  offsetsConsumer, topic, part.partitionId(), clientName, timestamp);
              if (offsets >= 0) {
                partitions.put(part.partitionId(), offsets);
              }
            } finally {
              offsetsConsumer.close();
            }
          }

          return partitions;
        }
      } catch (Exception e) {
        LOG.debug("Failed to retrieve offsets on {}:{}: {}",
            new Object[]{server, port, e});
      } finally {
        if (consumer != null) consumer.close();
      }
    }

    throw new KafkaException("No bootstrap servers!");
  }

  private static long getOffsetsForPartition(SimpleConsumer consumer,
                                             String topic,
                                             int partition,
                                             String clientName,
                                             long timestamp)
      throws Exception
  {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(timestamp, 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      throw new Exception(
          "Error fetching data Offset Data the Broker. Reason: "
              + response.errorCode(topic, partition));
    }

    long[] offsets = response.offsets(topic, partition);
    return (offsets != null && offsets.length > 0) ? offsets[0] : -1;
  }
}
