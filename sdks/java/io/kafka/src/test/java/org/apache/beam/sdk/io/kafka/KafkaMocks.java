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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class KafkaMocks {

  public static final class SendErrorProducer extends MockProducer<Integer, Long> {

    public SendErrorProducer() {
      super(false, new IntegerSerializer(), new LongSerializer());
    }

    @Override
    public synchronized Future<RecordMetadata> send(
        ProducerRecord<Integer, Long> record, Callback callback) {
      throw new KafkaException("fakeException");
    }
  }

  public static final class SendErrorProducerFactory
      implements SerializableFunction<Map<String, Object>, Producer<Integer, Long>> {
    public SendErrorProducerFactory() {}

    @Override
    public Producer<Integer, Long> apply(Map<String, Object> input) {
      return new SendErrorProducer();
    }
  }

  public static final class EndOffsetErrorConsumerFactory
      implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
    public EndOffsetErrorConsumerFactory() {}

    @Override
    public MockConsumer<byte[], byte[]> apply(Map<String, Object> input) {
      final MockConsumer<byte[], byte[]> consumer;
      if (input.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
        consumer =
            new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
              @Override
              public synchronized Map<TopicPartition, Long> endOffsets(
                  Collection<TopicPartition> partitions) {
                throw new KafkaException("fakeException");
              }
            };
      } else {
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
      }
      consumer.updatePartitions(
          "topic_a",
          Collections.singletonList(
              new PartitionInfo("topic_a", 1, new Node(1, "myServer1", 9092), null, null)));
      consumer.updateBeginningOffsets(
          Collections.singletonMap(new TopicPartition("topic_a", 1), 0L));
      consumer.updateEndOffsets(Collections.singletonMap(new TopicPartition("topic_a", 1), 0L));
      return consumer;
    }
  }

  public static final class PartitionGrowthMockConsumer extends MockConsumer<byte[], byte[]>
      implements Serializable {

    private List<List<KV<String, Integer>>> partitions;
    private int index = 0;

    public PartitionGrowthMockConsumer() {
      super(null);
    }

    public PartitionGrowthMockConsumer(List<List<KV<String, Integer>>> partitions) {
      super(null);
      this.partitions = partitions;
    }

    @Override
    public synchronized List<PartitionInfo> partitionsFor(String topic) {
      List<KV<String, Integer>> partitionInfos = partitions.get(index);
      index++;
      return partitionInfos.stream()
          .map(kv -> new PartitionInfo(kv.getKey(), kv.getValue(), null, null, null))
          .collect(Collectors.toList());
    }
  }
}
