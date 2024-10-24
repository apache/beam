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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/** This is a mock BeamKafkaTable. It will use a Mock Consumer. */
public class KafkaTestTable extends BeamKafkaTable {
  private final int partitionsPerTopic;
  private final List<KafkaTestRecord> records;
  private static final String TIMESTAMP_TYPE_CONFIG = "test.timestamp.type";

  public KafkaTestTable(Schema beamSchema, List<String> topics, int partitionsPerTopic) {
    super(beamSchema, "server:123", topics);
    this.partitionsPerTopic = partitionsPerTopic;
    this.records = new ArrayList<>();
  }

  @Override
  protected KafkaIO.Read<byte[], byte[]> createKafkaRead() {
    return super.createKafkaRead().withConsumerFactoryFn(this::mkMockConsumer);
  }

  public void addRecord(KafkaTestRecord record) {
    records.add(record);
  }

  @Override
  double computeRate(int numberOfRecords) throws NoEstimationException {
    return super.computeRate(mkMockConsumer(new HashMap<>()), numberOfRecords);
  }

  public void setNumberOfRecordsForRate(int numberOfRecordsForRate) {
    this.numberOfRecordsForRate = numberOfRecordsForRate;
  }

  private MockConsumer<byte[], byte[]> mkMockConsumer(Map<String, Object> config) {
    OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.EARLIEST;
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> kafkaRecords = new HashMap<>();
    Map<String, List<PartitionInfo>> partitionInfoMap = new HashMap<>();
    Map<String, List<TopicPartition>> partitionMap = new HashMap<>();

    // Create Topic Partitions
    for (String topic : this.getTopics()) {
      List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
      List<TopicPartition> topicPartitions = new ArrayList<>(partitionsPerTopic);
      for (int i = 0; i < partitionsPerTopic; i++) {
        TopicPartition tp = new TopicPartition(topic, i);
        topicPartitions.add(tp);
        partIds.add(new PartitionInfo(topic, i, null, null, null));
        kafkaRecords.put(tp, new ArrayList<>());
      }
      partitionInfoMap.put(topic, partIds);
      partitionMap.put(topic, topicPartitions);
    }

    TimestampType timestampType =
        TimestampType.forName(
            (String)
                config.getOrDefault(
                    TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString()));

    for (KafkaTestRecord record : this.records) {
      int partitionIndex = record.getKey().hashCode() % partitionsPerTopic;
      TopicPartition tp = partitionMap.get(record.getTopic()).get(partitionIndex);
      byte[] key = record.getKey().getBytes(UTF_8);
      byte[] value = record.getValue().toByteArray();
      kafkaRecords
          .get(tp)
          .add(
              new ConsumerRecord<>(
                  tp.topic(),
                  tp.partition(),
                  kafkaRecords.get(tp).size(),
                  record.getTimeStamp(),
                  timestampType,
                  0,
                  key.length,
                  value.length,
                  key,
                  value));
    }

    // This is updated when reader assigns partitions.
    final AtomicReference<List<TopicPartition>> assignedPartitions =
        new AtomicReference<>(Collections.emptyList());
    final MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<byte[], byte[]>(offsetResetStrategy) {
          @Override
          public synchronized void assign(final Collection<TopicPartition> assigned) {
            Collection<TopicPartition> realPartitions =
                assigned.stream()
                    .map(part -> partitionMap.get(part.topic()).get(part.partition()))
                    .collect(Collectors.toList());
            super.assign(realPartitions);
            assignedPartitions.set(ImmutableList.copyOf(realPartitions));
          }
          // Override offsetsForTimes() in order to look up the offsets by timestamp.
          @Override
          public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
              Map<TopicPartition, Long> timestampsToSearch) {
            return timestampsToSearch.entrySet().stream()
                .map(
                    e -> {
                      // In test scope, timestamp == offset. ????
                      long maxOffset = kafkaRecords.get(e.getKey()).size();
                      long offset = e.getValue();
                      OffsetAndTimestamp value =
                          (offset >= maxOffset) ? null : new OffsetAndTimestamp(offset, offset);
                      return new AbstractMap.SimpleEntry<>(e.getKey(), value);
                    })
                .collect(
                    Collectors.toMap(
                        AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
          }
        };

    for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry :
        kafkaRecords.entrySet()) {
      consumer.updatePartitions(
          entry.getKey().topic(), partitionInfoMap.get(entry.getKey().topic()));
      consumer.updateBeginningOffsets(ImmutableMap.of(entry.getKey(), 0L));
      consumer.updateEndOffsets(ImmutableMap.of(entry.getKey(), (long) entry.getValue().size()));
    }

    Runnable recordEnqueueTask =
        new Runnable() {
          @Override
          public void run() {
            // add all the records with offset >= current partition position.
            int recordsAdded = 0;
            for (TopicPartition tp : assignedPartitions.get()) {
              long curPos = consumer.position(tp);
              for (ConsumerRecord<byte[], byte[]> r : kafkaRecords.get(tp)) {
                if (r.offset() >= curPos) {
                  consumer.addRecord(r);
                  recordsAdded++;
                }
              }
            }
            if (recordsAdded == 0) {
              if (config.get("inject.error.at.eof") != null) {
                consumer.setException(new KafkaException("Injected error in consumer.poll()"));
              }
              // MockConsumer.poll(timeout) does not actually wait even when there aren't any
              // records.
              // Add a small wait here in order to avoid busy looping in the reader.
              Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
            consumer.schedulePollTask(this);
          }
        };

    consumer.schedulePollTask(recordEnqueueTask);

    return consumer;
  }

  @Override
  public PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput() {
    throw new RuntimeException("KafkaTestTable does not implement getPTransformForInput method.");
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>
      getPTransformForOutput() {
    throw new RuntimeException("KafkaTestTable does not implement getPTransformForOutput method.");
  }
}
