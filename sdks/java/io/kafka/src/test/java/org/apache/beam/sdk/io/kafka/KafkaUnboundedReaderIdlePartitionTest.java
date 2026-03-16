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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for idle partition optimization in {@link KafkaUnboundedReader}. */
@RunWith(JUnit4.class)
public class KafkaUnboundedReaderIdlePartitionTest {

  private static final Instant LOG_APPEND_START_TIME = new Instant(100000);

  /**
   * Verifies that idle partitions (partitions with no offset changes) are not committed repeatedly,
   * reducing load on Kafka brokers.
   */
  @Test
  public void testIdlePartitionsNotCommittedRepeatedly() throws Exception {
    int numElements = 50;
    int numPartitions = 10;
    List<String> topics = ImmutableList.of("test_topic");

    // Create a tracking consumer factory
    TrackingConsumerFactory consumerFactory =
        new TrackingConsumerFactory(topics, numPartitions, numElements);

    // Create a Kafka source with commit offsets enabled
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("test_server")
            .withTopics(topics)
            .withConsumerFactoryFn(consumerFactory)
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withConsumerConfigUpdates(
                ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id"))
            .commitOffsetsInFinalize()
            .makeSource()
            .split(1, PipelineOptionsFactory.create())
            .get(0);

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);

    // Read some elements
    assertTrue("Reader should start", reader.start());
    for (int i = 0; i < 10; i++) {
      assertTrue("Reader should have more elements", reader.advance());
    }

    // Get first checkpoint and finalize it
    KafkaCheckpointMark mark1 = (KafkaCheckpointMark) reader.getCheckpointMark();
    mark1.finalizeCheckpoint();

    // Allow commit to happen (it's async)
    Thread.sleep(2000);

    int initialCommitCount = consumerFactory.commitCounter.get();
    assertTrue("Should have committed at least once", initialCommitCount > 0);

    // Create another checkpoint without reading more data (all partitions idle)
    KafkaCheckpointMark mark2 = (KafkaCheckpointMark) reader.getCheckpointMark();
    mark2.finalizeCheckpoint();

    // Allow commit attempt to happen
    Thread.sleep(2000);

    int secondCommitCount = consumerFactory.commitCounter.get();

    // Verify that no new commits happened for idle partitions
    assertEquals(
        "Idle partitions should not trigger additional commits",
        initialCommitCount,
        secondCommitCount);

    // Read more elements to activate partitions again
    for (int i = 0; i < 10; i++) {
      reader.advance();
    }

    // Create another checkpoint after reading more data
    KafkaCheckpointMark mark3 = (KafkaCheckpointMark) reader.getCheckpointMark();
    mark3.finalizeCheckpoint();

    // Allow commit to happen
    Thread.sleep(2000);

    int thirdCommitCount = consumerFactory.commitCounter.get();

    // Verify that commits happened for partitions with new data
    assertTrue("Active partitions should trigger commits", thirdCommitCount > secondCommitCount);

    reader.close();
  }

  /** Consumer factory that creates a mock consumer with commit tracking. */
  private static class TrackingConsumerFactory
      implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {

    private final List<String> topics;
    private final int partitionsPerTopic;
    private final int numElements;
    final AtomicInteger commitCounter = new AtomicInteger(0);

    TrackingConsumerFactory(List<String> topics, int partitionsPerTopic, int numElements) {
      this.topics = topics;
      this.partitionsPerTopic = partitionsPerTopic;
      this.numElements = numElements;
    }

    @Override
    public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
      return createMockConsumer(
          topics,
          partitionsPerTopic,
          numElements,
          config,
          i -> ByteBuffer.wrap(new byte[4]).putInt(i).array(),
          i -> ByteBuffer.wrap(new byte[8]).putLong((long) i).array());
    }

    private MockConsumer<byte[], byte[]> createMockConsumer(
        List<String> topics,
        int partitionsPerTopic,
        int numElements,
        Map<String, Object> config,
        SerializableFunction<Integer, byte[]> keyFunction,
        SerializableFunction<Integer, byte[]> valueFunction) {

      final List<TopicPartition> partitions = new ArrayList<>();
      final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
      Map<String, List<PartitionInfo>> partitionMap = new HashMap<>();

      for (String topic : topics) {
        List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
        for (int i = 0; i < partitionsPerTopic; i++) {
          TopicPartition tp = new TopicPartition(topic, i);
          partitions.add(tp);
          partIds.add(new PartitionInfo(topic, i, null, null, null));
          records.put(tp, new ArrayList<>());
        }
        partitionMap.put(topic, partIds);
      }

      int numPartitions = partitions.size();
      final long[] offsets = new long[numPartitions];

      for (int i = 0; i < numElements; i++) {
        int pIdx = i % numPartitions;
        TopicPartition tp = partitions.get(pIdx);

        byte[] key = keyFunction.apply(i);
        byte[] value = valueFunction.apply(i);

        records
            .get(tp)
            .add(
                new ConsumerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    offsets[pIdx]++,
                    LOG_APPEND_START_TIME.getMillis() + Duration.standardSeconds(i).getMillis(),
                    TimestampType.LOG_APPEND_TIME,
                    0,
                    key.length,
                    value.length,
                    key,
                    value));
      }

      final AtomicReference<List<TopicPartition>> assignedPartitions =
          new AtomicReference<>(Collections.emptyList());

      final MockConsumer<byte[], byte[]> consumer =
          new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void assign(final Collection<TopicPartition> assigned) {
              super.assign(assigned);
              assignedPartitions.set(ImmutableList.copyOf(assigned));
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
              if (!offsets.isEmpty()) {
                commitCounter.incrementAndGet();
              }
              super.commitSync(offsets);
            }
          };

      partitionMap.forEach(consumer::updatePartitions);
      consumer.updateBeginningOffsets(
          records.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> 0L)));
      consumer.updateEndOffsets(
          records.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> (long) e.getValue().size())));

      Runnable recordEnqueueTask =
          new Runnable() {
            @Override
            public void run() {
              int recordsAdded = 0;
              for (TopicPartition tp : assignedPartitions.get()) {
                long curPos = consumer.position(tp);
                for (ConsumerRecord<byte[], byte[]> r : records.get(tp)) {
                  if (r.offset() >= curPos) {
                    consumer.addRecord(r);
                    recordsAdded++;
                  }
                }
              }
              if (recordsAdded == 0) {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
              }
              consumer.schedulePollTask(this);
            }
          };

      consumer.schedulePollTask(recordEnqueueTask);
      return consumer;
    }
  }
}
