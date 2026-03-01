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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for idle partition optimization in {@link KafkaUnboundedReader}. */
@RunWith(JUnit4.class)
public class KafkaUnboundedReaderIdlePartitionTest {

  /**
   * Verifies that idle partitions (partitions with no offset changes) are not committed repeatedly,
   * reducing load on Kafka brokers.
   */
  @Test
  public void testIdlePartitionsNotCommittedRepeatedly() throws Exception {
    int numPartitions = 10;
    int numElements = 50; // Only first 5 partitions will have data (numElements / numPartitions)

    List<String> topics = ImmutableList.of("test_topic");

    // Create a mock consumer factory that tracks commit calls
    TrackingMockConsumerFactory consumerFactory =
        new TrackingMockConsumerFactory(topics, numPartitions, numElements);

    // Create a Kafka source with commit offsets enabled
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("test")
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
    for (int i = 0; i < 10; i++) {
      assertTrue(
          "Reader should have more elements", reader.advance() || (i == 0 && reader.start()));
    }

    // Get first checkpoint and finalize it
    KafkaCheckpointMark mark1 = (KafkaCheckpointMark) reader.getCheckpointMark();
    mark1.finalizeCheckpoint();

    // Allow commit to happen (it's async)
    Thread.sleep(2000);

    int initialCommitCount = consumerFactory.mainConsumer.commitCount;

    assertTrue("Should have committed at least once", initialCommitCount > 0);

    // Create another checkpoint without reading more data (all partitions idle)
    KafkaCheckpointMark mark2 = (KafkaCheckpointMark) reader.getCheckpointMark();
    mark2.finalizeCheckpoint();

    // Allow commit attempt to happen
    Thread.sleep(2000);

    int secondCommitCount = consumerFactory.mainConsumer.commitCount;

    // Verify that no new commits happened for idle partitions
    // The commit count should be the same since all partitions are idle
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

    int thirdCommitCount = consumerFactory.mainConsumer.commitCount;

    // Verify that commits happened for partitions with new data
    assertTrue("Active partitions should trigger commits", thirdCommitCount > secondCommitCount);

    reader.close();
  }

  /** Mock consumer factory that creates a tracking consumer for testing. */
  private static class TrackingMockConsumerFactory
      implements org.apache.beam.sdk.transforms.SerializableFunction<
          Map<String, Object>, Consumer<byte[], byte[]>> {

    private final List<String> topics;
    private final int numPartitions;
    private final int numElements;
    final TrackingMockConsumer mainConsumer;

    TrackingMockConsumerFactory(List<String> topics, int numPartitions, int numElements) {
      this.topics = topics;
      this.numPartitions = numPartitions;
      this.numElements = numElements;
      this.mainConsumer = new TrackingMockConsumer(OffsetResetStrategy.EARLIEST);
      initializeConsumer(mainConsumer);
    }

    @Override
    public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
      // Return the same consumer instance to track commits
      return mainConsumer;
    }

    private void initializeConsumer(MockConsumer<byte[], byte[]> consumer) {
      List<TopicPartition> partitions = new ArrayList<>();
      for (String topic : topics) {
        for (int i = 0; i < numPartitions; i++) {
          partitions.add(new TopicPartition(topic, i));
        }
      }

      // Assign partitions
      consumer.assign(partitions);

      // Set beginning offsets
      Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
      for (TopicPartition tp : partitions) {
        beginningOffsets.put(tp, 0L);
      }
      consumer.updateBeginningOffsets(beginningOffsets);

      // Set end offsets (distribute elements across partitions)
      Map<TopicPartition, Long> endOffsets = new HashMap<>();
      int elementsPerPartition = numElements / numPartitions;
      for (TopicPartition tp : partitions) {
        endOffsets.put(tp, (long) elementsPerPartition);
      }
      consumer.updateEndOffsets(endOffsets);

      // Add test records
      for (int i = 0; i < numElements; i++) {
        int partition = i % numPartitions;
        TopicPartition tp = new TopicPartition(topics.get(0), partition);
        consumer.addRecord(
            new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                topics.get(0), partition, i / numPartitions, new byte[0], new byte[0]));
      }
    }
  }

  /** Mock consumer that tracks commit calls. */
  private static class TrackingMockConsumer extends MockConsumer<byte[], byte[]> {
    int commitCount = 0;
    final Map<TopicPartition, Long> committedOffsets = new HashMap<>();

    TrackingMockConsumer(OffsetResetStrategy offsetResetStrategy) {
      super(offsetResetStrategy);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (!offsets.isEmpty()) {
        commitCount++;
        offsets.forEach((tp, metadata) -> committedOffsets.put(tp, metadata.offset()));
      }
      super.commitSync(offsets);
    }

    @Override
    public synchronized void close(long timeout, TimeUnit unit) {
      // Don't actually close for testing
    }
  }
}
