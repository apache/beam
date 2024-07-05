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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link KafkaConsumerPollThread}. Run with 'mvn test -Dkafka.clients.version=0.10.1.1',
 * to test with a specific Kafka version.
 */
@RunWith(JUnit4.class)
public class KafkaConsumerPollThreadTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private final TopicPartition topicPartition = new TopicPartition("topic", 0);

  private MockConsumer<byte[], byte[]> makeConsumer() {
    MockConsumer<byte[], byte[]> mockConsumer =
        new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
          private final AtomicLong offset = new AtomicLong();

          @Override
          public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            offset.set(offsets.get(topicPartition).offset());
          }

          @Override
          public synchronized Map<TopicPartition, OffsetAndMetadata> committed(
              Set<TopicPartition> partitions) {
            return ImmutableMap.of(topicPartition, new OffsetAndMetadata(offset.get()));
          }
        };
    mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
    mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 100L));
    mockConsumer.assign(ImmutableList.of(topicPartition));
    return mockConsumer;
  }

  private ConsumerRecord<byte[], byte[]> makeRecord(int i) {
    return new ConsumerRecord<>(
        "topic",
        0,
        i,
        ("key" + i).getBytes(StandardCharsets.UTF_8),
        ("value" + i).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testBackgroundPollOrdering() throws Exception {
    KafkaConsumerPollThread pollThread = new KafkaConsumerPollThread();
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final MockConsumer<byte[], byte[]> mockConsumer = makeConsumer();
    CountDownLatch done = new CountDownLatch(1);
    Runnable recordEnqueueTask =
        new Runnable() {
          int recordsAdded = 0;

          @Override
          public void run() {
            for (int i = 0; i < 10; ++i, ++recordsAdded) {
              mockConsumer.addRecord(makeRecord(recordsAdded));
            }
            if (recordsAdded < 100) {
              Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
              mockConsumer.schedulePollTask(this);
            } else {
              done.countDown();
            }
          }
        };

    mockConsumer.schedulePollTask(recordEnqueueTask);
    pollThread.startOnExecutor(executorService, mockConsumer);

    for (int readRecords = 0; readRecords < 100; ) {
      for (ConsumerRecord<byte[], byte[]> record : pollThread.readRecords()) {
        assertEquals(readRecords, record.offset());
        assertEquals("topic", record.topic());
        assertEquals(0, record.partition());
        assertEquals("key" + readRecords, new String(record.key(), StandardCharsets.UTF_8));
        assertEquals(
            "value" + readRecords,
            new String(record.value(), StandardCharsets.UTF_8),
            "value" + readRecords);
        ++readRecords;
      }
    }
    done.await();

    pollThread.close();
  }

  @Test
  public void testCloseWithPendingRecords() throws Exception {
    KafkaConsumerPollThread pollThread = new KafkaConsumerPollThread();
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final MockConsumer<byte[], byte[]> mockConsumer = makeConsumer();
    CountDownLatch done = new CountDownLatch(1);
    Runnable recordEnqueueTask =
        new Runnable() {
          int recordsAdded = 0;

          @Override
          public void run() {
            for (int i = 0; i < 10; ++i, ++recordsAdded) {
              mockConsumer.addRecord(makeRecord(i));
            }
            done.countDown();
          }
        };

    mockConsumer.schedulePollTask(recordEnqueueTask);
    pollThread.startOnExecutor(executorService, mockConsumer);
    // Wait for records to be added.
    done.await();
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    // We read nothing but should still be able to close.
    pollThread.close();
  }

  @Test
  public void testBackgroundExceptionSurfacedOnRead() throws Exception {
    KafkaConsumerPollThread pollThread = new KafkaConsumerPollThread();
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final MockConsumer<byte[], byte[]> mockConsumer = makeConsumer();
    mockConsumer.setPollException(new KafkaException("test exception"));
    pollThread.startOnExecutor(executorService, mockConsumer);

    Assert.assertThrows(IOException.class, pollThread::readRecords);
    // We should be able to close after exception
    pollThread.close();
  }

  @Test
  public void finalizeCheckpointMarkAsync() throws Exception {
    KafkaConsumerPollThread pollThread = new KafkaConsumerPollThread();
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final MockConsumer<byte[], byte[]> mockConsumer = makeConsumer();
    assertFalse(
        pollThread.finalizeCheckpointMarksAsync(
            ImmutableList.of(new KafkaCheckpointMark.PartitionMark("topic", 0, 0, 1000))));
    assertTrue(
        pollThread.finalizeCheckpointMarksAsync(
            ImmutableList.of(new KafkaCheckpointMark.PartitionMark("topic", 0, 100, 2000))));
    // Not yet committed since background thread running.
    assertEquals(
        0, mockConsumer.committed(ImmutableSet.of(topicPartition)).get(topicPartition).offset());
    pollThread.startOnExecutor(executorService, mockConsumer);

    while (mockConsumer.committed(ImmutableSet.of(topicPartition)).get(topicPartition).offset()
        != 100) {
      Thread.sleep(100);
    }

    assertFalse(
        pollThread.finalizeCheckpointMarksAsync(
            ImmutableList.of(new KafkaCheckpointMark.PartitionMark("topic", 0, 200, 1000))));
    while (mockConsumer.committed(ImmutableSet.of(topicPartition)).get(topicPartition).offset()
        != 200) {
      Thread.sleep(100);
    }

    pollThread.close();
  }
}
