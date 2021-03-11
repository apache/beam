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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class ReadFromKafkaDoFnTest {

  private final TopicPartition topicPartition = new TopicPartition("topic", 0);

  private final SimpleMockKafkaConsumer consumer =
      new SimpleMockKafkaConsumer(OffsetResetStrategy.NONE, topicPartition);

  private final ReadFromKafkaDoFn<String, String> dofnInstance =
      new ReadFromKafkaDoFn(makeReadSourceDescriptor(consumer));

  private ReadSourceDescriptors<String, String> makeReadSourceDescriptor(
      Consumer kafkaMockConsumer) {
    return ReadSourceDescriptors.<String, String>read()
        .withKeyDeserializer(StringDeserializer.class)
        .withValueDeserializer(StringDeserializer.class)
        .withConsumerFactoryFn(
            new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
              @Override
              public Consumer<byte[], byte[]> apply(Map<String, Object> input) {
                return kafkaMockConsumer;
              }
            })
        .withBootstrapServers("bootstrap_server");
  }

  private static class SimpleMockKafkaConsumer extends MockConsumer<byte[], byte[]> {

    private final TopicPartition topicPartition;
    private boolean isRemoved = false;
    private long currentPos = 0L;
    private long startOffset = 0L;
    private long startOffsetForTime = 0L;
    private long numOfRecordsPerPoll;

    public SimpleMockKafkaConsumer(
        OffsetResetStrategy offsetResetStrategy, TopicPartition topicPartition) {
      super(offsetResetStrategy);
      this.topicPartition = topicPartition;
    }

    public void reset() {
      this.isRemoved = false;
      this.currentPos = 0L;
      this.startOffset = 0L;
      this.startOffsetForTime = 0L;
      this.numOfRecordsPerPoll = 0L;
    }

    public void setRemoved() {
      this.isRemoved = true;
    }

    public void setNumOfRecordsPerPoll(long num) {
      this.numOfRecordsPerPoll = num;
    }

    public void setCurrentPos(long pos) {
      this.currentPos = pos;
    }

    public void setStartOffsetForTime(long pos) {
      this.startOffsetForTime = pos;
    }

    @Override
    public synchronized Map<String, List<PartitionInfo>> listTopics() {
      if (this.isRemoved) {
        return ImmutableMap.of();
      }
      return ImmutableMap.of(
          topicPartition.topic(),
          ImmutableList.of(
              new PartitionInfo(
                  topicPartition.topic(), topicPartition.partition(), null, null, null)));
    }

    @Override
    public synchronized void assign(Collection<TopicPartition> partitions) {
      assertTrue(Iterables.getOnlyElement(partitions).equals(this.topicPartition));
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
      assertTrue(partition.equals(this.topicPartition));
      this.startOffset = offset;
    }

    @Override
    public synchronized ConsumerRecords<byte[], byte[]> poll(long timeout) {
      if (topicPartition == null) {
        return ConsumerRecords.empty();
      }
      String key = "key";
      String value = "value";
      List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
      for (long i = 0; i <= numOfRecordsPerPoll; i++) {
        records.add(
            new ConsumerRecord<byte[], byte[]>(
                topicPartition.topic(),
                topicPartition.partition(),
                startOffset + i,
                key.getBytes(Charsets.UTF_8),
                value.getBytes(Charsets.UTF_8)));
      }
      if (records.isEmpty()) {
        return ConsumerRecords.empty();
      }
      return new ConsumerRecords(ImmutableMap.of(topicPartition, records));
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
      assertTrue(
          Iterables.getOnlyElement(
                  timestampsToSearch.keySet().stream().collect(Collectors.toList()))
              .equals(this.topicPartition));
      return ImmutableMap.of(
          topicPartition,
          new OffsetAndTimestamp(
              this.startOffsetForTime, Iterables.getOnlyElement(timestampsToSearch.values())));
    }

    @Override
    public synchronized long position(TopicPartition partition) {
      assertTrue(partition.equals(this.topicPartition));
      return this.currentPos;
    }
  }

  private static class MockOutputReceiver
      implements OutputReceiver<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> {

    private final List<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> records =
        new ArrayList<>();

    @Override
    public void output(KV<KafkaSourceDescriptor, KafkaRecord<String, String>> output) {}

    @Override
    public void outputWithTimestamp(
        KV<KafkaSourceDescriptor, KafkaRecord<String, String>> output,
        @UnknownKeyFor @NonNull @Initialized Instant timestamp) {
      records.add(output);
    }

    public List<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> getOutputs() {
      return this.records;
    }
  }

  private List<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> createExpectedRecords(
      KafkaSourceDescriptor descriptor,
      long startOffset,
      int numRecords,
      String key,
      String value) {
    List<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(
          KV.of(
              descriptor,
              new KafkaRecord<String, String>(
                  topicPartition.topic(),
                  topicPartition.partition(),
                  startOffset + i,
                  -1L,
                  KafkaTimestampType.NO_TIMESTAMP_TYPE,
                  new RecordHeaders(),
                  KV.of(key, value))));
    }
    return records;
  }

  @Before
  public void setUp() throws Exception {
    dofnInstance.setup();
    consumer.reset();
  }

  @Test
  public void testInitialRestrictionWhenHasStartOffset() throws Exception {
    long expectedStartOffset = 10L;
    consumer.setStartOffsetForTime(15L);
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(
                topicPartition, expectedStartOffset, Instant.now(), ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWhenHasStartTime() throws Exception {
    long expectedStartOffset = 10L;
    consumer.setStartOffsetForTime(expectedStartOffset);
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(topicPartition, null, Instant.now(), ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWithConsumerPosition() throws Exception {
    long expectedStartOffset = 5L;
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(topicPartition, null, null, ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testProcessElement() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setNumOfRecordsPerPoll(3L);
    long startOffset = 5L;
    OffsetRangeTracker tracker =
        new OffsetRangeTracker(new OffsetRange(startOffset, startOffset + 3));
    KafkaSourceDescriptor descriptor = KafkaSourceDescriptor.of(topicPartition, null, null, null);
    ProcessContinuation result =
        dofnInstance.processElement(descriptor, tracker, null, (OutputReceiver) receiver);
    assertEquals(ProcessContinuation.stop(), result);
    assertEquals(
        createExpectedRecords(descriptor, startOffset, 3, "key", "value"), receiver.getOutputs());
  }

  @Test
  public void testProcessElementWithEmptyPoll() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setNumOfRecordsPerPoll(-1);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    ProcessContinuation result =
        dofnInstance.processElement(
            KafkaSourceDescriptor.of(topicPartition, null, null, null),
            tracker,
            null,
            (OutputReceiver) receiver);
    assertEquals(ProcessContinuation.resume(), result);
    assertTrue(receiver.getOutputs().isEmpty());
  }

  @Test
  public void testProcessElementWhenTopicPartitionIsRemoved() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setRemoved();
    consumer.setNumOfRecordsPerPoll(10);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    ProcessContinuation result =
        dofnInstance.processElement(
            KafkaSourceDescriptor.of(topicPartition, null, null, null),
            tracker,
            null,
            (OutputReceiver) receiver);
    assertEquals(ProcessContinuation.stop(), result);
  }

  @Test
  public void testProcessElementWhenTopicPartitionIsStopped() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    ReadFromKafkaDoFn<String, String> instance =
        new ReadFromKafkaDoFn(
            makeReadSourceDescriptor(consumer)
                .toBuilder()
                .setCheckStopReadingFn(
                    new SerializableFunction<TopicPartition, Boolean>() {
                      @Override
                      public Boolean apply(TopicPartition input) {
                        assertTrue(input.equals(topicPartition));
                        return true;
                      }
                    })
                .build());
    consumer.setNumOfRecordsPerPoll(10);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    ProcessContinuation result =
        instance.processElement(
            KafkaSourceDescriptor.of(topicPartition, null, null, null),
            tracker,
            null,
            (OutputReceiver) receiver);
    assertEquals(ProcessContinuation.stop(), result);
  }
}
