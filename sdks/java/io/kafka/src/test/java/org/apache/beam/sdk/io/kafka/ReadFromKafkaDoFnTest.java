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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.DistributionCell;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ReadFromKafkaDoFnTest {

  private final TopicPartition topicPartition = new TopicPartition("topic", 0);

  @Rule public ExpectedException thrown = ExpectedException.none();

  private final SimpleMockKafkaConsumer consumer =
      new SimpleMockKafkaConsumer(OffsetResetStrategy.NONE, topicPartition);

  private final ReadFromKafkaDoFn<String, String> dofnInstance =
      ReadFromKafkaDoFn.create(makeReadSourceDescriptor(consumer));

  private final ExceptionMockKafkaConsumer exceptionConsumer =
      new ExceptionMockKafkaConsumer(OffsetResetStrategy.NONE, topicPartition);

  private final ReadFromKafkaDoFn<String, String> exceptionDofnInstance =
      ReadFromKafkaDoFn.create(makeReadSourceDescriptor(exceptionConsumer));

  private ReadSourceDescriptors<String, String> makeReadSourceDescriptor(
      Consumer<byte[], byte[]> kafkaMockConsumer) {
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

  private static class ExceptionMockKafkaConsumer extends MockConsumer<byte[], byte[]> {

    private final TopicPartition topicPartition;

    public ExceptionMockKafkaConsumer(
        OffsetResetStrategy offsetResetStrategy, TopicPartition topicPartition) {
      super(offsetResetStrategy);
      this.topicPartition = topicPartition;
    }

    @Override
    public synchronized long position(TopicPartition partition) {
      throw new KafkaException("PositionException");
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
      throw new KafkaException("SeekException");
    }

    @Override
    public synchronized Map<String, List<PartitionInfo>> listTopics() {
      return ImmutableMap.of(
          topicPartition.topic(),
          ImmutableList.of(
              new PartitionInfo(
                  topicPartition.topic(), topicPartition.partition(), null, null, null)));
    }
  }

  private static class SimpleMockKafkaConsumer extends MockConsumer<byte[], byte[]> {

    private final TopicPartition topicPartition;
    private boolean isRemoved = false;
    private long currentPos = 0L;
    private long startOffset = 0L;
    private KV<Long, Instant> startOffsetForTime = KV.of(0L, Instant.now());
    private KV<Long, Instant> stopOffsetForTime = KV.of(Long.MAX_VALUE, null);
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
      this.startOffsetForTime = KV.of(0L, Instant.now());
      this.stopOffsetForTime = KV.of(Long.MAX_VALUE, null);
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

    public void setStartOffsetForTime(long offset, Instant time) {
      this.startOffsetForTime = KV.of(offset, time);
    }

    public void setStopOffsetForTime(long offset, Instant time) {
      this.stopOffsetForTime = KV.of(offset, time);
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
    public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
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
      return new ConsumerRecords<>(ImmutableMap.of(topicPartition, records));
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
      assertTrue(
          Iterables.getOnlyElement(
                  timestampsToSearch.keySet().stream().collect(Collectors.toList()))
              .equals(this.topicPartition));
      Long timeToSearch = Iterables.getOnlyElement(timestampsToSearch.values());
      Long returnOffset = 0L;
      if (timeToSearch == this.startOffsetForTime.getValue().getMillis()) {
        returnOffset = this.startOffsetForTime.getKey();
      } else if (timeToSearch == this.stopOffsetForTime.getValue().getMillis()) {
        returnOffset = this.stopOffsetForTime.getKey();
      }
      return ImmutableMap.of(topicPartition, new OffsetAndTimestamp(returnOffset, timeToSearch));
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
    exceptionDofnInstance.setup();
    consumer.reset();
  }

  @Test
  public void testInitialRestrictionWhenHasStartOffset() throws Exception {
    long expectedStartOffset = 10L;
    consumer.setStartOffsetForTime(15L, Instant.now());
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(
                topicPartition, expectedStartOffset, null, null, null, ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWhenHasStopOffset() throws Exception {
    long expectedStartOffset = 10L;
    long expectedStopOffset = 20L;
    consumer.setStartOffsetForTime(15L, Instant.now());
    consumer.setStopOffsetForTime(18L, Instant.now());
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(
                topicPartition,
                expectedStartOffset,
                null,
                expectedStopOffset,
                null,
                ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, expectedStopOffset), result);
  }

  @Test
  public void testInitialRestrictionWhenHasStartTime() throws Exception {
    long expectedStartOffset = 10L;
    Instant startReadTime = Instant.now();
    consumer.setStartOffsetForTime(expectedStartOffset, startReadTime);
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(
                topicPartition, null, startReadTime, null, null, ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWhenHasStopTime() throws Exception {
    long expectedStartOffset = 10L;
    Instant startReadTime = Instant.now();
    long expectedStopOffset = 100L;
    Instant stopReadTime = startReadTime.plus(org.joda.time.Duration.millis(2000));
    consumer.setStartOffsetForTime(expectedStartOffset, startReadTime);
    consumer.setStopOffsetForTime(expectedStopOffset, stopReadTime);
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(
                topicPartition, null, startReadTime, null, stopReadTime, ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, expectedStopOffset), result);
  }

  @Test
  public void testInitialRestrictionWithConsumerPosition() throws Exception {
    long expectedStartOffset = 5L;
    consumer.setCurrentPos(5L);
    OffsetRange result =
        dofnInstance.initialRestriction(
            KafkaSourceDescriptor.of(topicPartition, null, null, null, null, ImmutableList.of()));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWithException() throws Exception {
    thrown.expect(KafkaException.class);
    thrown.expectMessage("PositionException");

    exceptionDofnInstance.initialRestriction(
        KafkaSourceDescriptor.of(topicPartition, null, null, null, null, ImmutableList.of()));
  }

  @Test
  public void testProcessElement() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setNumOfRecordsPerPoll(3L);
    long startOffset = 5L;
    OffsetRangeTracker tracker =
        new OffsetRangeTracker(new OffsetRange(startOffset, startOffset + 3));
    KafkaSourceDescriptor descriptor =
        KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null);
    ProcessContinuation result = dofnInstance.processElement(descriptor, tracker, null, receiver);
    assertEquals(ProcessContinuation.stop(), result);
    assertEquals(
        createExpectedRecords(descriptor, startOffset, 3, "key", "value"), receiver.getOutputs());
  }

  @Test
  public void testRawSizeMetric() throws Exception {
    final int numElements = 1000;
    final int recordSize = 8; // The size of key and value is defined in SimpleMockKafkaConsumer.
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);

    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setNumOfRecordsPerPoll(numElements);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0, numElements));
    KafkaSourceDescriptor descriptor =
        KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null);
    ProcessContinuation result = dofnInstance.processElement(descriptor, tracker, null, receiver);
    assertEquals(ProcessContinuation.stop(), result);

    DistributionCell d =
        container.getDistribution(
            MetricName.named(
                ReadFromKafkaDoFn.METRIC_NAMESPACE,
                ReadFromKafkaDoFn.RAW_SIZE_METRIC_PREFIX + topicPartition));

    assertEquals(
        d.getCumulative(),
        DistributionData.create(recordSize * numElements, numElements, recordSize, recordSize));
  }

  @Test
  public void testProcessElementWithEmptyPoll() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    consumer.setNumOfRecordsPerPoll(-1);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    ProcessContinuation result =
        dofnInstance.processElement(
            KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null),
            tracker,
            null,
            receiver);
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
            KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null),
            tracker,
            null,
            receiver);
    assertEquals(ProcessContinuation.stop(), result);
  }

  @Test
  public void testProcessElementWhenTopicPartitionIsStopped() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    ReadFromKafkaDoFn<String, String> instance =
        ReadFromKafkaDoFn.create(
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
    instance.setup();
    consumer.setNumOfRecordsPerPoll(10);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    ProcessContinuation result =
        instance.processElement(
            KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null),
            tracker,
            null,
            receiver);
    tracker.checkDone();
    assertEquals(ProcessContinuation.stop(), result);
  }

  @Test
  public void testProcessElementWithException() throws Exception {
    thrown.expect(KafkaException.class);
    thrown.expectMessage("SeekException");

    MockOutputReceiver receiver = new MockOutputReceiver();
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));

    exceptionDofnInstance.processElement(
        KafkaSourceDescriptor.of(topicPartition, null, null, null, null, null),
        tracker,
        null,
        receiver);
  }

  private static final TypeDescriptor<KafkaSourceDescriptor>
      KAFKA_SOURCE_DESCRIPTOR_TYPE_DESCRIPTOR = new TypeDescriptor<KafkaSourceDescriptor>() {};

  @Test
  public void testBounded() {
    BoundednessVisitor visitor = testBoundedness(rsd -> rsd.withBounded());
    Assert.assertEquals(0, visitor.unboundedPCollections.size());
  }

  @Test
  public void testUnbounded() {
    BoundednessVisitor visitor = testBoundedness(rsd -> rsd);
    Assert.assertNotEquals(0, visitor.unboundedPCollections.size());
  }

  private BoundednessVisitor testBoundedness(
      Function<ReadSourceDescriptors<String, String>, ReadSourceDescriptors<String, String>>
          readSourceDescriptorsDecorator) {
    TestPipeline p = TestPipeline.create();
    p.apply(Create.empty(KAFKA_SOURCE_DESCRIPTOR_TYPE_DESCRIPTOR))
        .apply(
            ParDo.of(
                ReadFromKafkaDoFn.<String, String>create(
                    readSourceDescriptorsDecorator.apply(makeReadSourceDescriptor(consumer)))))
        .setCoder(
            KvCoder.of(
                SerializableCoder.of(KafkaSourceDescriptor.class),
                org.apache.beam.sdk.io.kafka.KafkaRecordCoder.of(
                    StringUtf8Coder.of(), StringUtf8Coder.of())));

    BoundednessVisitor visitor = new BoundednessVisitor();
    p.traverseTopologically(visitor);
    return visitor;
  }

  static class BoundednessVisitor extends PipelineVisitor.Defaults {
    final List<PCollection<?>> unboundedPCollections = new ArrayList<>();

    @Override
    public void visitValue(PValue value, Node producer) {
      if (value instanceof PCollection) {
        PCollection<?> pc = (PCollection<?>) value;
        if (pc.isBounded() == IsBounded.UNBOUNDED) {
          unboundedPCollections.add(pc);
        }
      }
    }
  }
}
