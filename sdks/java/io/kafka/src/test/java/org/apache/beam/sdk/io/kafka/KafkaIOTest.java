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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link KafkaIO}.
 * Run with 'mvn test -Dkafka.clients.version=0.10.1.1',
 * or 'mvn test -Dkafka.clients.version=0.9.0.1' for either Kafka client version
 */
@RunWith(JUnit4.class)
public class KafkaIOTest {
  /*
   * The tests below borrow code and structure from CountingSourceTest. In addition verifies
   * the reader interleaves the records from multiple partitions.
   *
   * Other tests to consider :
   *   - test KafkaRecordCoder
   */

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // Update mock consumer with records distributed among the given topics, each with given number
  // of partitions. Records are assigned in round-robin order among the partitions.
  private static MockConsumer<byte[], byte[]> mkMockConsumer(
      List<String> topics, int partitionsPerTopic, int numElements,
      OffsetResetStrategy offsetResetStrategy) {

    final List<TopicPartition> partitions = new ArrayList<>();
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    Map<String, List<PartitionInfo>> partitionMap = new HashMap<>();

    for (String topic : topics) {
      List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
      for (int i = 0; i < partitionsPerTopic; i++) {
        TopicPartition tp = new TopicPartition(topic, i);
        partitions.add(tp);
        partIds.add(new PartitionInfo(topic, i, null, null, null));
        records.put(tp, new ArrayList<ConsumerRecord<byte[], byte[]>>());
      }
      partitionMap.put(topic, partIds);
    }

    int numPartitions = partitions.size();
    long[] offsets = new long[numPartitions];

    for (int i = 0; i < numElements; i++) {
      int pIdx = i % numPartitions;
      TopicPartition tp = partitions.get(pIdx);

      records.get(tp).add(
          new ConsumerRecord<>(
              tp.topic(),
              tp.partition(),
              offsets[pIdx]++,
              ByteBuffer.wrap(new byte[4]).putInt(i).array(),    // key is 4 byte record id
              ByteBuffer.wrap(new byte[8]).putLong(i).array())); // value is 8 byte record id
    }

    // This is updated when reader assigns partitions.
    final AtomicReference<List<TopicPartition>> assignedPartitions =
        new AtomicReference<>(Collections.<TopicPartition>emptyList());

    final MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<byte[], byte[]>(offsetResetStrategy) {
          // override assign() in order to set offset limits & to save assigned partitions.
          //remove keyword '@Override' here, it can work with Kafka client 0.9 and 0.10 as:
          //1. SpEL can find this function, either input is List or Collection;
          //2. List extends Collection, so super.assign() could find either assign(List)
          //  or assign(Collection).
          public void assign(final List<TopicPartition> assigned) {
            super.assign(assigned);
            assignedPartitions.set(ImmutableList.copyOf(assigned));
            for (TopicPartition tp : assigned) {
              updateBeginningOffsets(ImmutableMap.of(tp, 0L));
              updateEndOffsets(ImmutableMap.of(tp, (long) records.get(tp).size()));
            }
          }
        };

    for (String topic : topics) {
      consumer.updatePartitions(topic, partitionMap.get(topic));
    }

    // MockConsumer does not maintain any relationship between partition seek position and the
    // records added. e.g. if we add 10 records to a partition and then seek to end of the
    // partition, MockConsumer is still going to return the 10 records in next poll. It is
    // our responsibility to make sure currently enqueued records sync with partition offsets.
    // The following task will be called inside each invocation to MockConsumer.poll().
    // We enqueue only the records with the offset >= partition's current position.
    Runnable recordEnquerTask = new Runnable() {
      @Override
      public void run() {
        // add all the records with offset >= current partition position.
        for (TopicPartition tp : assignedPartitions.get()) {
          long curPos = consumer.position(tp);
          for (ConsumerRecord<byte[], byte[]> r : records.get(tp)) {
            if (r.offset() >= curPos) {
              consumer.addRecord(r);
            }
          }
        }
        consumer.schedulePollTask(this);
      }
    };

    consumer.schedulePollTask(recordEnquerTask);
    return consumer;
  }

  private static class ConsumerFactoryFn
                implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
    private final List<String> topics;
    private final int partitionsPerTopic;
    private final int numElements;
    private final OffsetResetStrategy offsetResetStrategy;

    public ConsumerFactoryFn(List<String> topics,
                             int partitionsPerTopic,
                             int numElements,
                             OffsetResetStrategy offsetResetStrategy) {
      this.topics = topics;
      this.partitionsPerTopic = partitionsPerTopic;
      this.numElements = numElements;
      this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
      return mkMockConsumer(topics, partitionsPerTopic, numElements, offsetResetStrategy);
    }
  }

  /**
   * Creates a consumer with two topics, with 5 partitions each.
   * numElements are (round-robin) assigned all the 10 partitions.
   */
  private static KafkaIO.Read<Integer, Long> mkKafkaReadTransform(
      int numElements,
      @Nullable SerializableFunction<KV<Integer, Long>, Instant> timestampFn) {

    List<String> topics = ImmutableList.of("topic_a", "topic_b");

    KafkaIO.Read<Integer, Long> reader = KafkaIO.<Integer, Long>read()
        .withBootstrapServers("myServer1:9092,myServer2:9092")
        .withTopics(topics)
        .withConsumerFactoryFn(new ConsumerFactoryFn(
            topics, 10, numElements, OffsetResetStrategy.EARLIEST)) // 20 partitions
        .withKeyCoder(BigEndianIntegerCoder.of())
        .withValueCoder(BigEndianLongCoder.of())
        .withMaxNumRecords(numElements);

    if (timestampFn != null) {
      return reader.withTimestampFn(timestampFn);
    } else {
      return reader;
    }
  }

  private static class AssertMultipleOf implements SerializableFunction<Iterable<Long>, Void> {
    private final int num;

    public AssertMultipleOf(int num) {
      this.num = num;
    }

    @Override
    public Void apply(Iterable<Long> values) {
      for (Long v : values) {
        assertEquals(0, v % num);
      }
      return null;
    }
  }

  public static void addCountingAsserts(PCollection<Long> input, long numElements) {
    // Count == numElements
    PAssert
      .thatSingleton(input.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    PAssert
      .thatSingleton(input.apply(Distinct.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    PAssert
      .thatSingleton(input.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    PAssert
      .thatSingleton(input.apply("Max", Max.<Long>globally()))
      .isEqualTo(numElements - 1);
  }

  @Test
  public void testUnboundedSource() {
    int numElements = 1000;

    PCollection<Long> input = p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withoutMetadata())
        .apply(Values.<Long>create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testUnboundedSourceWithSingleTopic() {
    // same as testUnboundedSource, but with single topic

    int numElements = 1000;
    String topic = "my_topic";

    KafkaIO.Read<Integer, Long> reader = KafkaIO.<Integer, Long>read()
        .withBootstrapServers("none")
        .withTopic("my_topic")
        .withConsumerFactoryFn(new ConsumerFactoryFn(
            ImmutableList.of(topic), 10, numElements, OffsetResetStrategy.EARLIEST))
        .withKeyCoder(BigEndianIntegerCoder.of())
        .withValueCoder(BigEndianLongCoder.of())
        .withMaxNumRecords(numElements);

    PCollection<Long> input = p
        .apply(reader.withoutMetadata())
        .apply(Values.<Long>create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testUnboundedSourceWithExplicitPartitions() {
    int numElements = 1000;

    List<String> topics = ImmutableList.of("test");

    KafkaIO.Read<byte[], Long> reader = KafkaIO.<byte[], Long>read()
        .withBootstrapServers("none")
        .withTopicPartitions(ImmutableList.of(new TopicPartition("test", 5)))
        .withConsumerFactoryFn(new ConsumerFactoryFn(
            topics, 10, numElements, OffsetResetStrategy.EARLIEST)) // 10 partitions
        .withKeyCoder(ByteArrayCoder.of())
        .withValueCoder(BigEndianLongCoder.of())
        .withMaxNumRecords(numElements / 10);

    PCollection<Long> input = p
        .apply(reader.withoutMetadata())
        .apply(Values.<Long>create());

    // assert that every element is a multiple of 5.
    PAssert
      .that(input)
      .satisfies(new AssertMultipleOf(5));

    PAssert
      .thatSingleton(input.apply(Count.<Long>globally()))
      .isEqualTo(numElements / 10L);

    p.run();
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  @Test
  public void testUnboundedSourceTimestamps() {

    int numElements = 1000;

    PCollection<Long> input = p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
        .apply(Values.<Long>create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs = input
        .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
        .apply("DistinctTimestamps", Distinct.<Long>create());
    // This assert also confirms that diffs only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  private static class RemoveKafkaMetadata<K, V> extends DoFn<KafkaRecord<K, V>, KV<K, V>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
      ctx.output(ctx.element().getKV());
    }
  }

  @Test
  public void testUnboundedSourceSplits() throws Exception {

    int numElements = 1000;
    int numSplits = 10;

    UnboundedSource<KafkaRecord<Integer, Long>, ?> initial =
        mkKafkaReadTransform(numElements, null).makeSource();
    List<? extends UnboundedSource<KafkaRecord<Integer, Long>, ?>> splits =
        initial.split(numSplits, p.getOptions());
    assertEquals("Expected exact splitting", numSplits, splits.size());

    long elementsPerSplit = numElements / numSplits;
    assertEquals("Expected even splits", numElements, elementsPerSplit * numSplits);
    PCollectionList<Long> pcollections = PCollectionList.empty(p);
    for (int i = 0; i < splits.size(); ++i) {
      pcollections = pcollections.and(
          p.apply("split" + i, Read.from(splits.get(i)).withMaxNumRecords(elementsPerSplit))
           .apply("Remove Metadata " + i, ParDo.of(new RemoveKafkaMetadata<Integer, Long>()))
           .apply("collection " + i, Values.<Long>create()));
    }
    PCollection<Long> input = pcollections.apply(Flatten.<Long>pCollections());

    addCountingAsserts(input, numElements);
    p.run();
  }

  /**
   * A timestamp function that uses the given value as the timestamp.
   */
  private static class ValueAsTimestampFn
                       implements SerializableFunction<KV<Integer, Long>, Instant> {
    @Override
    public Instant apply(KV<Integer, Long> input) {
      return new Instant(input.getValue());
    }
  }

  // Kafka records are read in a separate thread inside the reader. As a result advance() might not
  // read any records even from the mock consumer, especially for the first record.
  // This is a helper method to loop until we read a record.
  private static void advanceOnce(UnboundedReader<?> reader, boolean isStarted) throws IOException {
    if (!isStarted && reader.start()) {
      return;
    }
    while (!reader.advance()) {
      // very rarely will there be more than one attempts.
      // In case of a bug we might end up looping forever, and test will fail with a timeout.

      // Avoid hard cpu spinning in case of a test failure.
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMark() throws Exception {
    int numElements = 85; // 85 to make sure some partitions have more records than other.

    // create a single split:
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
          .makeSource()
          .split(1, PipelineOptionsFactory.create())
          .get(0);

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);
    final int numToSkip = 20; // one from each partition.

    // advance numToSkip elements
    for (int i = 0; i < numToSkip; ++i) {
      advanceOnce(reader, i > 0);
    }

    // Confirm that we get the expected element in sequence before checkpointing.
    assertEquals(numToSkip - 1, (long) reader.getCurrent().getKV().getValue());
    assertEquals(numToSkip - 1, reader.getCurrentTimestamp().getMillis());

    // Checkpoint and restart, and confirm that the source continues correctly.
    KafkaCheckpointMark mark = CoderUtils.clone(
        source.getCheckpointMarkCoder(), (KafkaCheckpointMark) reader.getCheckpointMark());
    reader = source.createReader(null, mark);

    // Confirm that we get the next elements in sequence.
    // This also confirms that Reader interleaves records from each partitions by the reader.

    for (int i = numToSkip; i < numElements; i++) {
      advanceOnce(reader, i > numToSkip);
      assertEquals(i, (long) reader.getCurrent().getKV().getValue());
      assertEquals(i, reader.getCurrentTimestamp().getMillis());
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMarkWithEmptyPartitions() throws Exception {
    // Similar to testUnboundedSourceCheckpointMark(), but verifies that source resumes
    // properly from empty partitions, without missing messages added since checkpoint.

    // Initialize consumer with fewer elements than number of partitions so that some are empty.
    int initialNumElements = 5;
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        mkKafkaReadTransform(initialNumElements, new ValueAsTimestampFn())
            .makeSource()
            .split(1, PipelineOptionsFactory.create())
            .get(0);

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);

    for (int l = 0; l < initialNumElements; ++l) {
      advanceOnce(reader, l > 0);
    }

    // Checkpoint and restart, and confirm that the source continues correctly.
    KafkaCheckpointMark mark = CoderUtils.clone(
        source.getCheckpointMarkCoder(), (KafkaCheckpointMark) reader.getCheckpointMark());

    // Create another source with MockConsumer with OffsetResetStrategy.LATEST. This insures that
    // the reader need to explicitly need to seek to first offset for partitions that were empty.

    int numElements = 100; // all the 20 partitions will have elements
    List<String> topics = ImmutableList.of("topic_a", "topic_b");

    source = KafkaIO.<Integer, Long>read()
        .withBootstrapServers("none")
        .withTopics(topics)
        .withConsumerFactoryFn(new ConsumerFactoryFn(
            topics, 10, numElements, OffsetResetStrategy.LATEST))
        .withKeyCoder(BigEndianIntegerCoder.of())
        .withValueCoder(BigEndianLongCoder.of())
        .withMaxNumRecords(numElements)
        .withTimestampFn(new ValueAsTimestampFn())
        .makeSource()
        .split(1, PipelineOptionsFactory.create())
        .get(0);

    reader = source.createReader(null, mark);

    // Verify in any order. As the partitions are unevenly read, the returned records are not in a
    // simple order. Note that testUnboundedSourceCheckpointMark() verifies round-robin oder.

    List<Long> expected = new ArrayList<>();
    List<Long> actual = new ArrayList<>();
    for (long i = initialNumElements; i < numElements; i++) {
      advanceOnce(reader, i > initialNumElements);
      expected.add(i);
      actual.add(reader.getCurrent().getKV().getValue());
    }
    assertThat(actual, IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testSink() throws Exception {
    // Simply read from kafka source and write to kafka sink. Then verify the records
    // are correctly published to mock kafka producer.

    int numElements = 1000;

    synchronized (MOCK_PRODUCER_LOCK) {

      MOCK_PRODUCER.clear();

      ProducerSendCompletionThread completionThread = new ProducerSendCompletionThread().start();

      String topic = "test";

      p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withoutMetadata())
        .apply(KafkaIO.<Integer, Long>write()
            .withBootstrapServers("none")
            .withTopic(topic)
            .withKeyCoder(BigEndianIntegerCoder.of())
            .withValueCoder(BigEndianLongCoder.of())
            .withProducerFactoryFn(new ProducerFactoryFn()));

      p.run();

      completionThread.shutdown();

      verifyProducerRecords(topic, numElements, false);
    }
  }

  @Test
  public void testValuesSink() throws Exception {
    // similar to testSink(), but use values()' interface.

    int numElements = 1000;

    synchronized (MOCK_PRODUCER_LOCK) {

      MOCK_PRODUCER.clear();

      ProducerSendCompletionThread completionThread = new ProducerSendCompletionThread().start();

      String topic = "test";

      p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withoutMetadata())
        .apply(Values.<Long>create()) // there are no keys
        .apply(KafkaIO.<Integer, Long>write()
            .withBootstrapServers("none")
            .withTopic(topic)
            .withValueCoder(BigEndianLongCoder.of())
            .withProducerFactoryFn(new ProducerFactoryFn())
            .values());

      p.run();

      completionThread.shutdown();

      verifyProducerRecords(topic, numElements, true);
    }
  }

  @Test
  public void testSinkWithSendErrors() throws Throwable {
    // similar to testSink(), except that up to 10 of the send calls to producer will fail
    // asynchronously.

    // TODO: Ideally we want the pipeline to run to completion by retrying bundles that fail.
    // We limit the number of errors injected to 10 below. This would reflect a real streaming
    // pipeline. But I am sure how to achieve that. For now expect an exception:

    thrown.expect(InjectedErrorException.class);
    thrown.expectMessage("Injected Error #1");

    int numElements = 1000;

    synchronized (MOCK_PRODUCER_LOCK) {

      MOCK_PRODUCER.clear();

      String topic = "test";

      ProducerSendCompletionThread completionThreadWithErrors =
          new ProducerSendCompletionThread(10, 100).start();

      p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withoutMetadata())
        .apply(KafkaIO.<Integer, Long>write()
            .withBootstrapServers("none")
            .withTopic(topic)
            .withKeyCoder(BigEndianIntegerCoder.of())
            .withValueCoder(BigEndianLongCoder.of())
            .withProducerFactoryFn(new ProducerFactoryFn()));

      try {
        p.run();
      } catch (PipelineExecutionException e) {
        // throwing inner exception helps assert that first exception is thrown from the Sink
        throw e.getCause().getCause();
      } finally {
        completionThreadWithErrors.shutdown();
      }
    }
  }

  @Test
  public void testSourceDisplayData() {
    KafkaIO.Read<Integer, Long> read = mkKafkaReadTransform(10, null);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topics", "topic_a,topic_b"));
    assertThat(displayData, hasDisplayItem("enable.auto.commit", false));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServer1:9092,myServer2:9092"));
    assertThat(displayData, hasDisplayItem("auto.offset.reset", "latest"));
    assertThat(displayData, hasDisplayItem("receive.buffer.bytes", 524288));
  }

  @Test
  public void testSourceWithExplicitPartitionsDisplayData() {
    KafkaIO.Read<byte[], Long> read = KafkaIO.<byte[], Long>read()
        .withBootstrapServers("myServer1:9092,myServer2:9092")
        .withTopicPartitions(ImmutableList.of(new TopicPartition("test", 5),
            new TopicPartition("test", 6)))
        .withConsumerFactoryFn(new ConsumerFactoryFn(
            Lists.newArrayList("test"), 10, 10, OffsetResetStrategy.EARLIEST)) // 10 partitions
        .withKeyCoder(ByteArrayCoder.of())
        .withValueCoder(BigEndianLongCoder.of());

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topicPartitions", "test-5,test-6"));
    assertThat(displayData, hasDisplayItem("enable.auto.commit", false));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServer1:9092,myServer2:9092"));
    assertThat(displayData, hasDisplayItem("auto.offset.reset", "latest"));
    assertThat(displayData, hasDisplayItem("receive.buffer.bytes", 524288));
  }

  @Test
  public void testSinkDisplayData() {
    KafkaIO.Write<Integer, Long> write = KafkaIO.<Integer, Long>write()
        .withBootstrapServers("myServerA:9092,myServerB:9092")
        .withTopic("myTopic")
        .withValueCoder(BigEndianLongCoder.of())
        .withProducerFactoryFn(new ProducerFactoryFn());

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("topic", "myTopic"));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServerA:9092,myServerB:9092"));
    assertThat(displayData, hasDisplayItem("retries", 3));
  }

  private static void verifyProducerRecords(String topic, int numElements, boolean keyIsAbsent) {

    // verify that appropriate messages are written to kafka
    List<ProducerRecord<Integer, Long>> sent = MOCK_PRODUCER.history();

    // sort by values
    Collections.sort(sent, new Comparator<ProducerRecord<Integer, Long>>() {
      @Override
      public int compare(ProducerRecord<Integer, Long> o1, ProducerRecord<Integer, Long> o2) {
        return Long.compare(o1.value(), o2.value());
      }
    });

    for (int i = 0; i < numElements; i++) {
      ProducerRecord<Integer, Long> record = sent.get(i);
      assertEquals(topic, record.topic());
      if (keyIsAbsent) {
        assertNull(record.key());
      } else {
        assertEquals(i, record.key().intValue());
      }
      assertEquals(i, record.value().longValue());
    }
  }

  /**
   * Singleton MockProudcer. Using a singleton here since we need access to the object to fetch
   * the actual records published to the producer. This prohibits running the tests using
   * the producer in parallel, but there are only one or two tests.
   */
  private static final MockProducer<Integer, Long> MOCK_PRODUCER =
    new MockProducer<Integer, Long>(
      false, // disable synchronous completion of send. see ProducerSendCompletionThread below.
      new KafkaIO.CoderBasedKafkaSerializer<Integer>(),
      new KafkaIO.CoderBasedKafkaSerializer<Long>()) {

      // override flush() so that it does not complete all the waiting sends, giving a chance to
      // ProducerCompletionThread to inject errors.

      @Override
      public void flush() {
        while (completeNext()) {
          // there are some uncompleted records. let the completion thread handle them.
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
      }
    };

  // use a separate object serialize tests using MOCK_PRODUCER so that we don't interfere
  // with Kafka MockProducer locking itself.
  private static final Object MOCK_PRODUCER_LOCK = new Object();

  private static class ProducerFactoryFn
    implements SerializableFunction<Map<String, Object>, Producer<Integer, Long>> {

    @SuppressWarnings("unchecked")
    @Override
    public Producer<Integer, Long> apply(Map<String, Object> config) {

      // Make sure the config is correctly set up for serializers.
      Utils.newInstance(
          ((Class<?>) config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
              .asSubclass(Serializer.class)
      ).configure(config, true);

      Utils.newInstance(
          ((Class<?>) config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
              .asSubclass(Serializer.class)
      ).configure(config, false);

      return MOCK_PRODUCER;
    }
  }

  private static class InjectedErrorException extends RuntimeException {
    public InjectedErrorException(String message) {
      super(message);
    }
  }

  /**
   * We start MockProducer with auto-completion disabled. That implies a record is not marked sent
   * until #completeNext() is called on it. This class starts a thread to asynchronously 'complete'
   * the the sends. During completion, we can also make those requests fail. This error injection
   * is used in one of the tests.
   */
  private static class ProducerSendCompletionThread {

    private final int maxErrors;
    private final int errorFrequency;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final ExecutorService injectorThread;
    private int numCompletions = 0;

    ProducerSendCompletionThread() {
      // complete everything successfully
      this(0, 0);
    }

    ProducerSendCompletionThread(final int maxErrors, final int errorFrequency) {
      this.maxErrors = maxErrors;
      this.errorFrequency = errorFrequency;
      injectorThread = Executors.newSingleThreadExecutor();
    }

    ProducerSendCompletionThread start() {
      injectorThread.submit(new Runnable() {
        @Override
        public void run() {
          int errorsInjected = 0;

          while (!done.get()) {
            boolean successful;

            if (errorsInjected < maxErrors && ((numCompletions + 1) % errorFrequency) == 0) {
              successful = MOCK_PRODUCER.errorNext(
                  new InjectedErrorException("Injected Error #" + (errorsInjected + 1)));

              if (successful) {
                errorsInjected++;
              }
            } else {
              successful = MOCK_PRODUCER.completeNext();
            }

            if (successful) {
              numCompletions++;
            } else {
              // wait a bit since there are no unsent records
              try {
                Thread.sleep(1);
              } catch (InterruptedException e) {
              }
            }
          }
        }
      });

      return this;
    }

    void shutdown() {
      done.set(true);
      injectorThread.shutdown();
      try {
        assertTrue(injectorThread.awaitTermination(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }
}
