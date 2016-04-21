/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Tests of {@link KafkaSource}.
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

  // Update mock consumer with records distributed among the given topics, each with given number
  // of partitions. Records are assigned in round-robin order among the partitions.
  private static MockConsumer<byte[], byte[]> mkMockConsumer(
      List<String> topics, int partitionsPerTopic, int numElements) {

    final List<TopicPartition> partitions = new ArrayList<>();
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    Map<String, List<PartitionInfo>> partitionMap = new HashMap<>();

    for (String topic : topics) {
      List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
      for (int i = 0; i < partitionsPerTopic; i++) {
        partitions.add(new TopicPartition(topic, i));
        partIds.add(new PartitionInfo(topic, i, null, null, null));
      }
      partitionMap.put(topic, partIds);
    }

    int numPartitions = partitions.size();
    long[] offsets = new long[numPartitions];

    for (int i = 0; i < numElements; i++) {
      int pIdx = i % numPartitions;
      TopicPartition tp = partitions.get(pIdx);

      if (!records.containsKey(tp)) {
        records.put(tp, new ArrayList<ConsumerRecord<byte[], byte[]>>());
      }
      records.get(tp).add(
          // Note: this interface has changed in 0.10. may get fixed before the release.
          new ConsumerRecord<byte[], byte[]>(
              tp.topic(),
              tp.partition(),
              offsets[pIdx]++,
              null, // key
              ByteBuffer.wrap(new byte[8]).putLong(i).array())); // value is 8 byte record id.
    }

    MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
          // override assign() to add records that belong to the assigned partitions.
          public void assign(List<TopicPartition> assigned) {
            super.assign(assigned);
            for (TopicPartition tp : assigned) {
              for (ConsumerRecord<byte[], byte[]> r : records.get(tp)) {
                addRecord(r);
              }
              updateBeginningOffsets(ImmutableMap.of(tp, 0L));
              updateEndOffsets(ImmutableMap.of(tp, (long)records.get(tp).size()));
              seek(tp, 0);
            }
          }
        };

    for (String topic : topics) {
      consumer.updatePartitions(topic, partitionMap.get(topic));
    }

    return consumer;
  }

  private static class ConsumerFactoryFn
                implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
    private final List<String> topics;
    private final int partitionsPerTopic;
    private final int numElements;

    public ConsumerFactoryFn(List<String> topics, int partitionsPerTopic, int numElements) {
      this.topics = topics;
      this.partitionsPerTopic = partitionsPerTopic;
      this.numElements = numElements;
    }

    public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
      return mkMockConsumer(topics, partitionsPerTopic, numElements);
    }
  }

  /**
   * Creates a consumer with two topics, with 5 partitions each.
   * numElements are (round-robin) assigned all the 10 partitions.
   */
  private static KafkaIO.TypedRead<byte[], Long> mkKafkaReadTransform(
      int numElements,
      @Nullable SerializableFunction<KV<byte[], Long>, Instant> timestampFn) {

    List<String> topics = ImmutableList.of("topic_a", "topic_b");

    KafkaIO.Read<byte[], Long> reader = KafkaIO.read()
        .withBootstrapServers("none")
        .withTopics(topics)
        .withConsumerFactoryFn(new ConsumerFactoryFn(topics, 10, numElements)) // 20 partitions
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
    DataflowAssert
      .thatSingleton(input.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    DataflowAssert
      .thatSingleton(input.apply(RemoveDuplicates.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    DataflowAssert
      .thatSingleton(input.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    DataflowAssert
      .thatSingleton(input.apply("Max", Max.<Long>globally()))
      .isEqualTo(numElements - 1);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSource() {
    Pipeline p = TestPipeline.create();
    int numElements = 1000;

    PCollection<Long> input = p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withoutMetadata())
        .apply(Values.<Long>create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSourceWithExplicitPartitions() {
    Pipeline p = TestPipeline.create();
    int numElements = 1000;

    List<String> topics = ImmutableList.of("test");

    KafkaIO.TypedRead<byte[], Long> reader = KafkaIO.read()
        .withBootstrapServers("none")
        .withTopicPartitions(ImmutableList.of(new TopicPartition("test", 5)))
        .withConsumerFactoryFn(new ConsumerFactoryFn(topics, 10, numElements)) // 10 partitions
        .withValueCoder(BigEndianLongCoder.of())
        .withMaxNumRecords(numElements / 10);

    PCollection<Long> input = p
        .apply(reader.withoutMetadata())
        .apply(Values.<Long>create());

    // assert that every element is a multiple of 5.
    DataflowAssert
      .that(input)
      .satisfies(new AssertMultipleOf(5));

    DataflowAssert
      .thatSingleton(input.apply(Count.<Long>globally()))
      .isEqualTo(numElements / 10L);

    p.run();
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSourceTimestamps() {
    Pipeline p = TestPipeline.create();
    int numElements = 1000;

    PCollection<Long> input = p
        .apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
        .apply(Values.<Long>create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs = input
        .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
        .apply("RemoveDuplicateTimestamps", RemoveDuplicates.<Long>create());
    // This assert also confirms that diffs only has one unique value.
    DataflowAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  private static class RemoveKafkaMetadata<K, V> extends DoFn<KafkaRecord<K, V>, KV<K, V>> {
    @Override
    public void processElement(ProcessContext ctx) throws Exception {
      ctx.output(ctx.element().getKV());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSourceSplits() throws Exception {
    Pipeline p = TestPipeline.create();
    int numElements = 1000;
    int numSplits = 10;

    UnboundedSource<KafkaRecord<byte[], Long>, ?> initial =
        mkKafkaReadTransform(numElements, null).makeSource();
    List<? extends UnboundedSource<KafkaRecord<byte[], Long>, ?>> splits =
        initial.generateInitialSplits(numSplits, p.getOptions());
    assertEquals("Expected exact splitting", numSplits, splits.size());

    long elementsPerSplit = numElements / numSplits;
    assertEquals("Expected even splits", numElements, elementsPerSplit * numSplits);
    PCollectionList<Long> pcollections = PCollectionList.empty(p);
    for (int i = 0; i < splits.size(); ++i) {
      pcollections = pcollections.and(
          p.apply("split" + i, Read.from(splits.get(i)).withMaxNumRecords(elementsPerSplit))
           .apply("Remove Metadata " + i, ParDo.of(new RemoveKafkaMetadata<byte[], Long>()))
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
                       implements SerializableFunction<KV<byte[], Long>, Instant> {
    @Override
    public Instant apply(KV<byte[], Long> input) {
      return new Instant(input.getValue());
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMark() throws Exception {
    int numElements = 85; // 85 to make sure some partitions have more records than other.

    // create a single split:
    UnboundedSource<KafkaRecord<byte[], Long>, KafkaCheckpointMark> source =
        mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
          .makeSource()
          .generateInitialSplits(1, PipelineOptionsFactory.fromArgs(new String[0]).create())
          .get(0);

    UnboundedReader<KafkaRecord<byte[], Long>> reader = source.createReader(null, null);
    final int numToSkip = 3;
    // advance once:
    assertTrue(reader.start());

    // Advance the source numToSkip-1 elements and manually save state.
    for (long l = 0; l < numToSkip - 1; ++l) {
      assertTrue(reader.advance());
    }

    // Confirm that we get the expected element in sequence before checkpointing.

    assertEquals(numToSkip - 1, (long) reader.getCurrent().getKV().getValue());
    assertEquals(numToSkip - 1, reader.getCurrentTimestamp().getMillis());

    // Checkpoint and restart, and confirm that the source continues correctly.
    KafkaCheckpointMark mark = CoderUtils.clone(
        source.getCheckpointMarkCoder(), (KafkaCheckpointMark) reader.getCheckpointMark());
    reader = source.createReader(null, mark);
    assertTrue(reader.start());

    // Confirm that we get the next elements in sequence.
    // This also confirms that Reader interleaves records from each partitions by the reader.
    for (int i = numToSkip; i < numElements; i++) {
      assertEquals(i, (long) reader.getCurrent().getKV().getValue());
      assertEquals(i, reader.getCurrentTimestamp().getMillis());
      reader.advance();
    }
  }
}
