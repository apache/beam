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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaCommitOffset.CommitOffsetDoFn;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link KafkaCommitOffset}. */
@RunWith(JUnit4.class)
public class KafkaCommitOffsetTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(CommitOffsetDoFn.class);

  private final KafkaCommitOffsetMockConsumer consumer =
      new KafkaCommitOffsetMockConsumer(null, false);
  private final KafkaCommitOffsetMockConsumer errorConsumer =
      new KafkaCommitOffsetMockConsumer(null, true);

  private static final KafkaCommitOffsetMockConsumer COMPOSITE_CONSUMER =
      new KafkaCommitOffsetMockConsumer(null, false);
  private static final KafkaCommitOffsetMockConsumer COMPOSITE_CONSUMER_BOOTSTRAP =
      new KafkaCommitOffsetMockConsumer(null, false);

  private static final Map<String, Object> configMap =
      ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "group1");

  @Test
  public void testCommitOffsetDoFn() {
    ReadSourceDescriptors<Object, Object> descriptors =
        ReadSourceDescriptors.read()
            .withBootstrapServers("bootstrap_server")
            .withConsumerConfigUpdates(configMap)
            .withConsumerFactoryFn(
                (SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>)
                    input -> {
                      Assert.assertEquals("group1", input.get(ConsumerConfig.GROUP_ID_CONFIG));
                      return consumer;
                    });
    CommitOffsetDoFn doFn = new CommitOffsetDoFn(descriptors);

    final TopicPartition topicPartition1 = new TopicPartition("topic", 0);
    final TopicPartition topicPartition2 = new TopicPartition("other_topic", 1);
    doFn.processElement(
        KV.of(KafkaSourceDescriptor.of(topicPartition1, null, null, null, null, null), 2L));
    doFn.processElement(
        KV.of(KafkaSourceDescriptor.of(topicPartition2, null, null, null, null, null), 200L));

    Assert.assertEquals(3L, (long) consumer.commitOffsets.get(topicPartition1));
    Assert.assertEquals(201L, (long) consumer.commitOffsets.get(topicPartition2));

    doFn.processElement(
        KV.of(KafkaSourceDescriptor.of(topicPartition1, null, null, null, null, null), 3L));
    Assert.assertEquals(4L, (long) consumer.commitOffsets.get(topicPartition1));
  }

  KafkaRecord<String, String> makeTestRecord(int i) {
    return new KafkaRecord<>(
        "", 0, i, 0, KafkaTimestampType.NO_TIMESTAMP_TYPE, null, KV.of("key" + i, "value" + i));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKafkaOffsetComposite() throws CannotProvideCoderException {
    testKafkaOffsetHelper(false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKafkaOffsetCompositeLegacy() throws CannotProvideCoderException {
    testKafkaOffsetHelper(true);
  }

  private void testKafkaOffsetHelper(boolean use259Implementation)
      throws CannotProvideCoderException {
    COMPOSITE_CONSUMER.commitOffsets.clear();
    COMPOSITE_CONSUMER_BOOTSTRAP.commitOffsets.clear();

    ReadSourceDescriptors<String, String> descriptors =
        ReadSourceDescriptors.<String, String>read()
            .withBootstrapServers("bootstrap_server")
            .withConsumerConfigUpdates(configMap)
            .withConsumerFactoryFn(
                (SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>)
                    input -> {
                      Assert.assertEquals("group1", input.get(ConsumerConfig.GROUP_ID_CONFIG));
                      if (input
                          .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
                          .equals("bootstrap_server")) {
                        return COMPOSITE_CONSUMER;
                      }
                      Assert.assertEquals(
                          "bootstrap_overridden",
                          input.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
                      return COMPOSITE_CONSUMER_BOOTSTRAP;
                    });

    String topic0 = "topic0_" + (use259Implementation ? "259" : "260");
    String topic1 = "topic1_" + (use259Implementation ? "259" : "260");
    KafkaSourceDescriptor d1 =
        KafkaSourceDescriptor.of(new TopicPartition(topic0, 0), null, null, null, null, null);
    KafkaSourceDescriptor d2 =
        KafkaSourceDescriptor.of(new TopicPartition(topic0, 1), null, null, null, null, null);
    KafkaSourceDescriptor d3 =
        KafkaSourceDescriptor.of(
            new TopicPartition(topic1, 0),
            null,
            null,
            null,
            null,
            ImmutableList.of("bootstrap_overridden"));
    KafkaSourceDescriptor d4 =
        KafkaSourceDescriptor.of(
            new TopicPartition(topic1, 1),
            null,
            null,
            null,
            null,
            ImmutableList.of("bootstrap_overridden"));
    List<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> elements = new ArrayList<>();
    elements.add(KV.of(d1, makeTestRecord(10)));

    elements.add(KV.of(d2, makeTestRecord(20)));
    elements.add(KV.of(d3, makeTestRecord(30)));
    elements.add(KV.of(d4, makeTestRecord(40)));
    elements.add(KV.of(d2, makeTestRecord(10)));
    elements.add(KV.of(d1, makeTestRecord(100)));
    PCollection<KV<KafkaSourceDescriptor, KafkaRecord<String, String>>> input =
        pipeline.apply(
            Create.of(elements)
                .withCoder(
                    KvCoder.of(
                        pipeline.getCoderRegistry().getCoder(KafkaSourceDescriptor.class),
                        KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
    input.apply(new KafkaCommitOffset<>(descriptors, use259Implementation));
    pipeline.run();

    HashMap<TopicPartition, Long> expectedOffsets = new HashMap<>();
    expectedOffsets.put(d1.getTopicPartition(), 101L);
    expectedOffsets.put(d2.getTopicPartition(), 21L);
    Assert.assertEquals(expectedOffsets, COMPOSITE_CONSUMER.commitOffsets);
    expectedOffsets.clear();
    expectedOffsets.put(d3.getTopicPartition(), 31L);
    expectedOffsets.put(d4.getTopicPartition(), 41L);
    Assert.assertEquals(expectedOffsets, COMPOSITE_CONSUMER_BOOTSTRAP.commitOffsets);
  }

  @Test
  public void testCommitOffsetError() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

    ReadSourceDescriptors<Object, Object> descriptors =
        ReadSourceDescriptors.read()
            .withBootstrapServers("bootstrap_server")
            .withConsumerConfigUpdates(configMap)
            .withConsumerFactoryFn(
                (SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>)
                    input -> {
                      Assert.assertEquals("group1", input.get(ConsumerConfig.GROUP_ID_CONFIG));
                      return errorConsumer;
                    });
    CommitOffsetDoFn doFn = new CommitOffsetDoFn(descriptors);

    final TopicPartition partition = new TopicPartition("topic", 0);
    doFn.processElement(
        KV.of(KafkaSourceDescriptor.of(partition, null, null, null, null, null), 1L));

    expectedLogs.verifyWarn("Getting exception when committing offset: Test Exception");
    Assert.assertTrue(errorConsumer.commitOffsets.isEmpty());
  }

  private static class KafkaCommitOffsetMockConsumer extends MockConsumer<byte[], byte[]> {

    public final HashMap<TopicPartition, Long> commitOffsets = new HashMap<>();
    private final boolean throwException;

    public KafkaCommitOffsetMockConsumer(
        OffsetResetStrategy offsetResetStrategy, boolean throwException) {
      super(offsetResetStrategy);
      this.throwException = throwException;
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (throwException) {
        throw new RuntimeException("Test Exception");
      } else {
        commitAsync(offsets, null);
        offsets.forEach(
            (topic, offsetMetadata) -> commitOffsets.put(topic, offsetMetadata.offset()));
      }
    }

    @Override
    public synchronized void close(long timeout, TimeUnit unit) {
      // Ignore closing since we're using a single consumer.
    }
  }
}
