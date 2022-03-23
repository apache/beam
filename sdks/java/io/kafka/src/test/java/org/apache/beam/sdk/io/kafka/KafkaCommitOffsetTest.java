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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaCommitOffset.CommitOffsetDoFn;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link KafkaCommitOffset}. */
@RunWith(JUnit4.class)
public class KafkaCommitOffsetTest {

  private final TopicPartition partition = new TopicPartition("topic", 0);

  private final KafkaCommitOffsetMockConsumer consumer = new KafkaCommitOffsetMockConsumer(null);

  @Test
  public void testCommitOffsetDoFn() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

    ReadSourceDescriptors<Object, Object> descriptors =
        ReadSourceDescriptors.read()
            .withBootstrapServers("bootstrap_server")
            .withConsumerConfigUpdates(configMap)
            .withConsumerFactoryFn(
                new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
                  @Override
                  public Consumer<byte[], byte[]> apply(Map<String, Object> input) {
                    Assert.assertEquals("group1", input.get(ConsumerConfig.GROUP_ID_CONFIG));
                    return consumer;
                  }
                });
    CommitOffsetDoFn doFn = new CommitOffsetDoFn(descriptors);

    doFn.processElement(
        KV.of(KafkaSourceDescriptor.of(partition, null, null, null, null, null), 1L));

    Assert.assertEquals(2L, consumer.commit.get(partition).offset());
  }

  private static class KafkaCommitOffsetMockConsumer extends MockConsumer<byte[], byte[]> {

    public Map<TopicPartition, OffsetAndMetadata> commit;

    public KafkaCommitOffsetMockConsumer(OffsetResetStrategy offsetResetStrategy) {
      super(offsetResetStrategy);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      commitAsync(offsets, null);
      commit = offsets;
    }
  }
}
