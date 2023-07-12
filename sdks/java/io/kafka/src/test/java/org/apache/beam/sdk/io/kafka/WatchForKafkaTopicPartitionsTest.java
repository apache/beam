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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.kafka.KafkaMocks.PartitionGrowthMockConsumer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class WatchForKafkaTopicPartitionsTest {

  public static final TestPipelineOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

  static {
    OPTIONS.setBlockOnRun(false);
  }

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(OPTIONS);

  @Test
  public void testGetAllTopicPartitions() throws Exception {
    Consumer<byte[], byte[]> mockConsumer = Mockito.mock(Consumer.class);
    when(mockConsumer.listTopics())
        .thenReturn(
            ImmutableMap.of(
                "topic1",
                ImmutableList.of(
                    new PartitionInfo("topic1", 0, null, null, null),
                    new PartitionInfo("topic1", 1, null, null, null)),
                "topic2",
                ImmutableList.of(
                    new PartitionInfo("topic2", 0, null, null, null),
                    new PartitionInfo("topic2", 1, null, null, null))));
    assertEquals(
        ImmutableList.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1)),
        WatchForKafkaTopicPartitions.getAllTopicPartitions(
            (input) -> mockConsumer, null, null, null));
  }

  @Test
  public void testGetAllTopicPartitionsWithGivenTopics() throws Exception {
    Set<String> givenTopics = ImmutableSet.of("topic1", "topic2");

    Consumer<byte[], byte[]> mockConsumer = Mockito.mock(Consumer.class);
    when(mockConsumer.partitionsFor("topic1"))
        .thenReturn(
            ImmutableList.of(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
    when(mockConsumer.partitionsFor("topic2"))
        .thenReturn(
            ImmutableList.of(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));
    verify(mockConsumer, never()).listTopics();
    assertEquals(
        ImmutableList.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1)),
        WatchForKafkaTopicPartitions.getAllTopicPartitions(
            (input) -> mockConsumer, null, givenTopics, null));
  }

  @Test
  public void testGetAllTopicPartitionsWithGivenPattern() throws Exception {
    Consumer<byte[], byte[]> mockConsumer = Mockito.mock(Consumer.class);
    when(mockConsumer.listTopics())
        .thenReturn(
            ImmutableMap.of(
                "topic1",
                ImmutableList.of(
                    new PartitionInfo("topic1", 0, null, null, null),
                    new PartitionInfo("topic1", 1, null, null, null)),
                "topic2",
                ImmutableList.of(
                    new PartitionInfo("topic2", 0, null, null, null),
                    new PartitionInfo("topic2", 1, null, null, null)),
                "topicA",
                ImmutableList.of(
                    new PartitionInfo("topicA", 0, null, null, null),
                    new PartitionInfo("topicA", 1, null, null, null)),
                "topicB",
                ImmutableList.of(
                    new PartitionInfo("topicB", 0, null, null, null),
                    new PartitionInfo("topicB", 1, null, null, null))));
    assertEquals(
        ImmutableList.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1)),
        WatchForKafkaTopicPartitions.getAllTopicPartitions(
            (input) -> mockConsumer, null, null, Pattern.compile("topic[0-9]")));
    assertEquals(
        ImmutableList.of(
            new TopicPartition("topicA", 0),
            new TopicPartition("topicA", 1),
            new TopicPartition("topicB", 0),
            new TopicPartition("topicB", 1)),
        WatchForKafkaTopicPartitions.getAllTopicPartitions(
            (input) -> mockConsumer, null, null, Pattern.compile("topic[A-Z]")));
  }

  @Test
  public void testPartitionSingle() {
    Set<String> givenTopics = ImmutableSet.of("topic1");

    WatchForKafkaTopicPartitions watchForKafkaTopicPartitions =
        new WatchForKafkaTopicPartitions(
            Duration.millis(1L),
            (input) ->
                new PartitionGrowthMockConsumer(
                    ImmutableList.of(ImmutableList.of(KV.of("topic1", 0)))),
            null,
            null,
            givenTopics,
            null,
            null,
            null);

    PCollection<KafkaSourceDescriptor> descriptors = p.apply(watchForKafkaTopicPartitions);

    PAssert.that(descriptors).containsInAnyOrder(new KafkaSourceDescriptionMatcher("topic1", 0));

    p.run().waitUntilFinish(Duration.millis(10));
  }

  @Test
  public void testPartitionGrowth() {
    Set<String> givenTopics = ImmutableSet.of("topic1");

    WatchForKafkaTopicPartitions watchForKafkaTopicPartitions =
        new WatchForKafkaTopicPartitions(
            Duration.millis(1L),
            (input) ->
                new PartitionGrowthMockConsumer(
                    ImmutableList.of(
                        ImmutableList.of(KV.of("topic1", 0)),
                        ImmutableList.of(KV.of("topic1", 0), KV.of("topic1", 1)))),
            null,
            null,
            givenTopics,
            null,
            null,
            null);

    PCollection<KafkaSourceDescriptor> descriptors = p.apply(watchForKafkaTopicPartitions);

    PAssert.that(descriptors)
        .containsInAnyOrder(
            new KafkaSourceDescriptionMatcher("topic1", 0),
            new KafkaSourceDescriptionMatcher("topic1", 1));

    p.run().waitUntilFinish(Duration.millis(10));
  }

  private static class KafkaSourceDescriptionMatcher extends BaseMatcher<KafkaSourceDescriptor>
      implements SerializableMatcher<KafkaSourceDescriptor> {

    private String topic;
    private int partition;

    KafkaSourceDescriptionMatcher(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }

    @Override
    public void describeMismatch(Object actual, Description mismatchDescription) {}

    @Override
    public void describeTo(Description description) {}

    @Override
    public boolean matches(Object o) {
      if (o instanceof KafkaSourceDescriptor) {
        KafkaSourceDescriptor descriptor = (KafkaSourceDescriptor) o;
        return descriptor.getTopic().equals(topic) && descriptor.getPartition() == partition;
      } else {
        return false;
      }
    }
  }
}
