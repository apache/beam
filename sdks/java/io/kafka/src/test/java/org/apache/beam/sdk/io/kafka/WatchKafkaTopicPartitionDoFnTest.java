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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Instant.class})
@SuppressWarnings({"nullness"})
public class WatchKafkaTopicPartitionDoFnTest {

  @Mock Consumer<byte[], byte[]> mockConsumer;
  @Mock Timer timer;

  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFn =
      new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
        @Override
        public Consumer<byte[], byte[]> apply(Map<String, Object> input) {
          return mockConsumer;
        }
      };

  @Test
  public void testGetAllTopicPartitions() throws Exception {
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
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(1L), consumerFn, null, ImmutableMap.of(), null, null);
    assertEquals(
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1)),
        dofnInstance.getAllTopicPartitions());
  }

  @Test
  public void testGetAllTopicPartitionsWithGivenTopics() throws Exception {
    List<String> givenTopics = ImmutableList.of("topic1", "topic2");
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
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(1L), consumerFn, null, ImmutableMap.of(), null, givenTopics);
    verify(mockConsumer, never()).listTopics();
    assertEquals(
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1)),
        dofnInstance.getAllTopicPartitions());
  }

  @Test
  public void testProcessElementWhenNoAvailableTopicPartition() throws Exception {
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L), consumerFn, null, ImmutableMap.of(), null, null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

    when(mockConsumer.listTopics()).thenReturn(ImmutableMap.of());
    MockBagState bagState = new MockBagState(ImmutableList.of());

    when(timer.offset(Duration.millis(600L))).thenReturn(timer);
    dofnInstance.processElement(timer, bagState, outputReceiver);
    verify(timer, times(1)).setRelative();
    assertTrue(outputReceiver.getOutputs().isEmpty());
    assertTrue(bagState.getCurrentStates().isEmpty());
  }

  @Test
  public void testProcessElementWithAvailableTopicPartitions() throws Exception {
    Instant startReadTime = Instant.ofEpochMilli(1L);
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L), consumerFn, null, ImmutableMap.of(), startReadTime, null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

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
    MockBagState bagState = new MockBagState(ImmutableList.of());

    when(timer.offset(Duration.millis(600L))).thenReturn(timer);
    dofnInstance.processElement(timer, bagState, outputReceiver);

    verify(timer, times(1)).setRelative();
    Set<TopicPartition> expectedOutputTopicPartitions =
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));
    Set<KafkaSourceDescriptor> expectedOutputDescriptor =
        generateDescriptorsFromTopicPartitions(expectedOutputTopicPartitions, startReadTime);
    assertEquals(expectedOutputDescriptor, new HashSet<>(outputReceiver.getOutputs()));
    assertEquals(expectedOutputTopicPartitions, bagState.getCurrentStates());
  }

  @Test
  public void testProcessElementWithStoppingReadingTopicPartition() throws Exception {
    Instant startReadTime = Instant.ofEpochMilli(1L);
    SerializableFunction<TopicPartition, Boolean> checkStopReadingFn =
        new SerializableFunction<TopicPartition, Boolean>() {
          @Override
          public Boolean apply(TopicPartition input) {
            if (input.equals(new TopicPartition("topic1", 1))) {
              return true;
            }
            return false;
          }
        };
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L),
            consumerFn,
            checkStopReadingFn,
            ImmutableMap.of(),
            startReadTime,
            null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

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
    MockBagState bagState = new MockBagState(ImmutableList.of());

    when(timer.offset(Duration.millis(600L))).thenReturn(timer);
    dofnInstance.processElement(timer, bagState, outputReceiver);
    verify(timer, times(1)).setRelative();

    Set<TopicPartition> expectedOutputTopicPartitions =
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));
    Set<KafkaSourceDescriptor> expectedOutputDescriptor =
        generateDescriptorsFromTopicPartitions(expectedOutputTopicPartitions, startReadTime);
    assertEquals(expectedOutputDescriptor, new HashSet<>(outputReceiver.getOutputs()));
    assertEquals(expectedOutputTopicPartitions, bagState.getCurrentStates());
  }

  @Test
  public void testOnTimerWithNoAvailableTopicPartition() throws Exception {
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L), consumerFn, null, ImmutableMap.of(), null, null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

    when(mockConsumer.listTopics()).thenReturn(ImmutableMap.of());
    MockBagState bagState = new MockBagState(ImmutableList.of(new TopicPartition("topic1", 0)));
    Instant now = Instant.EPOCH;
    mockStatic(Instant.class);
    when(Instant.now()).thenReturn(now);

    dofnInstance.onTimer(timer, bagState, outputReceiver);

    verify(timer, times(1)).set(now.plus(600L));
    assertTrue(outputReceiver.getOutputs().isEmpty());
    assertTrue(bagState.getCurrentStates().isEmpty());
  }

  @Test
  public void testOnTimerWithAdditionOnly() throws Exception {
    Instant startReadTime = Instant.ofEpochMilli(1L);
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L), consumerFn, null, ImmutableMap.of(), startReadTime, null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

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
    MockBagState bagState =
        new MockBagState(
            ImmutableList.of(new TopicPartition("topic1", 0), new TopicPartition("topic1", 1)));
    Instant now = Instant.EPOCH;
    mockStatic(Instant.class);
    when(Instant.now()).thenReturn(now);

    dofnInstance.onTimer(timer, bagState, outputReceiver);

    verify(timer, times(1)).set(now.plus(600L));
    Set<TopicPartition> expectedOutputTopicPartitions =
        ImmutableSet.of(new TopicPartition("topic2", 0), new TopicPartition("topic2", 1));
    Set<TopicPartition> expectedCurrentTopicPartitions =
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));
    Set<KafkaSourceDescriptor> expectedOutputDescriptor =
        generateDescriptorsFromTopicPartitions(expectedOutputTopicPartitions, startReadTime);
    assertEquals(expectedOutputDescriptor, new HashSet<>(outputReceiver.getOutputs()));
    assertEquals(expectedCurrentTopicPartitions, bagState.getCurrentStates());
  }

  @Test
  public void testOnTimerWithRemovalOnly() throws Exception {
    Instant startReadTime = Instant.ofEpochMilli(1L);
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L), consumerFn, null, ImmutableMap.of(), startReadTime, null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

    when(mockConsumer.listTopics())
        .thenReturn(
            ImmutableMap.of(
                "topic1",
                ImmutableList.of(new PartitionInfo("topic1", 0, null, null, null)),
                "topic2",
                ImmutableList.of(
                    new PartitionInfo("topic2", 0, null, null, null),
                    new PartitionInfo("topic2", 1, null, null, null))));
    MockBagState bagState =
        new MockBagState(
            ImmutableList.of(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)));
    Instant now = Instant.EPOCH;
    mockStatic(Instant.class);
    when(Instant.now()).thenReturn(now);

    dofnInstance.onTimer(timer, bagState, outputReceiver);

    verify(timer, times(1)).set(now.plus(600L));
    Set<TopicPartition> expectedCurrentTopicPartitions =
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));
    assertTrue(outputReceiver.getOutputs().isEmpty());
    assertEquals(expectedCurrentTopicPartitions, bagState.getCurrentStates());
  }

  @Test
  public void testOnTimerWithStoppedTopicPartitions() throws Exception {
    Instant startReadTime = Instant.ofEpochMilli(1L);
    SerializableFunction<TopicPartition, Boolean> checkStopReadingFn =
        new SerializableFunction<TopicPartition, Boolean>() {
          @Override
          public Boolean apply(TopicPartition input) {
            if (input.equals(new TopicPartition("topic1", 1))) {
              return true;
            }
            return false;
          }
        };
    WatchKafkaTopicPartitionDoFn dofnInstance =
        new WatchKafkaTopicPartitionDoFn(
            Duration.millis(600L),
            consumerFn,
            checkStopReadingFn,
            ImmutableMap.of(),
            startReadTime,
            null);
    MockOutputReceiver outputReceiver = new MockOutputReceiver();

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
    MockBagState bagState =
        new MockBagState(
            ImmutableList.of(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)));
    Instant now = Instant.EPOCH;
    mockStatic(Instant.class);
    when(Instant.now()).thenReturn(now);

    dofnInstance.onTimer(timer, bagState, outputReceiver);

    Set<TopicPartition> expectedCurrentTopicPartitions =
        ImmutableSet.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));

    verify(timer, times(1)).set(now.plus(600L));
    assertTrue(outputReceiver.getOutputs().isEmpty());
    assertEquals(expectedCurrentTopicPartitions, bagState.getCurrentStates());
  }

  private static class MockOutputReceiver implements OutputReceiver<KafkaSourceDescriptor> {

    private List<KafkaSourceDescriptor> outputs = new ArrayList<>();

    @Override
    public void output(KafkaSourceDescriptor output) {
      outputs.add(output);
    }

    @Override
    public void outputWithTimestamp(KafkaSourceDescriptor output, Instant timestamp) {}

    public List<KafkaSourceDescriptor> getOutputs() {
      return outputs;
    }
  }

  private static class MockBagState implements BagState<TopicPartition> {
    private Set<TopicPartition> topicPartitions = new HashSet<>();

    MockBagState(List<TopicPartition> readReturn) {
      topicPartitions.addAll(readReturn);
    }

    @Override
    public Iterable<TopicPartition> read() {
      return topicPartitions;
    }

    @Override
    public void add(TopicPartition value) {
      topicPartitions.add(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return null;
    }

    @Override
    public BagState<TopicPartition> readLater() {
      return null;
    }

    @Override
    public void clear() {
      topicPartitions.clear();
    }

    public Set<TopicPartition> getCurrentStates() {
      return topicPartitions;
    }
  }

  private Set<KafkaSourceDescriptor> generateDescriptorsFromTopicPartitions(
      Set<TopicPartition> topicPartitions, Instant startReadTime) {
    return topicPartitions.stream()
        .map(topicPartition -> KafkaSourceDescriptor.of(topicPartition, null, startReadTime, null))
        .collect(Collectors.toSet());
  }
}
