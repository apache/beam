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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Optional;
import java.util.Set;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;

/** Tests for {@link KStreamsPayloadPartitioner}. */
public class KStreamsPayloadPartitionerTest {

  private static final String TOPIC = "repartition";
  private static final byte[] KEY = new byte[] {1, 2, 3};

  private final KStreamsPayloadPartitioner<Integer> partitioner =
      new KStreamsPayloadPartitioner<>(8);

  @Test
  public void aWatermarkGoesToEveryPartition() {
    Optional<Set<Integer>> targets =
        partitioner.partitions(TOPIC, KEY, KStreamsPayload.watermark(5L, "t", 0, 1), 8);
    assertThat(targets, is(Optional.of(ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7))));
  }

  @Test
  public void aFlushGoesToExactlyThePartitionsItNames() {
    Optional<Set<Integer>> targets =
        partitioner.partitions(TOPIC, KEY, KStreamsPayload.flush(ImmutableSet.of(4, 5)), 8);
    assertThat(targets, is(Optional.of(ImmutableSet.of(4, 5))));
  }

  @Test
  public void dataWithTheSameKeyGoesToTheSamePartition() {
    KStreamsPayload<Integer> data = KStreamsPayload.data(WindowedValues.valueInGlobalWindow(1));
    Optional<Set<Integer>> first = partitioner.partitions(TOPIC, KEY, data, 8);
    assertThat(first.get().size(), is(1));
    assertThat(partitioner.partitions(TOPIC, KEY.clone(), data, 8), is(first));
  }

  @Test
  public void keylessDataIsLeftToTheProducer() {
    KStreamsPayload<Integer> data = KStreamsPayload.data(WindowedValues.valueInGlobalWindow(1));
    assertThat(partitioner.partitions(TOPIC, null, data, 8), is(Optional.empty()));
  }

  @Test
  public void aFlushFailsOnATopicWithADifferentPartitionCount() {
    // The targets were chosen for 8 partitions; this topic was left with 4 by an earlier run.
    assertThrows(
        IllegalStateException.class,
        () -> partitioner.partitions(TOPIC, KEY, KStreamsPayload.flush(ImmutableSet.of(0)), 4));
  }
}
