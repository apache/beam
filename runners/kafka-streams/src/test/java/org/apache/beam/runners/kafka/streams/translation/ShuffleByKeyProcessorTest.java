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

import java.util.Properties;
import java.util.Set;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Test;

/**
 * Tests how {@link ShuffleByKeyProcessor} restamps watermark reports and addresses flush markers as
 * they are about to cross a repartition topic.
 *
 * <p>Upstream of the shuffle a transform forwards its watermark in process, to its fused children,
 * which see exactly one instance of it — so the report names a single source. The sink below the
 * shuffle broadcasts each report to every partition, so a downstream task sees a report from every
 * instance of the upstream transform and has to tell them apart to know when it has heard from all
 * of them. The shuffle is where that identity is attached.
 */
public class ShuffleByKeyProcessorTest {

  private static final String UPSTREAM_ID = "upstream";

  private static ShuffleByKeyProcessor processorFor(int taskPartition, int upstreamPartitions) {
    return processorFor(taskPartition, upstreamPartitions, upstreamPartitions);
  }

  @SuppressWarnings("unchecked")
  private static ShuffleByKeyProcessor processorFor(
      int taskPartition, int upstreamPartitions, int downstreamPartitions) {
    ShuffleByKeyProcessor processor =
        new ShuffleByKeyProcessor(
            (org.apache.beam.sdk.coders.Coder<Object>)
                (org.apache.beam.sdk.coders.Coder<?>) StringUtf8Coder.of(),
            upstreamPartitions,
            downstreamPartitions,
            "shuffle-node",
            new TerminationTracker());
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx =
        new MockProcessorContext<>(new Properties(), new TaskId(0, taskPartition), null);
    processor.init(ctx);
    lastContext = ctx;
    return processor;
  }

  private static MockProcessorContext<byte[], KStreamsPayload<?>> lastContext;

  private static Record<byte[], KStreamsPayload<?>> watermark(long millis) {
    // As forwarded in process by the upstream transform: a single source, since a fused child sees
    // exactly one instance of it.
    return new Record<>(new byte[0], KStreamsPayload.watermark(millis, UPSTREAM_ID, 0, 1), 0L);
  }

  @Test
  public void restampsTheWatermarkWithTheUpstreamInstanceIdentity() {
    // Instance 2 of a 4-instance upstream transform.
    ShuffleByKeyProcessor processor = processorFor(2, 4);

    processor.process(watermark(500L));

    assertThat(lastContext.forwarded().size(), is(1));
    WatermarkPayload out = lastContext.forwarded().get(0).record().value().asWatermark();
    assertThat(out.getWatermarkMillis(), is(500L));
    // The transform id still names the producer, so a downstream aggregator matches it to the
    // upstream it expects; the partition identity is what it counts.
    assertThat(out.getTransformId(), is(UPSTREAM_ID));
    assertThat(out.getSourcePartition(), is(2));
    assertThat(out.getTotalSourcePartitions(), is(4));
  }

  @Test
  public void distinctUpstreamInstancesRestampDistinctly() {
    processorFor(0, 4).process(watermark(100L));
    WatermarkPayload first = lastContext.forwarded().get(0).record().value().asWatermark();
    processorFor(3, 4).process(watermark(100L));
    WatermarkPayload second = lastContext.forwarded().get(0).record().value().asWatermark();

    // Two instances of the same transform must be distinguishable downstream, or a consumer would
    // treat one report as if every instance had already reported.
    assertThat(first.getSourcePartition(), is(0));
    assertThat(second.getSourcePartition(), is(3));
  }

  @Test
  public void anUnpartitionedUpstreamStillReportsASingleSource() {
    ShuffleByKeyProcessor processor = processorFor(0, 1);

    processor.process(watermark(700L));

    WatermarkPayload out = lastContext.forwarded().get(0).record().value().asWatermark();
    assertThat(out.getSourcePartition(), is(0));
    assertThat(out.getTotalSourcePartitions(), is(1));
  }

  @Test
  public void dataIsRekeyedByTheBeamKeyAndNotRestamped() {
    ShuffleByKeyProcessor processor = processorFor(1, 4);

    processor.process(
        new Record<>(
            new byte[0],
            KStreamsPayload.data(
                WindowedValues.valueInGlobalWindow(
                    org.apache.beam.sdk.values.KV.of("key", "value"))),
            0L));

    assertThat(lastContext.forwarded().size(), is(1));
    assertThat(lastContext.forwarded().get(0).record().value().isData(), is(true));
  }

  private static Record<byte[], KStreamsPayload<?>> flush() {
    // The shuffle ignores these targets and picks its own.
    return new Record<>(new byte[0], KStreamsPayload.flush(ImmutableSet.of(0)), 0L);
  }

  @Test
  public void aFlushIsAddressedToThisInstancesShareOfTheRepartitionTopic() {
    // Partition 2 of 10 upstream, 8 downstream.
    processorFor(2, 10, 8).process(flush());

    assertThat(lastContext.forwarded().size(), is(1));
    FlushPayload out = lastContext.forwarded().get(0).record().value().asFlush();
    assertThat(out.getTargetPartitions(), is(ImmutableSet.of(1)));
  }

  @Test
  public void anInstanceWithNothingToAddressForwardsNoFlush() {
    // 10 upstream into 8 downstream: partition 0 addresses nothing.
    processorFor(0, 10, 8).process(flush());

    assertThat(lastContext.forwarded().isEmpty(), is(true));
  }

  @Test
  public void flushTargetsMatchTheWorkedExample() {
    // 10 upstream partitions owned in pairs, 8 downstream.
    assertThat(pairTargets(0, 10, 8), is(ImmutableSet.of(0)));
    assertThat(pairTargets(2, 10, 8), is(ImmutableSet.of(1, 2)));
    assertThat(pairTargets(4, 10, 8), is(ImmutableSet.of(3)));
    assertThat(pairTargets(6, 10, 8), is(ImmutableSet.of(4, 5)));
    assertThat(pairTargets(8, 10, 8), is(ImmutableSet.of(6, 7)));
  }

  private static Set<Integer> pairTargets(int first, int upstream, int downstream) {
    return ImmutableSet.<Integer>builder()
        .addAll(ShuffleByKeyProcessor.flushTargets(first, upstream, downstream))
        .addAll(ShuffleByKeyProcessor.flushTargets(first + 1, upstream, downstream))
        .build();
  }

  @Test
  public void everyDownstreamPartitionIsTargetedExactlyOnce() {
    int[][] shapes = {{10, 8}, {2, 8}, {8, 2}, {4, 4}, {1, 1}, {1, 16}, {16, 1}, {3, 7}, {7, 3}};
    for (int[] shape : shapes) {
      int upstream = shape[0];
      int downstream = shape[1];
      int[] hits = new int[downstream];
      for (int partition = 0; partition < upstream; partition++) {
        for (int target : ShuffleByKeyProcessor.flushTargets(partition, upstream, downstream)) {
          hits[target]++;
        }
      }
      for (int target = 0; target < downstream; target++) {
        assertThat(
            "upstream=" + upstream + " downstream=" + downstream + " target=" + target,
            hits[target],
            is(1));
      }
    }
  }
}
