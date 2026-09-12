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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Test;

/**
 * Tests how {@link ShuffleByKeyProcessor} restamps a watermark report as it is about to cross a
 * repartition topic.
 *
 * <p>Upstream of the shuffle a transform forwards its watermark in process, to its fused children,
 * which see exactly one instance of it — so the report names a single source. The sink below the
 * shuffle broadcasts each report to every partition, so a downstream task sees a report from every
 * instance of the upstream transform and has to tell them apart to know when it has heard from all
 * of them. The shuffle is where that identity is attached.
 */
public class ShuffleByKeyProcessorTest {

  private static final String UPSTREAM_ID = "upstream";

  @SuppressWarnings("unchecked")
  private static ShuffleByKeyProcessor processorFor(int taskPartition, int upstreamPartitions) {
    ShuffleByKeyProcessor processor =
        new ShuffleByKeyProcessor(
            (org.apache.beam.sdk.coders.Coder<Object>)
                (org.apache.beam.sdk.coders.Coder<?>) StringUtf8Coder.of(),
            upstreamPartitions,
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
}
