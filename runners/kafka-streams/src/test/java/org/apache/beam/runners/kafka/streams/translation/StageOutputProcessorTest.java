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

import java.util.List;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Test;

/**
 * Unit tests for {@link StageOutputProcessor}, the per-output relay of a multi-output stage: it
 * forwards data unchanged and relabels the watermark's transform id while preserving the reporting
 * stage instance's partition and partition count.
 *
 * <p>The partition preservation is what makes the watermark propagate correctly once the multi-
 * output stage runs in several parallel instances (per je-ik's review): each stage task's report
 * must reach downstream carrying its own {@code (partition, totalPartitions)}, so the downstream
 * aggregator can tell how many parallel instances produced the output. A {@link
 * MockProcessorContext} lets the reports from different stage partitions be fed directly, which a
 * single-instance {@code TopologyTestDriver} cannot do.
 */
public class StageOutputProcessorTest {

  private static final String RELAY_ID = "stage-output-0";
  private static final String STAGE_ID = "stage";

  private static Record<byte[], KStreamsPayload<?>> watermark(
      long millis, int sourcePartition, int totalPartitions) {
    return new Record<>(
        new byte[0],
        KStreamsPayload.watermark(millis, STAGE_ID, sourcePartition, totalPartitions),
        0L);
  }

  @Test
  public void watermarkKeepsPartitionIdentityAndRelabelsTransformId() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    StageOutputProcessor processor = new StageOutputProcessor(RELAY_ID);
    processor.init(ctx);

    // A report from partition 1 of a 3-instance stage.
    processor.process(watermark(500L, 1, 3));

    List<CapturedForward<? extends byte[], ? extends KStreamsPayload<?>>> forwarded =
        ctx.forwarded();
    assertThat(forwarded.size(), is(1));
    WatermarkPayload out = forwarded.get(0).record().value().asWatermark();
    assertThat(out.getWatermarkMillis(), is(500L));
    // Relabeled to the relay's id, but the stage instance's partition and count are preserved.
    assertThat(out.getTransformId(), is(RELAY_ID));
    assertThat(out.getSourcePartition(), is(1));
    assertThat(out.getTotalSourcePartitions(), is(3));
  }

  @Test
  public void distinctStagePartitionsStayDistinctDownstream() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    StageOutputProcessor processor = new StageOutputProcessor(RELAY_ID);
    processor.init(ctx);

    processor.process(watermark(100L, 0, 3));
    processor.process(watermark(200L, 2, 3));

    List<CapturedForward<? extends byte[], ? extends KStreamsPayload<?>>> forwarded =
        ctx.forwarded();
    assertThat(forwarded.size(), is(2));
    assertThat(forwarded.get(0).record().value().asWatermark().getSourcePartition(), is(0));
    assertThat(forwarded.get(1).record().value().asWatermark().getSourcePartition(), is(2));
    assertThat(forwarded.get(0).record().value().asWatermark().getTotalSourcePartitions(), is(3));
  }

  @Test
  public void dataIsForwardedUnchanged() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    StageOutputProcessor processor = new StageOutputProcessor(RELAY_ID);
    processor.init(ctx);

    WindowedValue<byte[]> element = WindowedValues.valueInGlobalWindow(new byte[] {7});
    KStreamsPayload<byte[]> data = KStreamsPayload.data(element);
    processor.process(new Record<>(new byte[0], data, 0L));

    List<CapturedForward<? extends byte[], ? extends KStreamsPayload<?>>> forwarded =
        ctx.forwarded();
    assertThat(forwarded.size(), is(1));
    assertThat(forwarded.get(0).record().value(), is(data));
  }
}
