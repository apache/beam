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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Test;

/**
 * Tests the watermark wiring of {@link ExecutableStageProcessor}: how it feeds incoming watermark
 * reports to the {@link WatermarkAggregator} and forwards the stage's output watermark.
 *
 * <p>Only the watermark path is exercised, so the SDK harness is never started (it is created
 * lazily on the first data element). A {@link MockProcessorContext} captures what the processor
 * forwards downstream.
 */
public class ExecutableStageProcessorWatermarkTest {

  /** The stage's own transform id, expected on every watermark it forwards. */
  private static final String STAGE_ID = "stage";

  /** The single upstream transform whose reports the stage aggregates. */
  private static final String UPSTREAM_ID = "upstream";

  private static ExecutableStageProcessor newProcessor() {
    JobInfo jobInfo =
        JobInfo.create(
            "job-id",
            "job-name",
            "",
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    return new ExecutableStageProcessor(
        RunnerApi.ExecutableStagePayload.getDefaultInstance(),
        jobInfo,
        STAGE_ID,
        ImmutableSet.of(UPSTREAM_ID),
        new MetricsContainerImpl(STAGE_ID),
        // Single-output: no per-output routing (this test drives the watermark path directly).
        ImmutableMap.of());
  }

  /** A report from the upstream transform's given partition. */
  private static Record<byte[], KStreamsPayload<?>> watermark(
      long millis, int sourcePartition, int totalSourcePartitions) {
    KStreamsPayload<?> payload =
        KStreamsPayload.watermark(millis, UPSTREAM_ID, sourcePartition, totalSourcePartitions);
    return new Record<>(new byte[0], payload, 0L);
  }

  private static KStreamsPayload<?> onlyForwarded(
      MockProcessorContext<byte[], KStreamsPayload<?>> ctx) {
    assertThat(ctx.forwarded().size(), is(1));
    return ctx.forwarded().get(0).record().value();
  }

  @Test
  public void singleSourcePartitionForwardsImmediatelyStampedAsItsOwnSource() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    ExecutableStageProcessor processor = newProcessor();
    processor.init(ctx);

    processor.process(watermark(100L, 0, 1));

    KStreamsPayload<?> out = onlyForwarded(ctx);
    assertThat(out.isWatermark(), is(true));
    WatermarkPayload report = out.asWatermark();
    assertThat(report.getWatermarkMillis(), is(100L));
    // The stage forwards under its own identity — its transform id and its own single partition
    // (0 of 1) — not the upstream's.
    assertThat(report.getTransformId(), is(STAGE_ID));
    assertThat(report.getSourcePartition(), is(0));
    assertThat(report.getTotalSourcePartitions(), is(1));
  }

  @Test
  public void holdsUntilAllSourcePartitionsReportThenForwardsMin() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    ExecutableStageProcessor processor = newProcessor();
    processor.init(ctx);

    processor.process(watermark(300L, 0, 3));
    processor.process(watermark(100L, 1, 3));
    // Two of three source partitions reported — still holding, nothing forwarded.
    assertThat(ctx.forwarded().isEmpty(), is(true));

    processor.process(watermark(500L, 2, 3));
    // All three reported — forward min(300, 100, 500) = 100.
    assertThat(onlyForwarded(ctx).asWatermark().getWatermarkMillis(), is(100L));
  }

  @Test
  public void doesNotReforwardWhenWatermarkDoesNotAdvance() {
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    ExecutableStageProcessor processor = newProcessor();
    processor.init(ctx);

    processor.process(watermark(100L, 0, 1));
    assertThat(ctx.forwarded().size(), is(1));

    // A repeated, non-advancing watermark must not be forwarded again.
    processor.process(watermark(100L, 0, 1));
    assertThat(ctx.forwarded().size(), is(1));
  }

  @Test
  public void forwardsTerminalMaxWatermark() {
    long maxMillis = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    MockProcessorContext<byte[], KStreamsPayload<?>> ctx = new MockProcessorContext<>();
    ExecutableStageProcessor processor = newProcessor();
    processor.init(ctx);

    processor.process(watermark(maxMillis, 0, 1));

    assertThat(onlyForwarded(ctx).asWatermark().getWatermarkMillis(), is(maxMillis));
  }
}
