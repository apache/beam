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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link Processor} that executes a fused {@link ExecutableStage} (stateless user
 * code such as ParDo) in the Beam SDK harness over the Fn API.
 *
 * <p>For each {@link KStreamsPayload#isData() data} payload it unwraps the {@link WindowedValue}
 * and feeds it to the harness through the stage's main input {@link FnDataReceiver}. Harness
 * outputs are collected on the harness threads into {@link #pendingOutputs} and then flushed
 * downstream on the Kafka Streams processing thread when the bundle closes — Kafka Streams' {@link
 * ProcessorContext#forward} must only be called from the processing thread, so outputs are never
 * forwarded directly from a harness callback.
 *
 * <p>A {@link KStreamsPayload#isWatermark() watermark} payload is a report from one partition of
 * one upstream transform and marks a bundle boundary: the open bundle (if any) is closed (flushing
 * outputs), the report is fed to the {@link WatermarkAggregator}, and the stage's output watermark
 * is forwarded downstream — stamped with this stage's own transform id — only when the aggregate
 * across the upstream transform's partitions actually advances. Until every partition has reported,
 * the watermark is held and nothing is forwarded — but data is still processed in the meantime.
 *
 * <p>This is the Kafka Streams analogue of Flink's {@code ExecutableStageDoFnOperator} and Spark's
 * {@code SparkExecutableStageFunction}. State, timers, and side inputs are out of scope for this
 * first version: the stage is executed with {@link StateRequestHandler#unsupported()} and no timer
 * receivers.
 */
class ExecutableStageProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageProcessor.class);

  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final JobInfo jobInfo;
  // This stage's own transform id, stamped on every watermark it forwards so downstream watermark
  // aggregators know which transform the report came from — regardless of who consumes it.
  private final String transformId;

  // pendingOutputs is enqueued by SDK harness threads (inside the OutputReceiverFactory callback)
  // and drained by the Kafka Streams processing thread on bundle close; needs to be thread-safe.
  // The element type is intentionally wildcarded: the runner does not need to know the runtime
  // value type — the bundle factory handles all coder application at the Fn-API boundary using
  // the PCollection coders from the ExecutableStagePayload. Pretending the type was byte[] was
  // only safe because the Impulse output coder happens to be ByteArrayCoder.
  private final Queue<WindowedValue<?>> pendingOutputs = new ConcurrentLinkedQueue<>();

  // Computes this stage's input watermark from its upstream transform's reports, holding until
  // every partition of the upstream transform has reported (see WatermarkAggregator).
  private final WatermarkAggregator watermarkAggregator;
  // The last watermark actually forwarded downstream, so we only forward when it advances.
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  private @Nullable ExecutableStageContext stageContext;
  private @Nullable StageBundleFactory stageBundleFactory;
  private @Nullable RemoteBundle currentBundle;

  /**
   * @param transformId this stage's own transform id, stamped on the watermarks it emits
   * @param upstreamTransformIds the transform ids feeding this stage (known from the pipeline
   *     graph), whose reports the {@link WatermarkAggregator} waits for
   */
  ExecutableStageProcessor(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      String transformId,
      Set<String> upstreamTransformIds) {
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.transformId = transformId;
    this.watermarkAggregator = new WatermarkAggregator(upstreamTransformIds);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    // The SDK harness (stage context + bundle factory) is created lazily on the first data
    // element, so a stage that only forwards watermarks never spins one up. This mirrors Spark's
    // SparkExecutableStageFunction, which likewise does not build a bundle factory when there are
    // no inputs to process.
  }

  private void ensureStageBundleFactory() {
    if (stageBundleFactory != null) {
      return;
    }
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    stageContext = KafkaStreamsExecutableStageContextFactory.getInstance().get(jobInfo);
    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    if (payload == null) {
      // A topic feeding the runner can always be written to from outside (or carry a tombstone),
      // so recover from the obvious error instead of crashing the task: warn and drop.
      LOG.warn(
          "Stage {} dropping record with null payload (external write or tombstone)", transformId);
      return;
    }
    if (payload.isWatermark()) {
      // Emit any buffered outputs before the watermark. Data is processed regardless of watermark
      // readiness; only the watermark itself is held until every source partition has reported.
      closeBundleAndFlush(record);
      // Feed the report into the aggregator and forward the stage's output watermark only when the
      // aggregate across the upstream transform's partitions actually advances.
      watermarkAggregator.observe(payload.asWatermark());
      Instant advanced = watermarkAggregator.advance();
      if (advanced.isAfter(lastForwardedWatermark)) {
        lastForwardedWatermark = advanced;
        forwardWatermark(record, advanced.getMillis());
      }
      return;
    }
    try {
      ensureBundleOpen();
      mainInputReceiver().accept(payload.getData());
    } catch (Exception e) {
      throw new RuntimeException("Failed to process element through SDK harness", e);
    }
  }

  private void ensureBundleOpen() throws Exception {
    if (currentBundle != null) {
      return;
    }
    ensureStageBundleFactory();
    StageBundleFactory factory = checkInitialized(stageBundleFactory);
    OutputReceiverFactory outputReceiverFactory =
        new OutputReceiverFactory() {
          @Override
          public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
            // Outputs are queued here on harness threads and drained on the processing thread
            // after the bundle closes.
            return receivedElement -> {
              if (receivedElement != null) {
                pendingOutputs.add((WindowedValue<?>) receivedElement);
              }
            };
          }
        };
    currentBundle =
        factory.getBundle(
            outputReceiverFactory,
            StateRequestHandler.unsupported(),
            BundleProgressHandler.ignored());
  }

  private FnDataReceiver<WindowedValue<?>> mainInputReceiver() {
    RemoteBundle bundle = checkInitialized(currentBundle);
    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver =
        (FnDataReceiver<WindowedValue<?>>)
            (FnDataReceiver<?>) Iterables.getOnlyElement(bundle.getInputReceivers().values());
    return receiver;
  }

  private void closeBundleAndFlush(Record<byte[], KStreamsPayload<?>> record) {
    RemoteBundle bundle = currentBundle;
    if (bundle == null) {
      return;
    }
    try {
      // close() blocks until the harness finishes the bundle and all outputs have been delivered
      // to the output receiver (and hence enqueued in pendingOutputs).
      bundle.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close SDK harness bundle", e);
    } finally {
      currentBundle = null;
    }
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    // The harness has finished the bundle (close() returned) so no further enqueues happen.
    // Drain via poll() so each element is removed as it is forwarded.
    WindowedValue<?> output;
    while ((output = pendingOutputs.poll()) != null) {
      ctx.forward(
          new Record<byte[], KStreamsPayload<?>>(
              record.key(), KStreamsPayload.data(output), record.timestamp()));
    }
  }

  private void forwardWatermark(Record<byte[], KStreamsPayload<?>> record, long watermarkMillis) {
    // Stamped with this stage's own transform id; this stage is a single instance for now, so the
    // report is for its only partition (0 of 1). Fanning the watermark out to every downstream
    // partition — and producing it atomically with the offset commit so it is durable — lands with
    // the topic-based shuffle work (#18479).
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            record.key(),
            KStreamsPayload.watermark(watermarkMillis, transformId, 0, 1),
            record.timestamp()));
  }

  @Override
  public void close() {
    try {
      if (currentBundle != null) {
        currentBundle.close();
        currentBundle = null;
      }
    } catch (Exception e) {
      LOG.warn("Error closing in-flight SDK harness bundle", e);
    }
    try {
      if (stageBundleFactory != null) {
        stageBundleFactory.close();
        stageBundleFactory = null;
      }
    } catch (Exception e) {
      LOG.warn("Error closing stage bundle factory", e);
    }
    try {
      if (stageContext != null) {
        stageContext.close();
        stageContext = null;
      }
    } catch (Exception e) {
      LOG.warn("Error closing executable stage context", e);
    }
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("ExecutableStageProcessor used before init()");
    }
    return value;
  }
}
