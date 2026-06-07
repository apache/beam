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
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * <p>A {@link KStreamsPayload#isWatermark() watermark} payload marks a bundle boundary: the open
 * bundle (if any) is closed (flushing outputs), and the watermark is then forwarded downstream so
 * that subsequent stages observe it after all data of the bundle.
 *
 * <p>This is the Kafka Streams analogue of Flink's {@code ExecutableStageDoFnOperator} and Spark's
 * {@code SparkExecutableStageFunction}. State, timers, and side inputs are out of scope for this
 * first version: the stage is executed with {@link StateRequestHandler#unsupported()} and no timer
 * receivers.
 */
class ExecutableStageProcessor
    implements Processor<byte[], KStreamsPayload<byte[]>, byte[], KStreamsPayload<byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageProcessor.class);

  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final JobInfo jobInfo;

  // pendingOutputs is enqueued by SDK harness threads (inside the OutputReceiverFactory callback)
  // and drained by the Kafka Streams processing thread on bundle close; needs to be thread-safe.
  private final Queue<WindowedValue<byte[]>> pendingOutputs = new ConcurrentLinkedQueue<>();

  private @Nullable ProcessorContext<byte[], KStreamsPayload<byte[]>> context;
  private @Nullable ExecutableStageContext stageContext;
  private @Nullable StageBundleFactory stageBundleFactory;
  private @Nullable RemoteBundle currentBundle;

  ExecutableStageProcessor(RunnerApi.ExecutableStagePayload stagePayload, JobInfo jobInfo) {
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<byte[]>> context) {
    this.context = context;
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    this.stageContext = KafkaStreamsExecutableStageContextFactory.getInstance().get(jobInfo);
    this.stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<byte[]>> record) {
    KStreamsPayload<byte[]> payload = record.value();
    if (payload.isWatermark()) {
      // NOTE: flushing the bundle on every received watermark is provisional. Once the
      // WatermarkManager lands, a stage will receive watermarks from multiple parent instances and
      // the output watermark becomes min() across them — the bundle should flush / the output
      // watermark advance only when that minimum actually moves forward, not on every received
      // watermark. Tracked in #38743.
      closeBundleAndFlush(record);
      forwardWatermark(record, payload.getWatermarkMillis());
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
    StageBundleFactory factory = checkInitialized(stageBundleFactory);
    OutputReceiverFactory outputReceiverFactory =
        new OutputReceiverFactory() {
          @Override
          public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
            // Outputs are queued here on harness threads and drained on the processing thread
            // after the bundle closes.
            return receivedElement -> {
              if (receivedElement != null) {
                pendingOutputs.add((WindowedValue<byte[]>) receivedElement);
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

  private void closeBundleAndFlush(Record<byte[], KStreamsPayload<byte[]>> record) {
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
    ProcessorContext<byte[], KStreamsPayload<byte[]>> ctx = checkInitialized(context);
    // The harness has finished the bundle (close() returned) so no further enqueues happen.
    // ConcurrentLinkedQueue's weakly-consistent iterator is therefore safe to drain via forEach.
    pendingOutputs.forEach(
        output ->
            ctx.forward(
                new Record<byte[], KStreamsPayload<byte[]>>(
                    record.key(), KStreamsPayload.data(output), record.timestamp())));
    pendingOutputs.clear();
  }

  private void forwardWatermark(
      Record<byte[], KStreamsPayload<byte[]>> record, long watermarkMillis) {
    // TODO(#38743 / WatermarkManager): a watermark must reach every parallel instance of every
    // downstream processor, but ctx.forward routes to one downstream partition per Kafka Streams'
    // partitioning. The simplest correct approach is to fan the watermark out to all downstream
    // partitions; that wiring lands with the WatermarkManager sub-issue (per Jan on PR #38764).
    ProcessorContext<byte[], KStreamsPayload<byte[]>> ctx = checkInitialized(context);
    ctx.forward(
        new Record<byte[], KStreamsPayload<byte[]>>(
            record.key(), KStreamsPayload.watermark(watermarkMillis), record.timestamp()));
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
