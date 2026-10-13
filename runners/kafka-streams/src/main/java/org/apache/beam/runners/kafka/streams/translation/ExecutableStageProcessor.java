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

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
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
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link Processor} that executes a fused {@link ExecutableStage} — stateless user
 * code such as ParDo — in the Beam SDK harness over the Fn API.
 *
 * <p>Each {@link KStreamsPayload#isData() data} payload is unwrapped and fed to the harness through
 * the stage's main input {@link FnDataReceiver}. Harness outputs are collected on the harness
 * threads into {@link #pendingOutputs} and flushed downstream when the bundle closes, because
 * {@link ProcessorContext#forward} may only be called from the processing thread.
 *
 * <p>A {@link KStreamsPayload#isWatermark() watermark} payload marks a bundle boundary: the open
 * bundle is closed and flushed, the report goes to the {@link WatermarkAggregator}, and the stage's
 * output watermark is forwarded — stamped with this stage's transform id — only once the aggregate
 * across the upstream partitions advances. Until every partition has reported the watermark is
 * held, though data is still processed meanwhile.
 *
 * <p>A bundle is also bounded by {@code --maxBundleSize}, checked as elements arrive; without it a
 * bundle would stay open until the next watermark and grow without limit on a steady stream. The
 * time bound {@code --maxBundleTimeMs} is not applied yet, see that option's documentation.
 *
 * <p>Closing a bundle asks Kafka Streams to commit, so the elements consumed and the records
 * produced commit together and a restart replays all of a bundle or none. This aligns commits to
 * bundle boundaries but does not stop Kafka Streams committing on its own interval mid-bundle;
 * ruling that out needs a pre-commit hook.
 *
 * <p>The analogue of Flink's {@code ExecutableStageDoFnOperator} and Spark's {@code
 * SparkExecutableStageFunction}. State, timers and side inputs are out of scope here: the stage
 * runs with {@link StateRequestHandler#unsupported()} and no timer receivers.
 */
class ExecutableStageProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageProcessor.class);

  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final JobInfo jobInfo;
  // Stamped on every watermark forwarded, so downstream aggregators know which transform reported.
  private final String transformId;
  // Updated from the MonitoringInfos the harness reports as each bundle completes.
  private final MetricsContainerImpl metricsContainer;

  // Enqueued by harness threads and drained by the processing thread on bundle close, so it must
  // be thread-safe. Each entry carries its output PCollection id for routing on flush. The element
  // type is wildcarded: coders are applied by the bundle factory at the Fn-API boundary.
  private final Queue<PendingOutput> pendingOutputs = new ConcurrentLinkedQueue<>();
  // Output PCollection id -> relay child node. Empty for a single-output stage.
  private final Map<String, String> outputChildByPCollectionId;

  // Holds until every partition of the upstream transform has reported; see WatermarkAggregator.
  private final WatermarkAggregator watermarkAggregator;
  // Reports this stage finished at the terminal watermark, so a bounded pipeline can stop.
  private final TerminationReporter terminationReporter;
  // The last watermark actually forwarded downstream, so we only forward when it advances.
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  private @Nullable ExecutableStageContext stageContext;
  private @Nullable StageBundleFactory stageBundleFactory;
  private @Nullable RemoteBundle currentBundle;

  /** Bound on how many elements may be fed to one bundle. */
  private final int maxBundleSize;

  /** Elements fed to the open bundle, for the size bound above. */
  private int elementsInBundle;

  /**
   * @param transformId this stage's own transform id, stamped on the watermarks it emits
   * @param upstreamTransformIds the transform ids feeding this stage (known from the pipeline
   *     graph), whose reports the {@link WatermarkAggregator} waits for
   * @param metricsContainer this stage's container in the job's metrics step map, updated with the
   *     harness's per-bundle MonitoringInfos
   */
  ExecutableStageProcessor(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      String transformId,
      Set<String> upstreamTransformIds,
      MetricsContainerImpl metricsContainer,
      Map<String, String> outputChildByPCollectionId,
      int maxBundleSize,
      TerminationTracker terminationTracker) {
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.transformId = transformId;
    this.watermarkAggregator = new WatermarkAggregator(upstreamTransformIds);
    this.metricsContainer = metricsContainer;
    this.outputChildByPCollectionId = ImmutableMap.copyOf(outputChildByPCollectionId);
    this.maxBundleSize = maxBundleSize;
    this.terminationReporter = new TerminationReporter(terminationTracker, transformId);
  }

  /** A harness output element together with the id of the output PCollection it belongs to. */
  private static final class PendingOutput {
    final String pCollectionId;
    final WindowedValue<?> value;

    PendingOutput(String pCollectionId, WindowedValue<?> value) {
      this.pCollectionId = pCollectionId;
      this.value = value;
    }
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    terminationReporter.init(context);
    // Created lazily on the first data element, so a stage that only forwards watermarks never
    // spins up a harness. Spark's SparkExecutableStageFunction does the same.
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
      // A topic can always be written to from outside, so warn and drop rather than crash.
      LOG.warn(
          "Stage {} dropping record with null payload (external write or tombstone)", transformId);
      return;
    }
    if (payload.isWatermark()) {
      // Flush buffered outputs before the watermark. Data is processed regardless of readiness;
      // only the watermark waits for every source partition.
      closeBundleAndFlush(record);
      // Forward the output watermark only when the aggregate across upstream partitions advances.
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
      elementsInBundle++;
    } catch (Exception e) {
      throw new RuntimeException("Failed to process element through SDK harness", e);
    }
    if (elementsInBundle >= maxBundleSize) {
      closeBundleAndFlush(record);
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
            // Queued on harness threads, drained on the processing thread after the bundle closes.
            return receivedElement -> {
              if (receivedElement != null) {
                pendingOutputs.add(
                    new PendingOutput(pCollectionId, (WindowedValue<?>) receivedElement));
              }
            };
          }
        };
    // Fold the harness's reported metrics into this stage's container when each bundle completes.
    // Only the completion response is applied: it carries the bundle's final cumulative values, and
    // the container's update() adds counter values, so also applying mid-bundle progress snapshots
    // would double-count them. Live mid-bundle metrics can come later if a use appears.
    BundleProgressHandler progressHandler =
        new BundleProgressHandler() {
          @Override
          public void onProgress(ProcessBundleProgressResponse progress) {
            // Deliberately not folded into the container; see comment above.
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Stage {} bundle progress: {}",
                  transformId,
                  TextFormat.printer().printToString(progress));
            }
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Stage {} bundle completed: {}",
                  transformId,
                  TextFormat.printer().printToString(response));
            }
            metricsContainer.update(response.getMonitoringInfosList());
          }
        };
    currentBundle =
        factory.getBundle(
            outputReceiverFactory, StateRequestHandler.unsupported(), progressHandler);
    elementsInBundle = 0;
  }

  private FnDataReceiver<WindowedValue<?>> mainInputReceiver() {
    RemoteBundle bundle = checkInitialized(currentBundle);
    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver =
        (FnDataReceiver<WindowedValue<?>>)
            (FnDataReceiver<?>) Iterables.getOnlyElement(bundle.getInputReceivers().values());
    return receiver;
  }

  /**
   * Finishes the open bundle, forwards everything it produced, and asks Kafka Streams to commit.
   *
   * <p>The commit request is what ties a bundle to a transaction: the elements the bundle consumed
   * and the records it produced are then committed together, so a restart either replays the whole
   * bundle or none of it.
   *
   * <p>The outputs carry the key of the record that closed the bundle. An executable stage is
   * unkeyed — it runs stateless, with no state or timers — so the Kafka record key means nothing to
   * it and is only being carried along; where the key does matter, downstream sets it, as {@link
   * ShuffleByKeyProcessor} does from the Beam key before a GroupByKey.
   */
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
      elementsInBundle = 0;
    }
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    // The harness has finished the bundle (close() returned) so no further enqueues happen.
    // Drain via poll() so each element is removed as it is forwarded. Each output is routed to its
    // own output's relay child for a multi-output stage; a single-output stage forwards directly to
    // its one downstream (empty routing map).
    PendingOutput output;
    while ((output = pendingOutputs.poll()) != null) {
      Record<byte[], KStreamsPayload<?>> outputRecord =
          new Record<byte[], KStreamsPayload<?>>(
              record.key(), KStreamsPayload.data(output.value), record.timestamp());
      String childNode = outputChildByPCollectionId.get(output.pCollectionId);
      if (childNode == null) {
        ctx.forward(outputRecord);
      } else {
        ctx.forward(outputRecord, childNode);
      }
    }
    ctx.commit();
  }

  private void forwardWatermark(Record<byte[], KStreamsPayload<?>> record, long watermarkMillis) {
    // Labelled as the only source a consumer will see. Forwarding here is in-process, to the
    // stage's
    // fused children, so exactly one instance of this stage reaches each of them. Where the output
    // instead crosses a shuffle, ShuffleByKeyProcessor relabels the report with the real partition
    // identity, because the broadcast then delivers every instance's report to every consumer.
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            record.key(),
            KStreamsPayload.watermark(watermarkMillis, transformId, 0, 1),
            record.timestamp()));
    terminationReporter.watermarkEmitted(ctx, watermarkMillis);
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
    // Last: this is what stops the pipeline waiting on this stage, and closing the bundle above can
    // still forward records downstream. Releasing it first would let the pipeline be declared
    // finished while this stage was flushing.
    terminationReporter.close();
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("ExecutableStageProcessor used before init()");
    }
    return value;
  }
}
