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
package org.apache.beam.runners.core;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.sdk.fn.splittabledofn.RestrictionTrackers;
import org.apache.beam.sdk.fn.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StartBundleContext;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.TimestampObservingWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link SplittableProcessElementInvoker} that requests a checkpoint after the {@link
 * DoFn.ProcessElement} call either outputs at least a given number of elements (in total over all
 * outputs), or runs for the given duration.
 */
public class OutputAndTimeBoundedSplittableProcessElementInvoker<
        InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
    extends SplittableProcessElementInvoker<
        InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT> {
  private final DoFn<InputT, OutputT> fn;
  private final PipelineOptions pipelineOptions;
  private final OutputWindowedValue<OutputT> output;
  private final SideInputReader sideInputReader;
  private final ScheduledExecutorService executor;
  private final int maxNumOutputs;
  private final Duration maxDuration;
  private final Supplier<BundleFinalizer> bundleFinalizer;

  /**
   * Creates a new invoker from components.
   *
   * @param fn The original {@link DoFn}.
   * @param pipelineOptions {@link PipelineOptions} to include in the {@link DoFn.ProcessContext}.
   * @param output Hook for outputting from the {@link DoFn.ProcessElement} method.
   * @param sideInputReader Hook for accessing side inputs.
   * @param executor Executor on which a checkpoint will be scheduled after the given duration.
   * @param maxNumOutputs Maximum number of outputs, in total over all output tags, after which a
   *     checkpoint will be requested. This is a best-effort request - the {@link DoFn} may output
   *     more after receiving the request.
   * @param maxDuration Maximum duration of the {@link DoFn.ProcessElement} call (counted from the
   *     first successful {@link RestrictionTracker#tryClaim} call) after which a checkpoint will be
   *     requested. This is a best-effort request - the {@link DoFn} may run for longer after
   *     receiving the request.
   */
  public OutputAndTimeBoundedSplittableProcessElementInvoker(
      DoFn<InputT, OutputT> fn,
      PipelineOptions pipelineOptions,
      OutputWindowedValue<OutputT> output,
      SideInputReader sideInputReader,
      ScheduledExecutorService executor,
      int maxNumOutputs,
      Duration maxDuration,
      Supplier<BundleFinalizer> bundleFinalizer) {
    this.fn = fn;
    this.pipelineOptions = pipelineOptions;
    this.output = output;
    this.sideInputReader = sideInputReader;
    this.executor = executor;
    this.maxNumOutputs = maxNumOutputs;
    this.maxDuration = maxDuration;
    this.bundleFinalizer = bundleFinalizer;
  }

  @Override
  public Result invokeProcessElement(
      DoFnInvoker<InputT, OutputT> invoker,
      final WindowedValue<InputT> element,
      final RestrictionTracker<RestrictionT, PositionT> tracker,
      final WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
    final ProcessContext processContext = new ProcessContext(element, tracker, watermarkEstimator);

    DoFn.ProcessContinuation cont =
        invoker.invokeProcessElement(
            new DoFnInvoker.BaseArgumentProvider<InputT, OutputT>() {
              @Override
              public String getErrorContext() {
                return OutputAndTimeBoundedSplittableProcessElementInvoker.class.getSimpleName();
              }

              @Override
              public DoFn<InputT, OutputT>.ProcessContext processContext(
                  DoFn<InputT, OutputT> doFn) {
                return processContext;
              }

              @Override
              public Object restriction() {
                return tracker.currentRestriction();
              }

              @Override
              public InputT element(DoFn<InputT, OutputT> doFn) {
                return processContext.element();
              }

              @Override
              public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                return processContext.timestamp();
              }

              @Override
              public String timerId(DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException(
                    "Cannot access timerId as parameter outside of @OnTimer method.");
              }

              @Override
              public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException(
                    "Access to time domain not supported in ProcessElement");
              }

              @Override
              public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
                return DoFnOutputReceivers.windowedReceiver(processContext, null);
              }

              @Override
              public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException("Not supported in SplittableDoFn");
              }

              @Override
              public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
                return DoFnOutputReceivers.windowedMultiReceiver(processContext, null);
              }

              @Override
              public RestrictionTracker<?, ?> restrictionTracker() {
                return processContext.tracker;
              }

              @Override
              public WatermarkEstimator<?> watermarkEstimator() {
                return processContext.watermarkEstimator;
              }

              @Override
              public PipelineOptions pipelineOptions() {
                return pipelineOptions;
              }

              @Override
              public BundleFinalizer bundleFinalizer() {
                return bundleFinalizer.get();
              }

              // Unsupported methods below.

              @Override
              public StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
                throw new IllegalStateException(
                    "Should not access startBundleContext() from @"
                        + DoFn.ProcessElement.class.getSimpleName());
              }

              @Override
              public FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
                throw new IllegalStateException(
                    "Should not access finishBundleContext() from @"
                        + DoFn.ProcessElement.class.getSimpleName());
              }
            });
    processContext.cancelScheduledCheckpoint();
    @Nullable
    KV<RestrictionT, KV<Instant, WatermarkEstimatorStateT>> residual =
        processContext.getTakenCheckpoint();
    if (cont.shouldResume()) {
      if (residual == null) {
        // No checkpoint had been taken by the runner while the ProcessElement call ran, however
        // the call says that not the whole restriction has been processed. So we need to take
        // a checkpoint now: checkpoint() guarantees that the primary restriction describes exactly
        // the work that was done in the current ProcessElement call, and returns a residual
        // restriction that describes exactly the work that wasn't done in the current call. The
        // residual is null when the entire restriction has been processed.
        residual = processContext.takeCheckpointNow();
        processContext.tracker.checkDone();
      } else {
        // A checkpoint was taken by the runner, and then the ProcessElement call returned resume()
        // without making more tryClaim() calls (since no tryClaim() calls can succeed after
        // checkpoint(), and since if it had made a failed tryClaim() call, it should have returned
        // stop()).
        // This means that the resulting primary restriction and the taken checkpoint already
        // accurately describe respectively the work that was and wasn't done in the current
        // ProcessElement call.
        // In other words, if we took a checkpoint *after* ProcessElement completed (like in the
        // branch above), it would have been equivalent to this one.
        processContext.tracker.checkDone();
      }
    } else {
      // The ProcessElement call returned stop() - that means the tracker's current restriction
      // has been fully processed by the call. A checkpoint may or may not have been taken in
      // "residual"; if it was, then we'll need to process it; if no, then we don't - nothing
      // special needs to be done.
      processContext.tracker.checkDone();
    }
    if (residual == null) {
      return new Result(null, cont, null, null);
    }
    return new Result(
        residual.getKey(), cont, residual.getValue().getKey(), residual.getValue().getValue());
  }

  private class ProcessContext extends DoFn<InputT, OutputT>.ProcessContext
      implements RestrictionTrackers.ClaimObserver<PositionT> {
    private final WindowedValue<InputT> element;
    private final RestrictionTracker<RestrictionT, PositionT> tracker;
    private final WatermarkEstimators.WatermarkAndStateObserver<WatermarkEstimatorStateT>
        watermarkEstimator;
    private int numClaimedBlocks;
    private boolean hasClaimFailed;

    private int numOutputs;
    // Checkpoint may be initiated either when the given number of outputs is reached,
    // or when the call runs for the given duration. It must be initiated at most once,
    // even if these events happen almost at the same time.
    // This is either the result of the sole tracker.checkpoint() call, or null if
    // the call completed before reaching the given number of outputs or duration.
    private @Nullable RestrictionT checkpoint;
    // Watermark captured at the moment before checkpoint was taken, describing a lower bound
    // on the output from "checkpoint" and its associated watermark estimator state.
    private @Nullable KV<Instant, WatermarkEstimatorStateT> residualWatermarkAndState;

    // A handle on the scheduled action to take a checkpoint.
    private @Nullable Future<?> scheduledCheckpoint;

    public ProcessContext(
        WindowedValue<InputT> element,
        RestrictionTracker<RestrictionT, PositionT> tracker,
        WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
      fn.super();
      this.element = element;
      this.tracker = RestrictionTrackers.observe(tracker, this);
      this.watermarkEstimator = WatermarkEstimators.threadSafe(watermarkEstimator);
    }

    @Override
    public void onClaimed(PositionT position) {
      checkState(
          !hasClaimFailed, "Must not call tryClaim() after it has previously returned false");
      if (numClaimedBlocks == 0) {
        // Claiming first block: can schedule the checkpoint now.
        // We don't schedule it right away to prevent checkpointing before any blocks are claimed,
        // in a state where no work has been done yet - because such a checkpoint is equivalent to
        // the original restriction, i.e. pointless.
        this.scheduledCheckpoint =
            executor.schedule(
                (Runnable) this::takeCheckpointNow, maxDuration.getMillis(), TimeUnit.MILLISECONDS);
      }
      ++numClaimedBlocks;
    }

    @Override
    public void onClaimFailed(PositionT position) {
      checkState(
          !hasClaimFailed, "Must not call tryClaim() after it has previously returned false");
      hasClaimFailed = true;
    }

    void cancelScheduledCheckpoint() {
      if (scheduledCheckpoint == null) {
        return;
      }
      scheduledCheckpoint.cancel(true);
      try {
        Futures.getUnchecked(scheduledCheckpoint);
      } catch (CancellationException e) {
        // This is expected if the call took less than the maximum duration.
      }
    }

    synchronized KV<RestrictionT, KV<Instant, WatermarkEstimatorStateT>> takeCheckpointNow() {
      // This method may be entered either via .output(), or via scheduledCheckpoint.
      // Only one of them "wins" - tracker.checkpoint() must be called only once.
      if (checkpoint == null) {
        residualWatermarkAndState = watermarkEstimator.getWatermarkAndState();
        SplitResult<RestrictionT> split = tracker.trySplit(0);
        if (split != null) {
          checkpoint = checkNotNull(split.getResidual());
        }
      }
      return getTakenCheckpoint();
    }

    @Nullable
    synchronized KV<RestrictionT, KV<Instant, WatermarkEstimatorStateT>> getTakenCheckpoint() {
      // The checkpoint may or may not have been taken.
      return (checkpoint == null) ? null : KV.of(checkpoint, residualWatermarkAndState);
    }

    @Override
    public InputT element() {
      return element.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return sideInputReader.get(
          view,
          view.getWindowMappingFn()
              .getSideInputWindow(Iterables.getOnlyElement(element.getWindows())));
    }

    @Override
    public Instant timestamp() {
      return element.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return element.getPane();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputWithTimestamp(output, element.getTimestamp());
    }

    @Override
    public void outputWithTimestamp(OutputT value, Instant timestamp) {
      noteOutput();
      if (watermarkEstimator instanceof TimestampObservingWatermarkEstimator) {
        ((TimestampObservingWatermarkEstimator) watermarkEstimator).observeTimestamp(timestamp);
      }
      output.outputWindowedValue(value, timestamp, element.getWindows(), element.getPane());
    }

    @Override
    public <T> void output(TupleTag<T> tag, T value) {
      outputWithTimestamp(tag, value, element.getTimestamp());
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T value, Instant timestamp) {
      noteOutput();
      if (watermarkEstimator instanceof TimestampObservingWatermarkEstimator) {
        ((TimestampObservingWatermarkEstimator) watermarkEstimator).observeTimestamp(timestamp);
      }
      output.outputWindowedValue(tag, value, timestamp, element.getWindows(), element.getPane());
    }

    private void noteOutput() {
      checkState(!hasClaimFailed, "Output is not allowed after a failed tryClaim()");
      checkState(numClaimedBlocks > 0, "Output is not allowed before tryClaim()");
      ++numOutputs;
      if (numOutputs >= maxNumOutputs) {
        takeCheckpointNow();
      }
    }
  }
}
