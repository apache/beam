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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link SplittableProcessElementInvoker} that requests a checkpoint after the {@link
 * DoFn.ProcessElement} call either outputs at least a given number of elements (in total over all
 * outputs), or runs for the given duration.
 */
public class OutputAndTimeBoundedSplittableProcessElementInvoker<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT> {
  private final DoFn<InputT, OutputT> fn;
  private final PipelineOptions pipelineOptions;
  private final OutputWindowedValue<OutputT> output;
  private final SideInputReader sideInputReader;
  private final ScheduledExecutorService executor;
  private final int maxNumOutputs;
  private final Duration maxDuration;

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
   * @param maxDuration Maximum duration of the {@link DoFn.ProcessElement} call after which a
   *     checkpoint will be requested. This is a best-effort request - the {@link DoFn} may run for
   *     longer after receiving the request.
   */
  public OutputAndTimeBoundedSplittableProcessElementInvoker(
      DoFn<InputT, OutputT> fn,
      PipelineOptions pipelineOptions,
      OutputWindowedValue<OutputT> output,
      SideInputReader sideInputReader,
      ScheduledExecutorService executor,
      int maxNumOutputs,
      Duration maxDuration) {
    this.fn = fn;
    this.pipelineOptions = pipelineOptions;
    this.output = output;
    this.sideInputReader = sideInputReader;
    this.executor = executor;
    this.maxNumOutputs = maxNumOutputs;
    this.maxDuration = maxDuration;
  }

  @Override
  public Result invokeProcessElement(
      DoFnInvoker<InputT, OutputT> invoker,
      final WindowedValue<InputT> element,
      final TrackerT tracker) {
    final ProcessContext processContext = new ProcessContext(element, tracker);
    invoker.invokeProcessElement(
        new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
          @Override
          public DoFn<InputT, OutputT>.ProcessContext processContext(
              DoFn<InputT, OutputT> doFn) {
            return processContext;
          }

          @Override
          public RestrictionTracker<?> restrictionTracker() {
            return tracker;
          }

          // Unsupported methods below.

          @Override
          public BoundedWindow window() {
            throw new UnsupportedOperationException(
                "Access to window of the element not supported in Splittable DoFn");
          }

          @Override
          public DoFn<InputT, OutputT>.Context context(DoFn<InputT, OutputT> doFn) {
            throw new IllegalStateException(
                "Should not access context() from @"
                    + DoFn.ProcessElement.class.getSimpleName());
          }

          @Override
          public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(
              DoFn<InputT, OutputT> doFn) {
            throw new UnsupportedOperationException(
                "Access to timers not supported in Splittable DoFn");
          }

          @Override
          public State state(String stateId) {
            throw new UnsupportedOperationException(
                "Access to state not supported in Splittable DoFn");
          }

          @Override
          public Timer timer(String timerId) {
            throw new UnsupportedOperationException(
                "Access to timers not supported in Splittable DoFn");
          }
        });

    tracker.checkDone();
    return new Result(
        processContext.extractCheckpoint(), processContext.getLastReportedWatermark());
  }

  private class ProcessContext extends DoFn<InputT, OutputT>.ProcessContext {
    private final WindowedValue<InputT> element;
    private final TrackerT tracker;

    private int numOutputs;
    // Checkpoint may be initiated either when the given number of outputs is reached,
    // or when the call runs for the given duration. It must be initiated at most once,
    // even if these events happen almost at the same time.
    // This is either the result of the sole tracker.checkpoint() call, or null if
    // the call completed before reaching the given number of outputs or duration.
    private RestrictionT checkpoint;
    // A handle on the scheduled action to take a checkpoint.
    private Future<?> scheduledCheckpoint;
    private Instant lastReportedWatermark;

    public ProcessContext(WindowedValue<InputT> element, TrackerT tracker) {
      fn.super();
      this.element = element;
      this.tracker = tracker;

      this.scheduledCheckpoint =
          executor.schedule(
              new Runnable() {
                @Override
                public void run() {
                  initiateCheckpoint();
                }
              },
              maxDuration.getMillis(),
              TimeUnit.MILLISECONDS);
    }

    @Nullable
    RestrictionT extractCheckpoint() {
      scheduledCheckpoint.cancel(true);
      try {
        Futures.getUnchecked(scheduledCheckpoint);
      } catch (CancellationException e) {
        // This is expected if the call took less than the maximum duration.
      }
      // By now, a checkpoint may or may not have been taken;
      // via .output() or via scheduledCheckpoint.
      synchronized (this) {
        return checkpoint;
      }
    }

    private synchronized void initiateCheckpoint() {
      // This method may be entered either via .output(), or via scheduledCheckpoint.
      // Only one of them "wins" - tracker.checkpoint() must be called only once.
      if (checkpoint == null) {
        checkpoint = checkNotNull(tracker.checkpoint());
      }
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
    public synchronized void updateWatermark(Instant watermark) {
      lastReportedWatermark = watermark;
    }

    public synchronized Instant getLastReportedWatermark() {
      return lastReportedWatermark;
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
      output.outputWindowedValue(value, timestamp, element.getWindows(), element.getPane());
      noteOutput();
    }

    @Override
    public <T> void output(TupleTag<T> tag, T value) {
      outputWithTimestamp(tag, value, element.getTimestamp());
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T value, Instant timestamp) {
      output.outputWindowedValue(
          tag, value, timestamp, element.getWindows(), element.getPane());
      noteOutput();
    }

    private void noteOutput() {
      ++numOutputs;
      if (numOutputs >= maxNumOutputs) {
        initiateCheckpoint();
      }
    }
  }
}
