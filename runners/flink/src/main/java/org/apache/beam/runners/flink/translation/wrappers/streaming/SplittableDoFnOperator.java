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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Flink operator for executing splittable {@link DoFn DoFns}. Specifically, for executing
 * the {@code @ProcessElement} method of a splittable {@link DoFn}.
 */
public class SplittableDoFnOperator<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT, ?>>
    extends DoFnOperator<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> {

  private transient ScheduledExecutorService executorService;

  public SplittableDoFnOperator(
      DoFn<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> doFn,
      String stepName,
      Coder<WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>>> inputCoder,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    super(
        doFn,
        stepName,
        inputCoder,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);
  }

  @Override
  protected DoFnRunner<
      KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> createWrappingDoFnRunner(
          DoFnRunner<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> wrappedRunner) {
    // don't wrap in anything because we don't need state cleanup because ProcessFn does
    // all that
    return wrappedRunner;
  }

  @Override
  public void open() throws Exception {
    super.open();

    checkState(doFn instanceof ProcessFn);

    StateInternalsFactory<String> stateInternalsFactory =
        key -> {
          // this will implicitly be keyed by the key of the incoming
          // element or by the key of a firing timer
          return (StateInternals) keyedStateInternals;
        };
    TimerInternalsFactory<String> timerInternalsFactory =
        key -> {
          // this will implicitly be keyed like the StateInternalsFactory
          return timerInternals;
        };

    executorService = Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory());

    ((ProcessFn) doFn).setStateInternalsFactory(stateInternalsFactory);
    ((ProcessFn) doFn).setTimerInternalsFactory(timerInternalsFactory);
    ((ProcessFn) doFn).setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            doFn,
            serializedOptions.get(),
            new OutputWindowedValue<OutputT>() {
              @Override
              public void outputWindowedValue(
                  OutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(
                    mainOutputTag,
                    WindowedValue.of(output, timestamp, windows, pane));
              }

              @Override
              public <AdditionalOutputT> void outputWindowedValue(
                  TupleTag<AdditionalOutputT> tag,
                  AdditionalOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
              }
            },
            sideInputReader,
            executorService,
            10000,
            Duration.standardSeconds(10)));
  }

  @Override
  public void fireTimer(InternalTimer<?, TimerInternals.TimerData> timer) {
    if (timer.getNamespace().getDomain().equals(TimeDomain.EVENT_TIME)) {
      // ignore this, it can only be a state cleanup timers from StatefulDoFnRunner and ProcessFn
      // does its own state cleanup and should never set event-time timers.
      return;
    }
    doFnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(
                (String) keyedStateInternals.getKey(),
                Collections.singletonList(timer.getNamespace()))));
  }

  @Override
  public void close() throws Exception {
    MetricsPusher.getInstance().pushMetrics();
    super.close();

    executorService.shutdown();

    long shutdownTimeout = Duration.standardSeconds(10).getMillis();
    try {
      if (!executorService.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.debug("The scheduled executor service did not properly terminate. Shutting "
            + "it down now.");
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.debug("Could not properly await the termination of the scheduled executor service.", e);
      executorService.shutdownNow();
    }
  }

}
