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
package org.apache.beam.runners.flink.translation.functions;

import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link RichGroupReduceFunction} for splittable {@link DoFn} in Flink Batch Runner.
 */
public class FlinkSplittableDoFnFunction<K, V, OutputT>
    extends RichGroupReduceFunction<WindowedValue<KeyedWorkItem<K, V>>, WindowedValue<OutputT>> {

  private final DoFn<KeyedWorkItem<K, V>, OutputT> dofn;
  private String stepName;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;
  private final SerializablePipelineOptions serializedOptions;
  private final Map<TupleTag<?>, Integer> outputMap;
  private final TupleTag<OutputT> mainOutputTag;
  private transient DoFnInvoker doFnInvoker;

  public FlinkSplittableDoFnFunction(
      DoFn<KeyedWorkItem<K, V>, OutputT> dofn,
      String stepName,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions,
      Map<TupleTag<?>, Integer> outputMap,
      TupleTag<OutputT> mainOutputTag) {

    this.dofn = dofn;
    this.stepName = stepName;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
    this.outputMap = outputMap;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KeyedWorkItem<K, V>>> values,
      Collector<WindowedValue<OutputT>> out) throws Exception {
    RuntimeContext runtimeContext = getRuntimeContext();

    DoFnRunners.OutputManager outputManager;
    if (outputMap.size() == 1) {
      outputManager = new FlinkDoFnFunction.DoFnOutputManager(out);
    } else {
      // it has some additional Outputs
      outputManager =
          new FlinkDoFnFunction.MultiDoFnOutputManager((Collector) out, outputMap);
    }

    final Iterator<WindowedValue<KeyedWorkItem<K, V>>> iterator = values.iterator();

    // get the first value, we need this for initializing the state internals with the key.
    // we are guaranteed to have a first value, otherwise reduce() would not have been called.
    WindowedValue<KeyedWorkItem<K, V>> currentValue = iterator.next();
    final K key = currentValue.getValue().key();

    final InMemoryStateInternals<K> stateInternals = InMemoryStateInternals.forKey(key);

    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and advance
    // time to the end after processing all elements.
    final InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
    timerInternals.advanceProcessingTime(Instant.now());
    timerInternals.advanceSynchronizedProcessingTime(Instant.now());

    FlinkSideInputReader sideInputReader = new FlinkSideInputReader(
        sideInputs,
        runtimeContext);

    List<TupleTag<?>> additionalOutputTags = Lists.newArrayList(outputMap.keySet());

    StateInternalsFactory<String> stateInternalsFactory =
        k -> {
          // this will implicitly be keyed by the key of the incoming
          // element or by the key of a firing timer
          return (StateInternals) stateInternals;
        };
    TimerInternalsFactory<String> timerInternalsFactory =
        k -> {
          // this will implicitly be keyed like the StateInternalsFactory
          return timerInternals;
        };

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
        Executors.defaultThreadFactory());

    ((SplittableParDoViaKeyedWorkItems.ProcessFn) dofn)
        .setStateInternalsFactory(stateInternalsFactory);

    ((SplittableParDoViaKeyedWorkItems.ProcessFn) dofn)
        .setTimerInternalsFactory(timerInternalsFactory);

    ((SplittableParDoViaKeyedWorkItems.ProcessFn) dofn).setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            dofn,
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

    DoFnRunner<KeyedWorkItem<K, V>, OutputT> doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.get(), dofn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        new FlinkNoOpStepContext() {
          @Override
          public StateInternals stateInternals() {
            return stateInternals;
          }
          @Override
          public TimerInternals timerInternals() {
            return timerInternals;
          }
        },
        windowingStrategy);

    if ((serializedOptions.get().as(FlinkPipelineOptions.class))
        .getEnableMetrics()) {
      doFnRunner = new DoFnRunnerWithMetricsUpdate<>(stepName, doFnRunner, getRuntimeContext());
    }

    doFnRunner.startBundle();

    doFnRunner.processElement(currentValue);
    while (iterator.hasNext()) {
      currentValue = iterator.next();
      doFnRunner.processElement(currentValue);
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

    fireEligibleTimers(key, timerInternals, doFnRunner);

    doFnRunner.finishBundle();

    executorService.shutdown();
  }

  private void fireEligibleTimers(
      K key,
      InMemoryTimerInternals timerInternals,
      DoFnRunner<KeyedWorkItem<K, V>, OutputT> runner)
      throws Exception {

    while ((timerInternals.getNextTimer(TimeDomain.PROCESSING_TIME) != null)
        || (timerInternals.getNextTimer(TimeDomain.SYNCHRONIZED_PROCESSING_TIME) != null)
        || (timerInternals.getNextTimer(TimeDomain.EVENT_TIME) != null)) {

      timerInternals.advanceProcessingTime(Instant.now());
      timerInternals.advanceSynchronizedProcessingTime(Instant.now());

      TimerInternals.TimerData timer;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        fireTimer(key, timer, runner);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        fireTimer(key, timer, runner);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        fireTimer(key, timer, runner);
      }
    }
  }

  private void fireTimer(
      K key,
      TimerInternals.TimerData timer,
      DoFnRunner<KeyedWorkItem<K, V>, OutputT> doFnRunner) {
    doFnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(
                key,
                Collections.singletonList(timer))));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    doFnInvoker = DoFnInvokers.invokerFor(dofn);
    doFnInvoker.invokeSetup();
  }

  @Override
  public void close() throws Exception {
    doFnInvoker.invokeTeardown();
  }

}
