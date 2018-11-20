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
package org.apache.beam.runners.samza.runtime;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/** This class wraps a DoFnRunner with keyed StateInternals and TimerInternals access. */
public class DoFnRunnerWithKeyedInternals<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> underlying;
  private final KeyedInternals keyedInternals;

  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> of(
      PipelineOptions options,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      DoFnRunners.OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      SamzaStoreStateInternals.Factory<?> stateInternalsFactory,
      SamzaTimerInternalsFactory<?> timerInternalsFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      SamzaMetricsContainer metricsContainer,
      String stepName) {

    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final KeyedInternals keyedInternals;
    final TimerInternals timerInternals;
    final StateInternals stateInternals;

    if (signature.usesState()) {
      keyedInternals = new KeyedInternals(stateInternalsFactory, timerInternalsFactory);
      stateInternals = keyedInternals.stateInternals();
      timerInternals = keyedInternals.timerInternals();
    } else {
      keyedInternals = null;
      stateInternals = stateInternalsFactory.stateInternalsForKey(null);
      timerInternals = timerInternalsFactory.timerInternalsForKey(null);
    }

    final DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            options,
            doFn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            createStepContext(stateInternals, timerInternals),
            inputCoder,
            outputCoders,
            windowingStrategy);

    final DoFnRunner<InputT, OutputT> doFnRunnerWithMetrics =
        DoFnRunnerWithMetrics.wrap(doFnRunner, metricsContainer, stepName);

    if (keyedInternals != null) {
      final DoFnRunner<InputT, OutputT> statefulDoFnRunner =
          DoFnRunners.defaultStatefulDoFnRunner(
              doFn,
              doFnRunnerWithMetrics,
              windowingStrategy,
              new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, windowingStrategy),
              createStateCleaner(doFn, windowingStrategy, keyedInternals.stateInternals()));

      return new DoFnRunnerWithKeyedInternals<>(statefulDoFnRunner, keyedInternals);
    } else {
      return doFnRunnerWithMetrics;
    }
  }

  /** Creates a {@link StepContext} that allows accessing state and timer internals. */
  private static StepContext createStepContext(
      StateInternals stateInternals, TimerInternals timerInternals) {
    return new StepContext() {
      @Override
      public StateInternals stateInternals() {
        return stateInternals;
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals;
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <InputT, OutputT> StatefulDoFnRunner.StateCleaner<?> createStateCleaner(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      StateInternals stateInternals) {
    final TypeDescriptor windowType = windowingStrategy.getWindowFn().getWindowTypeDescriptor();
    if (windowType.isSubtypeOf(TypeDescriptor.of(BoundedWindow.class))) {
      final Coder<? extends BoundedWindow> windowCoder =
          windowingStrategy.getWindowFn().windowCoder();
      return new StatefulDoFnRunner.StateInternalsStateCleaner<>(doFn, stateInternals, windowCoder);
    } else {
      return null;
    }
  }

  private DoFnRunnerWithKeyedInternals(
      DoFnRunner<InputT, OutputT> doFnRunner, KeyedInternals keyedInternals) {
    this.underlying = doFnRunner;
    this.keyedInternals = keyedInternals;
  }

  @Override
  public void startBundle() {
    underlying.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    // NOTE: this is thread-safe if we only allow concurrency on the per-key basis.
    setKeyedInternals(elem.getValue());

    try {
      underlying.processElement(elem);
    } finally {
      clearKeyedInternals();
    }
  }

  public void onTimer(KeyedTimerData keyedTimerData, BoundedWindow window) {
    setKeyedInternals(keyedTimerData);

    try {
      final TimerInternals.TimerData timer = keyedTimerData.getTimerData();
      onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
    } finally {
      clearKeyedInternals();
    }
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    checkState(keyedInternals.getKey() != null, "Key is not set for timer");

    underlying.onTimer(timerId, window, timestamp, timeDomain);
  }

  @Override
  public void finishBundle() {
    underlying.finishBundle();
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  @SuppressWarnings("unchecked")
  private void setKeyedInternals(Object value) {
    if (value instanceof KeyedWorkItem) {
      keyedInternals.setKey(((KeyedWorkItem<?, ?>) value).key());
    } else if (value instanceof KeyedTimerData) {
      final Object key = ((KeyedTimerData) value).getKey();
      if (key != null) {
        keyedInternals.setKey(key);
      }
    } else {
      keyedInternals.setKey(((KV<?, ?>) value).getKey());
    }
  }

  private void clearKeyedInternals() {
    keyedInternals.clearKey();
  }
}
