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

import static org.apache.beam.runners.core.TimerInternals.TimerData;

import java.util.*;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.java.functions.KeySelector;

/** Flink operator for executing window {@link DoFn DoFns}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindowDoFnOperator<K, InputT, OutputT>
    extends DoFnOperator<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, OutputT>> {

  private final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn;
  private Map<K, StateInternals> stateInternals;
  private Map<K, InMemoryTimerInternals> inMemTimerInternals;

  public WindowDoFnOperator(
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn,
      String stepName,
      Coder<WindowedValue<KeyedWorkItem<K, InputT>>> windowedInputCoder,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<KV<K, OutputT>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<K> keyCoder,
      KeySelector<WindowedValue<KeyedWorkItem<K, InputT>>, ?> keySelector) {
    super(
        null,
        stepName,
        windowedInputCoder,
        Collections.emptyMap(),
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder,
        keySelector,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());

    this.systemReduceFn = systemReduceFn;
  }

  @Override
  public void open() throws Exception {
    stateInternals = new HashMap<>();
    inMemTimerInternals = new HashMap<>();
    super.open();
  }

  @Override
  protected Iterable<WindowedValue<KeyedWorkItem<K, InputT>>> preProcess(
      WindowedValue<KV<K, InputT>> inWithMultipleWindows) {
    // we need to wrap each one work item per window for now
    // since otherwise the PushbackSideInputRunner will not correctly
    // determine whether side inputs are ready
    //
    // this is tracked as https://github.com/apache/beam/issues/18358
    ArrayList<WindowedValue<KeyedWorkItem<K, InputT>>> inputs = new ArrayList<>();
    for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
      SingletonKeyedWorkItem<K, InputT> workItem =
          new SingletonKeyedWorkItem<>(
              in.getValue().getKey(), in.withValue(in.getValue().getValue()));

      inputs.add(in.withValue(workItem));
    }
    return inputs;
  }

  @Override
  protected DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> createWrappingDoFnRunner(
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> wrappedRunner, StepContext stepContext) {
    // When the doFn is this, we know it came from WindowDoFnOperator and
    //   InputT = KeyedWorkItem<K, V>
    //   OutputT = KV<K, V>
    //
    // for some K, V

    if (timerInternals == null) {
      return doFnRunner;
    }

    return DoFnRunners.lateDataDroppingRunner(
        (DoFnRunner) doFnRunner, timerInternals, windowingStrategy);
  }

  @Override
  protected DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> getDoFn() {
    StateInternalsFactory<K> stateInternalsFactory;
    if (keyedStateInternals != null) {
      // this will implicitly be keyed by the key of the incoming
      // element or by the key of a firing timer
      stateInternalsFactory = key -> (StateInternals) keyedStateInternals;
    } else {
      stateInternalsFactory =
          key -> stateInternals.computeIfAbsent(key, k -> InMemoryStateInternals.forKey(k));
    }

    TimerInternalsFactory<K> timerInternalsFactory;
    if (timerInternals != null) {
      // this will implicitly be keyed like the StateInternalsFactory
      timerInternalsFactory = key -> timerInternals;
    } else {
      timerInternalsFactory =
          key -> inMemTimerInternals.computeIfAbsent(key, k -> new InMemoryTimerInternals());
    }

    // we have to do the unchecked cast because GroupAlsoByWindowViaWindowSetDoFn.create
    // has the window type as generic parameter while WindowingStrategy is almost always
    // untyped.
    @SuppressWarnings("unchecked")
    DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFn =
        GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy,
            stateInternalsFactory,
            timerInternalsFactory,
            sideInputReader,
            (SystemReduceFn) systemReduceFn,
            outputManager,
            mainOutputTag);
    return doFn;
  }

  @Override
  void flushData() throws Exception {
    for (Map.Entry<K, InMemoryTimerInternals> entry : inMemTimerInternals.entrySet()) {
      K key = entry.getKey();
      InMemoryTimerInternals timer = entry.getValue();
      timer.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
      timer.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
      timer.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

      TimerData timerData = timer.removeNextEventTimer();
      while (timerData != null) {
        doFnRunner.processElement(
            WindowedValue.valueInGlobalWindow(
                KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timerData))));
        timerData = timer.removeNextEventTimer();
      }

      timerData = timer.removeNextProcessingTimer();
      while (timerData != null) {
        doFnRunner.processElement(
            WindowedValue.valueInGlobalWindow(
                KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timerData))));
        timerData = timer.removeNextProcessingTimer();
      }

      timerData = timer.removeNextSynchronizedProcessingTimer();
      while (timerData != null) {
        doFnRunner.processElement(
            WindowedValue.valueInGlobalWindow(
                KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timerData))));
        timerData = timer.removeNextSynchronizedProcessingTimer();
      }
    }
    super.flushData();
  }

  @Override
  protected void fireTimer(TimerData timer) {
    timerInternals.onFiredOrDeletedTimer(timer);
    doFnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(
                (K) keyedStateInternals.getKey(), Collections.singletonList(timer))));
  }
}
