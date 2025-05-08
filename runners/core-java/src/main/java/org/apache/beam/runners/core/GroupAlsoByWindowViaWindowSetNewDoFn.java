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

import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A general {@link GroupAlsoByWindowsAggregators}. This delegates all of the logic to the {@link
 * ReduceFnRunner}.
 */
@SystemDoFnInternal
public class GroupAlsoByWindowViaWindowSetNewDoFn<
        K, InputT, OutputT, W extends BoundedWindow, RinT extends KeyedWorkItem<K, InputT>>
    extends DoFn<RinT, KV<K, OutputT>> {

  private static final long serialVersionUID = 1L;
  private final RunnerApi.Trigger triggerProto;

  public static <K, InputT, OutputT, W extends BoundedWindow>
      DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> create(
          WindowingStrategy<?, W> strategy,
          StateInternalsFactory<K> stateInternalsFactory,
          TimerInternalsFactory<K> timerInternalsFactory,
          SideInputReader sideInputReader,
          SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn,
          DoFnRunners.OutputManager outputManager,
          TupleTag<KV<K, OutputT>> mainTag) {
    return new GroupAlsoByWindowViaWindowSetNewDoFn<>(
        strategy,
        stateInternalsFactory,
        timerInternalsFactory,
        sideInputReader,
        reduceFn,
        outputManager,
        mainTag);
  }

  private final WindowingStrategy<Object, W> windowingStrategy;
  private SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;
  private transient StateInternalsFactory<K> stateInternalsFactory;
  private transient TimerInternalsFactory<K> timerInternalsFactory;
  private transient SideInputReader sideInputReader;
  private transient DoFnRunners.OutputManager outputManager;
  private TupleTag<KV<K, OutputT>> mainTag;

  public GroupAlsoByWindowViaWindowSetNewDoFn(
      WindowingStrategy<?, W> windowingStrategy,
      StateInternalsFactory<K> stateInternalsFactory,
      TimerInternalsFactory<K> timerInternalsFactory,
      SideInputReader sideInputReader,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn,
      DoFnRunners.OutputManager outputManager,
      TupleTag<KV<K, OutputT>> mainTag) {
    this.timerInternalsFactory = timerInternalsFactory;
    this.sideInputReader = sideInputReader;
    this.outputManager = outputManager;
    this.mainTag = mainTag;
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = noWildcard;
    this.reduceFn = reduceFn;
    this.stateInternalsFactory = stateInternalsFactory;
    this.triggerProto = TriggerTranslation.toProto(windowingStrategy.getTrigger());
  }

  private OutputWindowedValue<KV<K, OutputT>> outputWindowedValue() {
    return new OutputWindowedValue<KV<K, OutputT>>() {
      @Override
      public void outputWindowedValue(
          KV<K, OutputT> output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        outputManager.output(mainTag, WindowedValue.of(output, timestamp, windows, pane));
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
    };
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    KeyedWorkItem<K, InputT> keyedWorkItem = c.element();

    K key = keyedWorkItem.key();
    StateInternals stateInternals = stateInternalsFactory.stateInternalsForKey(key);
    TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey(key);

    ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner =
        new ReduceFnRunner<>(
            key,
            windowingStrategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(triggerProto)),
            stateInternals,
            timerInternals,
            outputWindowedValue(),
            sideInputReader,
            reduceFn,
            c.getPipelineOptions());

    reduceFnRunner.processElements(keyedWorkItem.elementsIterable());
    reduceFnRunner.onTimers(keyedWorkItem.timersIterable());
    reduceFnRunner.persist();
  }
}
