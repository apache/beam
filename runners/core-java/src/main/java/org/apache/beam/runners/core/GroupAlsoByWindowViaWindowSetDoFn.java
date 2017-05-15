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

import org.apache.beam.runners.core.construction.Triggers;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A general {@link GroupAlsoByWindowsDoFn}. This delegates all of the logic to the
 * {@link ReduceFnRunner}.
 */
@SystemDoFnInternal
public class GroupAlsoByWindowViaWindowSetDoFn<
        K, InputT, OutputT, W extends BoundedWindow, RinT extends KeyedWorkItem<K, InputT>>
    extends OldDoFn<RinT, KV<K, OutputT>> {

  public static <K, InputT, OutputT, W extends BoundedWindow>
      OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> create(
          WindowingStrategy<?, W> strategy,
          StateInternalsFactory<K> stateInternalsFactory,
          SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    return new GroupAlsoByWindowViaWindowSetDoFn<>(strategy, stateInternalsFactory, reduceFn);
  }

  private final WindowingStrategy<Object, W> windowingStrategy;
  private final StateInternalsFactory<K> stateInternalsFactory;
  private SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;

  private GroupAlsoByWindowViaWindowSetDoFn(
      WindowingStrategy<?, W> windowingStrategy,
      StateInternalsFactory<K> stateInternalsFactory,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = noWildcard;
    this.reduceFn = reduceFn;
    this.stateInternalsFactory = stateInternalsFactory;
  }

  @Override
  public void processElement(ProcessContext c) throws Exception {
    KeyedWorkItem<K, InputT> keyedWorkItem = c.element();

    K key = keyedWorkItem.key();
    TimerInternals timerInternals = c.windowingInternals().timerInternals();
    StateInternals stateInternals = stateInternalsFactory.stateInternalsForKey(key);

    ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner =
        new ReduceFnRunner<>(
            key,
            windowingStrategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(
                    Triggers.toProto(windowingStrategy.getTrigger()))),
            stateInternals,
            timerInternals,
            WindowingInternalsAdapters.outputWindowedValue(c.windowingInternals()),
            WindowingInternalsAdapters.sideInputReader(c.windowingInternals()),
            reduceFn,
            c.getPipelineOptions());

    reduceFnRunner.processElements(keyedWorkItem.elementsIterable());
    reduceFnRunner.onTimers(keyedWorkItem.timersIterable());
    reduceFnRunner.persist();
  }

  public OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> asDoFn() {
    // Safe contravariant cast
    @SuppressWarnings("unchecked")
    OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> asFn =
        (OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>>) this;
    return asFn;
  }
}
