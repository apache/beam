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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowFn;
import org.apache.beam.runners.dataflow.worker.util.StreamingGroupAlsoByWindowFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A general {@link BatchGroupAlsoByWindowFn}. This delegates all of the logic to the {@link
 * ReduceFnRunner}.
 */
public class StreamingGroupAlsoByWindowViaWindowSetFn<K, InputT, OutputT, W extends BoundedWindow>
    extends StreamingGroupAlsoByWindowFn<K, InputT, OutputT> {

  public static <K, InputT, OutputT, W extends BoundedWindow>
      GroupAlsoByWindowFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> create(
          WindowingStrategy<?, W> strategy,
          StateInternalsFactory<K> stateInternalsFactory,
          SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    return new StreamingGroupAlsoByWindowViaWindowSetFn<>(
        strategy, stateInternalsFactory, reduceFn);
  }

  private final WindowingStrategy<Object, W> windowingStrategy;
  private final RunnerApi.Trigger triggerProto;
  private final StateInternalsFactory<K> stateInternalsFactory;
  private SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;

  private StreamingGroupAlsoByWindowViaWindowSetFn(
      WindowingStrategy<?, W> windowingStrategy,
      StateInternalsFactory<K> stateInternalsFactory,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = noWildcard;
    this.triggerProto = TriggerTranslation.toProto(windowingStrategy.getTrigger());
    this.reduceFn = reduceFn;
    this.stateInternalsFactory = stateInternalsFactory;
  }

  @Override
  public void processElement(
      KeyedWorkItem<K, InputT> keyedWorkItem,
      PipelineOptions options,
      StepContext stepContext,
      SideInputReader sideInputReader,
      OutputWindowedValue<KV<K, OutputT>> output)
      throws Exception {
    K key = keyedWorkItem.key();
    StateInternals stateInternals = stateInternalsFactory.stateInternalsForKey(key);

    ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner =
        new ReduceFnRunner<K, InputT, OutputT, W>(
            key,
            windowingStrategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(triggerProto)),
            stateInternals,
            stepContext.timerInternals(),
            output,
            sideInputReader,
            reduceFn,
            options);

    reduceFnRunner.processElements(keyedWorkItem.elementsIterable());
    reduceFnRunner.onTimers(keyedWorkItem.timersIterable());
    reduceFnRunner.persist();
  }

  public GroupAlsoByWindowFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> asDoFn() {
    // Safe contravariant cast
    @SuppressWarnings("unchecked")
    GroupAlsoByWindowFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> asFn =
        (GroupAlsoByWindowFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>>) this;
    return asFn;
  }
}
