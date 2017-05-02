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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.construction.Triggers;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.joda.time.Instant;

/**
 * The default batch {@link GroupAlsoByWindowsDoFn} implementation, if no specialized "fast path"
 * implementation is applicable.
 */
@SystemDoFnInternal
public class GroupAlsoByWindowViaOutputBufferDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

  private final WindowingStrategy<?, W> strategy;
  private final StateInternalsFactory<K> stateInternalsFactory;
  private SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;

  public GroupAlsoByWindowViaOutputBufferDoFn(
      WindowingStrategy<?, W> windowingStrategy,
      StateInternalsFactory<K> stateInternalsFactory,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    this.strategy = windowingStrategy;
    this.reduceFn = reduceFn;
    this.stateInternalsFactory = stateInternalsFactory;
  }

  @Override
  public void processElement(ProcessContext c) throws Exception {
    K key = c.element().getKey();
    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and emulate the
    // watermark, knowing that we have all data and it is in timestamp order.
    InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
    timerInternals.advanceProcessingTime(Instant.now());
    timerInternals.advanceSynchronizedProcessingTime(Instant.now());
    StateInternals stateInternals = stateInternalsFactory.stateInternalsForKey(key);

    ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner =
        new ReduceFnRunner<>(
            key,
            strategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(
                    Triggers.toProto(strategy.getTrigger()))),
            stateInternals,
            timerInternals,
            WindowingInternalsAdapters.outputWindowedValue(c.windowingInternals()),
            WindowingInternalsAdapters.sideInputReader(c.windowingInternals()),
            reduceFn,
            c.getPipelineOptions());

    // Process the elements.
    reduceFnRunner.processElements(c.element().getValue());

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

    fireEligibleTimers(timerInternals, reduceFnRunner);

    reduceFnRunner.persist();
  }

  private void fireEligibleTimers(
      InMemoryTimerInternals timerInternals, ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner)
      throws Exception {
    List<TimerInternals.TimerData> timers = new ArrayList<>();
    while (true) {
      TimerInternals.TimerData timer;
      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      reduceFnRunner.onTimers(timers);
      timers.clear();
    }
  }
}
