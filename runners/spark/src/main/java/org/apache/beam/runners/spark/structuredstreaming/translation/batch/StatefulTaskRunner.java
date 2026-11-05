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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Manages the task scoped lifecycle of a stateful {@link DoFnRunner} and its {@link StepContext}.
 */
@Internal
public class StatefulTaskRunner<InT, T> implements Serializable {

  private transient @Nullable DoFnRunnerWithTeardown<InT, ?> runner;
  private transient @Nullable MutableStepContext stepContext;
  private transient boolean isTornDown;

  public <R extends DoFnRunnerWithTeardown<InT, ?>> R getOrCreate(
      Function<MutableStepContext, R> createRunner) {
    DoFnRunnerWithTeardown<InT, ?> r = runner;
    if (r == null) {
      MutableStepContext ctx = new MutableStepContext();
      this.stepContext = ctx;
      R created = createRunner.apply(ctx);
      this.runner = created;
      TaskContext taskContext = TaskContext.get();
      if (taskContext != null) {
        taskContext.addTaskCompletionListener(
            new TaskCompletionListener() {
              @Override
              public void onTaskCompletion(TaskContext context) {
                teardownOnce();
              }
            });
      }
      return created;
    }
    @SuppressWarnings("unchecked")
    R existing = (R) r;
    return existing;
  }

  public MutableStepContext stepContext() {
    MutableStepContext ctx = stepContext;
    if (ctx == null) {
      throw new IllegalStateException("StepContext requested before the runner was created");
    }
    return ctx;
  }

  public void teardownOnce() {
    DoFnRunnerWithTeardown<InT, ?> r = runner;
    if (r != null && !isTornDown) {
      isTornDown = true;
      r.teardown();
    }
  }

  public static void fireTimer(DoFnRunner<?, ?> runner, @Nullable Object key, TimerData timer) {
    BoundedWindow window = ((StateNamespaces.WindowNamespace<?>) timer.getNamespace()).getWindow();
    runner.onTimer(
        timer.getTimerId(),
        timer.getTimerFamilyId(),
        key,
        window,
        timer.getTimestamp(),
        timer.getOutputTimestamp(),
        timer.getDomain(),
        CausedByDrain.NORMAL);
  }

  /** A mutable step context that can rebind state and timers per key. */
  @Internal
  public static class MutableStepContext implements StepContext {
    private @Nullable StateInternals stateInternals;
    private @Nullable TimerInternals timerInternals;

    public void reset(@Nullable Object key) {
      this.stateInternals = InMemoryStateInternals.forKey(key);
      this.timerInternals = new InMemoryTimerInternals();
    }

    public void set(StateInternals stateInternals, TimerInternals timerInternals) {
      this.stateInternals = stateInternals;
      this.timerInternals = timerInternals;
    }

    public InMemoryTimerInternals timers() {
      TimerInternals timers = timerInternals();
      if (timers instanceof InMemoryTimerInternals) {
        return (InMemoryTimerInternals) timers;
      }
      throw new IllegalStateException(
          "Expected InMemoryTimerInternals but was " + timers.getClass().getName());
    }

    @Override
    public StateInternals stateInternals() {
      StateInternals state = stateInternals;
      if (state == null) {
        throw new IllegalStateException("StepContext used before reset");
      }
      return state;
    }

    @Override
    public TimerInternals timerInternals() {
      TimerInternals timers = timerInternals;
      if (timers == null) {
        throw new IllegalStateException("StepContext used before reset");
      }
      return timers;
    }
  }
}
