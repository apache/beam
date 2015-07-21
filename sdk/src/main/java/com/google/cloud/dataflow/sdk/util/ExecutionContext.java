/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Context for the current execution. This is guaranteed to exist during processing,
 * but does not necessarily persist between different batches of work.
 */
public abstract class ExecutionContext {
  private Map<String, StepContext> cachedStepContexts = new HashMap<>();

  /**
   * Returns the {@link StepContext} associated with the given step.
   */
  public StepContext getStepContext(String stepName) {
    StepContext context = cachedStepContexts.get(stepName);
    if (context == null) {
      context = createStepContext(stepName);
      cachedStepContexts.put(stepName, context);
    }
    return context;
  }

  /**
   * Returns a collection view of all of the {@link StepContext}s.
   */
  public Collection<StepContext> getAllStepContexts() {
    return cachedStepContexts.values();
  }

  /**
   * Implementations should override this to create the specific type
   * of {@link StepContext} they neeed.
   */
  public abstract StepContext createStepContext(String stepName);

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#output}
   * is called.
   */
  public void noteOutput(WindowedValue<?> output) {}

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#sideOutput}
   * is called.
   */
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {}

  /**
   * Writes the given {@link PCollectionView} data to a globally accessible location.
   */
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
      W window, Coder<W> windowCoder) throws IOException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  /**
   * Per-step, per-key context used for retrieving state.
   */
  public abstract class StepContext {
    private final String stepName;

    public StepContext(String stepName) {
      this.stepName = stepName;
    }

    public String getStepName() {
      return stepName;
    }

    public ExecutionContext getExecutionContext() {
      return ExecutionContext.this;
    }

    public void noteOutput(WindowedValue<?> output) {
      ExecutionContext.this.noteOutput(output);
    }

    public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {
      ExecutionContext.this.noteSideOutput(tag, output);
    }

    public abstract StateInternals stateInternals();
    public abstract TimerInternals timerInternals();
  }
}
