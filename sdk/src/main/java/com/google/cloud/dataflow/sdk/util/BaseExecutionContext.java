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
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for implementations of {@link ExecutionContext}.
 *
 * <p> A concrete subclass should implement {@link #createStepContext} to create the appropriate
 * {@link ExecutionContext.StepContext} implementation. Any {@code StepContext} created will
 * be cached for the lifetime of this {@link ExecutionContext}.
 */
public abstract class BaseExecutionContext implements ExecutionContext {

  private Map<String, ExecutionContext.StepContext> cachedStepContexts = new HashMap<>();

  /**
   * Implementations should override this to create the specific type
   * of {@link StepContext} they need.
   */
  protected abstract ExecutionContext.StepContext createStepContext(
      String stepName, String transformName);


  /**
   * Returns the {@link StepContext} associated with the given step.
   */
  @Override
  public ExecutionContext.StepContext getStepContext(String stepName, String transformName) {
    ExecutionContext.StepContext context = cachedStepContexts.get(stepName);
    if (context == null) {
      context = createStepContext(stepName, transformName);
      cachedStepContexts.put(stepName, context);
    }
    return context;
  }

  /**
   * Returns a collection view of all of the {@link StepContext}s.
   */
  @Override
  public Collection<ExecutionContext.StepContext> getAllStepContexts() {
    return cachedStepContexts.values();
  }

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#output}
   * is called.
   */
  @Override
  public void noteOutput(WindowedValue<?> output) {}

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#sideOutput}
   * is called.
   */
  @Override
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {}

  /**
   * Base class for implementations of {@link ExecutionContext.StepContext}.
   *
   * <p> To complete a concrete subclass, implement {@link #timerInternals} and
   * {@link #stateInternals}.
   */
  public abstract static class StepContext implements ExecutionContext.StepContext {
    private final ExecutionContext executionContext;
    private final String stepName;
    private final String transformName;

    public StepContext(ExecutionContext executionContext, String stepName, String transformName) {
      this.executionContext = executionContext;
      this.stepName = stepName;
      this.transformName = transformName;
    }

    @Override
    public String getStepName() {
      return stepName;
    }

    @Override
    public String getTransformName() {
      return transformName;
    }

    @Override
    public ExecutionContext getExecutionContext() {
      return executionContext;
    }

    @Override
    public void noteOutput(WindowedValue<?> output) {
      executionContext.noteOutput(output);
    }

    @Override
    public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {
      executionContext.noteSideOutput(tag, output);
    }

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window, Coder<W> windowCoder) throws IOException {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public abstract StateInternals stateInternals();

    @Override
    public abstract TimerInternals timerInternals();
  }
}
