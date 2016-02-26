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
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.io.IOException;
import java.util.Collection;

/**
 * Context for the current execution. This is guaranteed to exist during processing,
 * but does not necessarily persist between different batches of work.
 */
public interface ExecutionContext {
  /**
   * Returns the {@link StepContext} associated with the given step.
   */
  StepContext getOrCreateStepContext(
      String stepName, String transformName, StateSampler stateSampler);

  /**
   * Returns a collection view of all of the {@link StepContext}s.
   */
  Collection<? extends StepContext> getAllStepContexts();

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#output}
   * is called.
   */
  void noteOutput(WindowedValue<?> output);

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#sideOutput}
   * is called.
   */
  void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output);

  /**
   * Per-step, per-key context used for retrieving state.
   */
  public interface StepContext {

    /**
     * The name of the step.
     */
    String getStepName();

    /**
     * The name of the transform for the step.
     */
    String getTransformName();

    /**
     * Hook for subclasses to implement that will be called whenever
     * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#output}
     * is called.
     */
    void noteOutput(WindowedValue<?> output);

    /**
     * Hook for subclasses to implement that will be called whenever
     * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#sideOutput}
     * is called.
     */
    void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output);

    /**
     * Writes the given {@code PCollectionView} data to a globally accessible location.
     */
    <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data,
        Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window,
        Coder<W> windowCoder)
            throws IOException;

    StateInternals<?> stateInternals();

    TimerInternals timerInternals();
  }
}
