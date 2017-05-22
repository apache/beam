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
package org.apache.beam.runners.direct;

import java.io.IOException;
import org.apache.beam.runners.core.BaseExecutionContext;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Execution Context for the {@link DirectRunner}.
 *
 * <p>This implementation is not thread safe. A new {@link DirectExecutionContext} must be created
 * for each thread that requires it.
 */
class DirectExecutionContext
    extends BaseExecutionContext<DirectStepContext> {
  private final Clock clock;
  private final StructuralKey<?> key;
  private final CopyOnAccessInMemoryStateInternals existingState;
  private final TransformWatermarks watermarks;

  public DirectExecutionContext(
      Clock clock,
      StructuralKey<?> key,
      CopyOnAccessInMemoryStateInternals existingState,
      TransformWatermarks watermarks) {
    this.clock = clock;
    this.key = key;
    this.existingState = existingState;
    this.watermarks = watermarks;
  }

  @Override
  protected DirectStepContext createStepContext(String stepName, String transformName) {
    return new DirectStepContext(this, stepName, transformName);
  }

  /**
   * Step Context for the {@link DirectRunner}.
   */
  public class DirectStepContext implements StepContext {
    private CopyOnAccessInMemoryStateInternals<?> stateInternals;
    private DirectTimerInternals timerInternals;
    private final String stepName;
    private final String transformName;

    public DirectStepContext(
        ExecutionContext executionContext, String stepName, String transformName) {
      this.stepName = stepName;
      this.transformName = transformName;
    }

    @Override
    public CopyOnAccessInMemoryStateInternals<?> stateInternals() {
      if (stateInternals == null) {
        stateInternals = CopyOnAccessInMemoryStateInternals.withUnderlying(key, existingState);
      }
      return stateInternals;
    }

    @Override
    public DirectTimerInternals timerInternals() {
      if (timerInternals == null) {
        timerInternals =
            DirectTimerInternals.create(clock, watermarks, TimerUpdate.builder(key));
      }
      return timerInternals;
    }

    /**
     * Commits the state of this step, and returns the committed state. If the step has not
     * accessed any state, return null.
     */
    public CopyOnAccessInMemoryStateInternals commitState() {
      if (stateInternals != null) {
        return stateInternals.commit();
      }
      return null;
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
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window, Coder<W> windowCoder) throws IOException {
      throw new UnsupportedOperationException("Not implemented.");
    }

    /**
     * Gets the timer update of the {@link TimerInternals} of this {@link DirectStepContext},
     * which is empty if the {@link TimerInternals} were never accessed.
     */
    public TimerUpdate getTimerUpdate() {
      if (timerInternals == null) {
        return TimerUpdate.empty();
      }
      return timerInternals.getTimerUpdate();
    }
  }
}
