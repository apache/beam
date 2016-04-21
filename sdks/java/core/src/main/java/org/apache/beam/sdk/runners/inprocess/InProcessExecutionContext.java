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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.runners.inprocess.InMemoryWatermarkManager.TimerUpdate;
import org.apache.beam.sdk.runners.inprocess.InMemoryWatermarkManager.TransformWatermarks;
import org.apache.beam.sdk.util.BaseExecutionContext;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;

/**
 * Execution Context for the {@link InProcessPipelineRunner}.
 *
 * This implementation is not thread safe. A new {@link InProcessExecutionContext} must be created
 * for each thread that requires it.
 */
class InProcessExecutionContext
    extends BaseExecutionContext<InProcessExecutionContext.InProcessStepContext> {
  private final Clock clock;
  private final Object key;
  private final CopyOnAccessInMemoryStateInternals<Object> existingState;
  private final TransformWatermarks watermarks;

  public InProcessExecutionContext(Clock clock, Object key,
      CopyOnAccessInMemoryStateInternals<Object> existingState, TransformWatermarks watermarks) {
    this.clock = clock;
    this.key = key;
    this.existingState = existingState;
    this.watermarks = watermarks;
  }

  @Override
  protected InProcessStepContext createStepContext(String stepName, String transformName) {
    return new InProcessStepContext(this, stepName, transformName);
  }

  /**
   * Step Context for the {@link InProcessPipelineRunner}.
   */
  public class InProcessStepContext
      extends org.apache.beam.sdk.util.BaseExecutionContext.StepContext {
    private CopyOnAccessInMemoryStateInternals<Object> stateInternals;
    private InProcessTimerInternals timerInternals;

    public InProcessStepContext(
        ExecutionContext executionContext, String stepName, String transformName) {
      super(executionContext, stepName, transformName);
    }

    @Override
    public CopyOnAccessInMemoryStateInternals<Object> stateInternals() {
      if (stateInternals == null) {
        stateInternals = CopyOnAccessInMemoryStateInternals.withUnderlying(key, existingState);
      }
      return stateInternals;
    }

    @Override
    public InProcessTimerInternals timerInternals() {
      if (timerInternals == null) {
        timerInternals =
            InProcessTimerInternals.create(clock, watermarks, TimerUpdate.builder(key));
      }
      return timerInternals;
    }

    /**
     * Commits the state of this step, and returns the committed state. If the step has not
     * accessed any state, return null.
     */
    public CopyOnAccessInMemoryStateInternals<?> commitState() {
      if (stateInternals != null) {
        return stateInternals.commit();
      }
      return null;
    }

    /**
     * Gets the timer update of the {@link TimerInternals} of this {@link InProcessStepContext},
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
