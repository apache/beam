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

import com.google.cloud.dataflow.sdk.util.state.InMemoryStateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;

import java.util.Objects;

/**
 * {@link ExecutionContext} for use in batch mode.
 */
public class BatchModeExecutionContext extends BaseExecutionContext {
  private Object key;

  /**
   * Creates a {@code BatchModeExecutionContext}.
   */
  public BatchModeExecutionContext() { }

  /**
   * Create a new {@link ExecutionContext.StepContext}.
   */
  @Override
  public ExecutionContext.StepContext createStepContext(String stepName, String transformName) {
    return new StepContext(stepName, transformName);
  }

  /**
   * Sets the key of the work currently being processed.
   */
  public void setKey(Object key) {
    if (!Objects.equals(key, this.key)) {
      switchStateKey(key);
    }

    this.key = key;
  }

  /**
   * @param newKey the key being switched to
   */
  protected void switchStateKey(Object newKey) {
    // When the key changes, we clear out the in-memory state stored in the step contexts.
    // In BatchMode a specific key is only processed in a single chunk
    // because the state is either used after a GroupByKeyOnly where
    // each key only occurs once, or after some ParDo's that preserved
    // the key.
    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      InMemoryStateInternals stateInternals =
          (InMemoryStateInternals) stepContext.stateInternals();
      stateInternals.clear();
    }
  }

  /**
   * Returns the key of the work currently being processed.
   *
   * <p> If there is not a currently defined key, returns null.
   */
  public Object getKey() {
    return key;
  }

  /**
   * {@link ExecutionContext.StepContext} used in batch mode.
   */
  class StepContext extends BaseExecutionContext.StepContext {

    private final InMemoryStateInternals stateInternals = new InMemoryStateInternals();

    private StepContext(String stepName, String transformName) {
      super(BatchModeExecutionContext.this, stepName, transformName);
    }

    @Override
    public StateInternals stateInternals() {
      return stateInternals;
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Batch mode cannot return timerInternals");
    }
  }
}
