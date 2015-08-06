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

import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.ValueWithMetadata;
import com.google.cloud.dataflow.sdk.util.state.InMemoryStateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ExecutionContext} for use in direct mode.
 */
public class DirectModeExecutionContext extends BatchModeExecutionContext {

  private List<ValueWithMetadata<?>> output = new ArrayList<>();
  private Map<TupleTag<?>, List<ValueWithMetadata<?>>> sideOutputs = new HashMap<>();

  protected DirectModeExecutionContext() {}

  public static DirectModeExecutionContext create() {
    return new DirectModeExecutionContext();
  }

  @Override
  public ExecutionContext.StepContext createStepContext(String stepName, String transformName) {
    return new StepContext(stepName, transformName);
  }

  @Override
  protected void switchStateKey(Object newKey) {
    // The direct mode runner may reorder elements, so we need to keep
    // around the state used for each key.
    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      ((StepContext) stepContext).switchKey(newKey);
    }
  }

  @Override
  public void noteOutput(WindowedValue<?> outputElem) {
    output.add(ValueWithMetadata.of(outputElem).withKey(getKey()));
  }

  @Override
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> outputElem) {
    List<ValueWithMetadata<?>> output = sideOutputs.get(tag);
    if (output == null) {
      output = new ArrayList<>();
      sideOutputs.put(tag, output);
    }
    output.add(ValueWithMetadata.of(outputElem).withKey(getKey()));
  }

  public <T> List<ValueWithMetadata<T>> getOutput(@SuppressWarnings("unused") TupleTag<T> tag) {
    @SuppressWarnings({"unchecked", "rawtypes"}) // Cast not expressible without rawtypes
    List<ValueWithMetadata<T>> typedOutput = (List) output;
    return typedOutput;
  }

  public <T> List<ValueWithMetadata<T>> getSideOutput(TupleTag<T> tag) {
    if (sideOutputs.containsKey(tag)) {
      @SuppressWarnings({"unchecked", "rawtypes"}) // Cast not expressible without rawtypes
      List<ValueWithMetadata<T>> typedOutput = (List) sideOutputs.get(tag);
      return typedOutput;
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * {@link ExecutionContext.StepContext} used in direct mode.
   */
  class StepContext extends BaseExecutionContext.StepContext {

    private final Map<Object, InMemoryStateInternals> stateInternals = new HashMap<>();
    private InMemoryStateInternals currentStateInternals = null;

    private StepContext(String stepName, String transformName) {
      super(DirectModeExecutionContext.this, stepName, transformName);
      switchKey(null);
    }

    public void switchKey(Object newKey) {
      currentStateInternals = stateInternals.get(newKey);
      if (currentStateInternals == null) {
        currentStateInternals = new InMemoryStateInternals();
        stateInternals.put(newKey, currentStateInternals);
      }
    }

    @Override
    public StateInternals stateInternals() {
      return Preconditions.checkNotNull(currentStateInternals);
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Direct mode cannot return timerInternals");
    }
  }
}
