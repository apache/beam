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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.runners.DirectPipelineRunner.ValueWithMetadata;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * {@link ExecutionContext} for use in direct mode.
 */
public class DirectModeExecutionContext
    extends BaseExecutionContext<DirectModeExecutionContext.StepContext> {

  private Object key;
  private List<ValueWithMetadata<?>> output = Lists.newArrayList();
  private Map<TupleTag<?>, List<ValueWithMetadata<?>>> sideOutputs = Maps.newHashMap();

  protected DirectModeExecutionContext() {}

  public static DirectModeExecutionContext create() {
    return new DirectModeExecutionContext();
  }

  @Override
  protected StepContext createStepContext(String stepName, String transformName) {
    return new StepContext(this, stepName, transformName);
  }

  public Object getKey() {
    return key;
  }

  public void setKey(Object newKey) {
    // The direct mode runner may reorder elements, so we need to keep
    // around the state used for each key.
    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      ((StepContext) stepContext).switchKey(newKey);
    }
    key = newKey;
  }

  @Override
  public void noteOutput(WindowedValue<?> outputElem) {
    output.add(ValueWithMetadata.of(outputElem).withKey(getKey()));
  }

  @Override
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> outputElem) {
    List<ValueWithMetadata<?>> output = sideOutputs.get(tag);
    if (output == null) {
      output = Lists.newArrayList();
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
      return Lists.newArrayList();
    }
  }

  /**
   * {@link ExecutionContext.StepContext} used in direct mode.
   */
  public static class StepContext extends BaseExecutionContext.StepContext {

    /** A map from each key to the state associated with it. */
    private final Map<Object, InMemoryStateInternals<Object>> stateInternals = Maps.newHashMap();
    private InMemoryStateInternals<Object> currentStateInternals = null;

    private StepContext(ExecutionContext executionContext, String stepName, String transformName) {
      super(executionContext, stepName, transformName);
      switchKey(null);
    }

    public void switchKey(Object newKey) {
      currentStateInternals = stateInternals.get(newKey);
      if (currentStateInternals == null) {
        currentStateInternals = InMemoryStateInternals.forKey(newKey);
        stateInternals.put(newKey, currentStateInternals);
      }
    }

    @Override
    public StateInternals<Object> stateInternals() {
      return checkNotNull(currentStateInternals);
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Direct mode cannot return timerInternals");
    }
  }
}
