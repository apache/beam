/*
 * Copyright (C) 2014 Google Inc.
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

import static com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.ValueWithMetadata;

import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ExecutionContext} for use in direct mode.
 */
public class DirectModeExecutionContext extends BatchModeExecutionContext {
  List<ValueWithMetadata> output = new ArrayList<>();
  Map<TupleTag<?>, List<ValueWithMetadata>> sideOutputs = new HashMap<>();

  @Override
  public ExecutionContext.StepContext createStepContext(String stepName) {
    return new StepContext(stepName);
  }

  @Override
  public void noteOutput(WindowedValue<?> outputElem) {
    output.add(ValueWithMetadata.of(outputElem)
                                .withKey(getKey()));
  }

  @Override
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> outputElem) {
    List<ValueWithMetadata> output = sideOutputs.get(tag);
    if (output == null) {
      output = new ArrayList<>();
      sideOutputs.put(tag, output);
    }
    output.add(ValueWithMetadata.of(outputElem)
                                .withKey(getKey()));
  }

  public <T> List<ValueWithMetadata<T>> getOutput(TupleTag<T> tag) {
    return (List) output;
  }

  public <T> List<ValueWithMetadata<T>> getSideOutput(TupleTag<T> tag) {
    if (sideOutputs.containsKey(tag)) {
      return (List) sideOutputs.get(tag);
    } else {
      return new ArrayList<>();
    }
  }
}
