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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunners.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.List;

/**
 * Utility methods for creating {@link DoFnRunner} instances used by streaming Dataflow.
 */
public final class StreamingDoFnRunners {
  private StreamingDoFnRunners() {
    // Do not instantiate
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles streaming side inputs.
   *
   * <p>It blocks and caches input elements if their side inputs are not ready.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> streamingSideInputRunner(
      PipelineOptions options,
      DoFnInfo<InputT, OutputT> doFnInfo,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    return new StreamingSideInputDoFnRunner<>(
        options,
        doFnInfo,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        doFnInfo.getWindowingStrategy());
  }


}

