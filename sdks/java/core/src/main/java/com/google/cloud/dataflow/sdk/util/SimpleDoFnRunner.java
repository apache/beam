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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.DoFnRunners.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.List;

/**
 * Runs a {@link DoFn} by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the DoFn's (main) input elements
 * @param <OutputT> the type of the DoFn's (main) output elements
 */
public class SimpleDoFnRunner<InputT, OutputT> extends DoFnRunnerBase<InputT, OutputT>{

  protected SimpleDoFnRunner(PipelineOptions options, DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag, List<TupleTag<?>> sideOutputTags, StepContext stepContext,
      AddCounterMutator addCounterMutator, WindowingStrategy<?, ?> windowingStrategy) {
    super(options, fn, sideInputReader, outputManager, mainOutputTag, sideOutputTags, stepContext,
        addCounterMutator, windowingStrategy);
  }

  @Override
  protected void invokeProcessElement(WindowedValue<InputT> elem) {
    final DoFn<InputT, OutputT>.ProcessContext processContext = createProcessContext(elem);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
    }
  }
}

