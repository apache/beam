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

import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.DoFnRunners.OutputManager;
import org.apache.beam.sdk.util.ExecutionContext.StepContext;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Runs a {@link OldDoFn} by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the OldDoFn's (main) input elements
 * @param <OutputT> the type of the OldDoFn's (main) output elements
 */
public class SimpleDoFnRunner<InputT, OutputT> extends DoFnRunnerBase<InputT, OutputT>{

  protected SimpleDoFnRunner(PipelineOptions options, OldDoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag, List<TupleTag<?>> sideOutputTags, StepContext stepContext,
      AggregatorFactory aggregatorFactory, WindowingStrategy<?, ?> windowingStrategy) {
    super(options, fn, sideInputReader, outputManager, mainOutputTag, sideOutputTags, stepContext,
        aggregatorFactory, windowingStrategy);
  }

  @Override
  protected void invokeProcessElement(WindowedValue<InputT> elem) {
    final OldDoFn<InputT, OutputT>.ProcessContext processContext = createProcessContext(elem);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
    }
  }
}
