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
package org.apache.beam.runners.dataflow.worker;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

class SimpleDoFnRunnerFactory<InputT, OutputT> implements DoFnRunnerFactory<InputT, OutputT> {
  public static final SimpleDoFnRunnerFactory INSTANCE = new SimpleDoFnRunnerFactory();

  @Override
  public DoFnRunner<InputT, OutputT> createRunner(
      DoFn<InputT, OutputT> fn,
      PipelineOptions options,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      Iterable<PCollectionView<?>> sideInputViews,
      SideInputReader sideInputReader,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DataflowExecutionContext.DataflowStepContext stepContext,
      DataflowExecutionContext.DataflowStepContext userStepContext,
      OutputManager outputManager,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    DoFnRunner<InputT, OutputT> fnRunner =
        DoFnRunners.simpleRunner(
            options,
            fn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            userStepContext,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);
    boolean hasStreamingSideInput =
        options.as(StreamingOptions.class).isStreaming() && !sideInputReader.isEmpty();
    if (hasStreamingSideInput) {
      return new StreamingSideInputDoFnRunner<>(
          fnRunner,
          new StreamingSideInputFetcher<>(
              sideInputViews,
              inputCoder,
              windowingStrategy,
              (StreamingModeExecutionContext.StreamingModeStepContext) userStepContext));
    }
    return fnRunner;
  }
}
