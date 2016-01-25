/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.KeyedWorkItem;
import com.google.cloud.dataflow.sdk.runners.worker.LateDataDroppingDoFnRunner;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingGroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingSideInputDoFnRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.List;

/**
 * Static utility methods that provide {@link DoFnRunner} implementations.
 */
public class DoFnRunners {
  /**
   * Information about how to create output receivers and output to them.
   */
  public interface OutputManager {
    /**
     * Outputs a single element to the receiver indicated by the given {@link TupleTag}.
     */
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output);
  }

  /**
   * Returns a basic implementation of {@link DoFnRunner} that works for most {@link DoFn DoFns}.
   *
   * <p>It invokes {@link DoFn#processElement} for each input.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> simpleRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SimpleDoFnRunner<>(
        options, fn, sideInputReader, outputManager, mainOutputTag, sideOutputTags,
        stepContext, addCounterMutator, windowingStrategy);
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
      options, doFnInfo, sideInputReader, outputManager, mainOutputTag, sideOutputTags,
      stepContext, addCounterMutator, doFnInfo.getWindowingStrategy());
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping.
   *
   * <p>It drops elements from expired windows before they reach the underlying {@link DoFn}.
   */
  public static <K, InputT, OutputT> DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>>
  lateDataDroppingRunner(
      PipelineOptions options,
      DoFnInfo<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFnInfo,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    @SuppressWarnings({"unchecked"})
    StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, ?> streamingGabwDoFn =
        (StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, ?>) doFnInfo.getDoFn();
    return new LateDataDroppingDoFnRunner<>(
        options,  streamingGabwDoFn, sideInputReader, outputManager, mainOutputTag, sideOutputTags,
        stepContext, addCounterMutator, doFnInfo.getWindowingStrategy());
  }
}
