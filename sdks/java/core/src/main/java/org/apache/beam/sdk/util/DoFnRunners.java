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

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnRunner.ReduceFnExecutor;
import org.apache.beam.sdk.util.ExecutionContext.StepContext;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.common.CounterSet.AddCounterMutator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

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
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        windowingStrategy);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping.
   *
   * <p>It drops elements from expired windows before they reach the underlying {@link DoFn}.
   */
  public static <K, InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> lateDataDroppingRunner(
          PipelineOptions options,
          ReduceFnExecutor<K, InputT, OutputT, W> reduceFnExecutor,
          SideInputReader sideInputReader,
          OutputManager outputManager,
          TupleTag<KV<K, OutputT>> mainOutputTag,
          List<TupleTag<?>> sideOutputTags,
          StepContext stepContext,
          CounterSet.AddCounterMutator addCounterMutator,
          WindowingStrategy<?, W> windowingStrategy) {
    DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> simpleDoFnRunner =
        simpleRunner(
            options,
            reduceFnExecutor.asDoFn(),
            sideInputReader,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            stepContext,
            addCounterMutator,
            windowingStrategy);
    return new LateDataDroppingDoFnRunner<>(
        simpleDoFnRunner,
        windowingStrategy,
        stepContext.timerInternals(),
        reduceFnExecutor.getDroppedDueToLatenessAggregator());
  }

  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> createDefault(
      PipelineOptions options,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    if (doFn instanceof ReduceFnExecutor) {
      @SuppressWarnings("rawtypes")
      ReduceFnExecutor fn = (ReduceFnExecutor) doFn;
      @SuppressWarnings({"unchecked", "cast", "rawtypes"})
      DoFnRunner<InputT, OutputT> runner = (DoFnRunner<InputT, OutputT>) lateDataDroppingRunner(
          options,
          fn,
          sideInputReader,
          outputManager,
          (TupleTag) mainOutputTag,
          sideOutputTags,
          stepContext,
          addCounterMutator,
          (WindowingStrategy) windowingStrategy);
      return runner;
    }
    return simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        windowingStrategy);
  }
}
