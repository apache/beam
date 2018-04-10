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
package org.apache.beam.runners.core;

import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StatefulDoFnRunner.CleanupTimer;
import org.apache.beam.runners.core.StatefulDoFnRunner.StateCleaner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

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
    <T> void output(TupleTag<T> tag, WindowedValue<T> output);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that for a {@link DoFn}.
   *
   * <p>If the {@link DoFn} observes the window, this runner will explode the windows of a
   * compressed {@link WindowedValue}. It is the responsibility of the runner to perform any key
   * partitioning needed, etc.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> simpleRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SimpleDoFnRunner<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping.
   *
   * <p>It drops elements from expired windows before they reach the underlying {@link DoFn}.
   */
  public static <K, InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> lateDataDroppingRunner(
          DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> wrappedRunner,
          TimerInternals timerInternals,
          WindowingStrategy<?, W> windowingStrategy) {
    return new LateDataDroppingDoFnRunner<>(
        wrappedRunner,
        windowingStrategy,
        timerInternals);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles
   * late data dropping and garbage collection for stateful {@link DoFn DoFns}.
   *
   * <p>It registers a timer by TimeInternals, and clean all states by StateInternals.
   */
  public static <InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<InputT, OutputT> defaultStatefulDoFnRunner(
          DoFn<InputT, OutputT> fn,
          DoFnRunner<InputT, OutputT> doFnRunner,
          WindowingStrategy<?, ?> windowingStrategy,
          CleanupTimer cleanupTimer,
          StateCleaner<W> stateCleaner) {
    return new StatefulDoFnRunner<>(
        doFnRunner,
        windowingStrategy,
        cleanupTimer,
        stateCleaner);
  }

  public static <InputT, OutputT, RestrictionT>
  ProcessFnRunner<InputT, OutputT, RestrictionT>
  newProcessFnRunner(
      ProcessFn<InputT, OutputT, RestrictionT, ?> fn,
      PipelineOptions options,
      Collection<PCollectionView<?>> views,
      ReadyCheckingSideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new ProcessFnRunner<>(
        simpleRunner(
            options,
            fn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            stepContext,
            windowingStrategy),
        views,
        sideInputReader);
  }
}
