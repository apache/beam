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
import java.util.Map;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StatefulDoFnRunner.CleanupTimer;
import org.apache.beam.runners.core.StatefulDoFnRunner.StateCleaner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Static utility methods that provide {@link DoFnRunner} implementations. */
public class DoFnRunners {
  /** Information about how to create output receivers and output to them. */
  public interface OutputManager {
    /** Outputs a single element to the receiver indicated by the given {@link TupleTag}. */
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
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    return new SimpleDoFnRunner<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        inputCoder,
        outputCoders,
        windowingStrategy,
        doFnSchemaInformation,
        sideInputMapping);
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
    return new LateDataDroppingDoFnRunner<>(wrappedRunner, windowingStrategy, timerInternals);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping and garbage
   * collection for stateful {@link DoFn DoFns}.
   *
   * <p>It registers a timer by TimeInternals, and clean all states by StateInternals. It also
   * correctly handles {@link DoFn.RequiresTimeSortedInput} if the provided {@link DoFn} requires
   * this.
   */
  public static <InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<InputT, OutputT> defaultStatefulDoFnRunner(
          DoFn<InputT, OutputT> fn,
          Coder<InputT> inputCoder,
          DoFnRunner<InputT, OutputT> doFnRunner,
          StepContext stepContext,
          WindowingStrategy<?, ?> windowingStrategy,
          CleanupTimer<InputT> cleanupTimer,
          StateCleaner<W> stateCleaner) {

    return defaultStatefulDoFnRunner(
        fn,
        inputCoder,
        doFnRunner,
        stepContext,
        windowingStrategy,
        cleanupTimer,
        stateCleaner,
        false);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping and garbage
   * collection for stateful {@link DoFn DoFns}.
   *
   * <p>It registers a timer by TimeInternals, and clean all states by StateInternals. If {@code
   * requiresTimeSortedInputSupported} is {@code true} then it also handles {@link
   * DoFn.RequiresTimeSortedInput} if the provided {@link DoFn} requires this. If {@code
   * requiresTimeSortedInputSupported} is {@code false} and the provided {@link DoFn} has {@link
   * DoFn.RequiresTimeSortedInput} this method will throw {@link UnsupportedOperationException}.
   */
  public static <InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<InputT, OutputT> defaultStatefulDoFnRunner(
          DoFn<InputT, OutputT> fn,
          Coder<InputT> inputCoder,
          DoFnRunner<InputT, OutputT> doFnRunner,
          StepContext stepContext,
          WindowingStrategy<?, ?> windowingStrategy,
          CleanupTimer<InputT> cleanupTimer,
          StateCleaner<W> stateCleaner,
          boolean requiresTimeSortedInputSupported) {

    boolean doFnRequiresTimeSortedInput =
        DoFnSignatures.signatureForDoFn(doFnRunner.getFn())
            .processElement()
            .requiresTimeSortedInput();

    if (doFnRequiresTimeSortedInput && !requiresTimeSortedInputSupported) {
      throw new UnsupportedOperationException(
          "DoFn.RequiresTimeSortedInput not currently supported by this runner.");
    }
    return new StatefulDoFnRunner<>(
        doFnRunner,
        inputCoder,
        stepContext,
        windowingStrategy,
        cleanupTimer,
        stateCleaner,
        doFnRequiresTimeSortedInput);
  }

  public static <InputT, OutputT, RestrictionT>
      ProcessFnRunner<InputT, OutputT, RestrictionT> newProcessFnRunner(
          ProcessFn<InputT, OutputT, RestrictionT, ?, ?> fn,
          PipelineOptions options,
          Collection<PCollectionView<?>> views,
          ReadyCheckingSideInputReader sideInputReader,
          OutputManager outputManager,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          StepContext stepContext,
          @Nullable Coder<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> inputCoder,
          Map<TupleTag<?>, Coder<?>> outputCoders,
          WindowingStrategy<?, ?> windowingStrategy,
          DoFnSchemaInformation doFnSchemaInformation,
          Map<String, PCollectionView<?>> sideInputMapping) {
    return new ProcessFnRunner<>(
        simpleRunner(
            options,
            fn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            stepContext,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping),
        views,
        sideInputReader);
  }
}
