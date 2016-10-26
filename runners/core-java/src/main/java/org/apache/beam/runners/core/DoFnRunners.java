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

import java.util.List;
import org.apache.beam.runners.core.DoFnRunner.ReduceFnExecutor;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupAlsoByWindow;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ExecutionContext.StepContext;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

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
  static <InputT, OutputT> DoFnRunner<InputT, OutputT> simpleRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SimpleDoFnRunner<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        aggregatorFactory,
        windowingStrategy);
  }

  /**
   * Returns a basic implementation of {@link DoFnRunner} that works for most {@link OldDoFn DoFns}.
   *
   * <p>It invokes {@link OldDoFn#processElement} for each input.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> simpleRunner(
      PipelineOptions options,
      OldDoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SimpleOldDoFnRunner<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        aggregatorFactory,
        windowingStrategy);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping.
   *
   * <p>It drops elements from expired windows before they reach the underlying {@link OldDoFn}.
   */
  public static <K, InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> lateDataDroppingRunner(
          DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> wrappedRunner,
          StepContext stepContext,
          WindowingStrategy<?, W> windowingStrategy,
          Aggregator<Long, Long> droppedDueToLatenessAggregator) {
    return new LateDataDroppingDoFnRunner<>(
        wrappedRunner,
        windowingStrategy,
        stepContext.timerInternals(),
        droppedDueToLatenessAggregator);
  }

  /**
   * Creates a {@link DoFnRunner} for the provided {@link DoFn}.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> createDefault(
      PipelineOptions options,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {

    // Unlike for OldDoFn, there is no ReduceFnExecutor that is a new DoFn,
    // and window-exploded processing is achieved within the simple runner
    return simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        aggregatorFactory,
        windowingStrategy);
  }

  /**
   * Creates a {@link DoFnRunner} for the provided {@link OldDoFn}.
   *
   * <p>In particular, if the {@link OldDoFn} is a {@link ReduceFnExecutor}, a specialized
   * implementation detail of streaming {@link GroupAlsoByWindow}, then it will create a special
   * runner that operates on {@link KeyedWorkItem KeyedWorkItems}, drops late data and counts
   * dropped elements.
   *
   * @deprecated please port uses of {@link OldDoFn} to use {@link DoFn}
   */
  @Deprecated
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> createDefault(
      PipelineOptions options,
      OldDoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {

    DoFnRunner<InputT, OutputT> doFnRunner = simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        aggregatorFactory,
        windowingStrategy);

    if (!(doFn instanceof ReduceFnExecutor)) {
      return doFnRunner;
    } else {
      // When a DoFn is a ReduceFnExecutor, we know it has to have an aggregator for dropped
      // elements and we also learn that for some K and V,
      //   InputT = KeyedWorkItem<K, V>
      //   OutputT = KV<K, V>

      Aggregator<Long, Long> droppedDueToLatenessAggregator =
          ((ReduceFnExecutor<?, ?, ?, ?>) doFn).getDroppedDueToLatenessAggregator();

      @SuppressWarnings({"unchecked", "cast", "rawtypes"})
      DoFnRunner<InputT, OutputT> runner = (DoFnRunner<InputT, OutputT>) lateDataDroppingRunner(
          (DoFnRunner) doFnRunner,
          stepContext,
          (WindowingStrategy) windowingStrategy,
          droppedDueToLatenessAggregator);

      return runner;
    }
  }

  /**
   * Creates the right kind of {@link DoFnRunner} for an object that can be either a {@link DoFn} or
   * {@link OldDoFn}. This can be used so that the client need not explicitly reference either such
   * class, but merely deserialize a payload and pass it to this method.
   *
   * @deprecated for migration purposes only for services where users may still submit either {@link
   *     OldDoFn} or {@link DoFn}. If you know that you have a {@link DoFn} then you should use the
   *     variant for that instead.
   */
  @Deprecated
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> createDefault(
      PipelineOptions options,
      Object deserializedFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {
    if (deserializedFn instanceof DoFn) {
      return createDefault(
          options,
          (DoFn) deserializedFn,
          sideInputReader,
          outputManager,
          mainOutputTag,
          sideOutputTags,
          stepContext,
          aggregatorFactory,
          windowingStrategy);
    } else if (deserializedFn instanceof OldDoFn) {
      return createDefault(
          options,
          (OldDoFn) deserializedFn,
          sideInputReader,
          outputManager,
          mainOutputTag,
          sideOutputTags,
          stepContext,
          aggregatorFactory,
          windowingStrategy);
    } else {
      throw new IllegalArgumentException(String.format("Cannot create %s for %s of class %s",
          DoFnRunner.class.getSimpleName(),
          deserializedFn,
          deserializedFn.getClass()));
    }
  }
}
