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

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.DirectModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.DirectSideInputReader;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunnerBase;
import com.google.cloud.dataflow.sdk.util.DoFnRunners;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A harness for unit-testing a {@link DoFn}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * DoFn<InputT, OutputT> fn = ...;
 *
 * DoFnTester<InputT, OutputT> fnTester = DoFnTester.of(fn);
 *
 * // Set arguments shared across all batches:
 * fnTester.setSideInputs(...);      // If fn takes side inputs.
 * fnTester.setSideOutputTags(...);  // If fn writes to side outputs.
 *
 * // Process a batch containing a single input element:
 * Input testInput = ...;
 * List<OutputT> testOutputs = fnTester.processBatch(testInput);
 * Assert.assertThat(testOutputs,
 *                   JUnitMatchers.hasItems(...));
 *
 * // Process a bigger batch:
 * Assert.assertThat(fnTester.processBatch(i1, i2, ...),
 *                   JUnitMatchers.hasItems(...));
 * } </pre>
 *
 * @param <InputT> the type of the {@code DoFn}'s (main) input elements
 * @param <OutputT> the type of the {@code DoFn}'s (main) output elements
 */
public class DoFnTester<InputT, OutputT> {
  /**
   * Returns a {@code DoFnTester} supporting unit-testing of the given
   * {@link DoFn}.
   */
  @SuppressWarnings("unchecked")
  public static <InputT, OutputT> DoFnTester<InputT, OutputT> of(DoFn<InputT, OutputT> fn) {
    return new DoFnTester<InputT, OutputT>(fn);
  }

  /**
   * Returns a {@code DoFnTester} supporting unit-testing of the given
   * {@link DoFn}.
   */
  @SuppressWarnings("unchecked")
  public static <InputT, OutputT> DoFnTester<InputT, OutputT>
      of(DoFnWithContext<InputT, OutputT> fn) {
    return new DoFnTester<InputT, OutputT>(DoFnReflector.of(fn.getClass()).toDoFn(fn));
  }

  /**
   * Registers the tuple of values of the side input {@link PCollectionView}s to
   * pass to the {@link DoFn} under test.
   *
   * <p>If needed, first creates a fresh instance of the {@link DoFn}
   * under test.
   *
   * <p>If this isn't called, {@code DoFnTester} assumes the
   * {@link DoFn} takes no side inputs.
   */
  public void setSideInputs(Map<PCollectionView<?>, Iterable<WindowedValue<?>>> sideInputs) {
    this.sideInputs = sideInputs;
    resetState();
  }

  /**
   * Registers the values of a side input {@link PCollectionView} to
   * pass to the {@link DoFn} under test.
   *
   * <p>If needed, first creates a fresh instance of the {@code DoFn}
   * under test.
   *
   * <p>If this isn't called, {@code DoFnTester} assumes the
   * {@code DoFn} takes no side inputs.
   */
  public void setSideInput(PCollectionView<?> sideInput, Iterable<WindowedValue<?>> value) {
    sideInputs.put(sideInput, value);
  }

  /**
   * Registers the values for a side input {@link PCollectionView} to
   * pass to the {@link DoFn} under test. All values are placed
   * in the global window.
   */
  public void setSideInputInGlobalWindow(
      PCollectionView<?> sideInput,
      Iterable<?> value) {
    sideInputs.put(
        sideInput,
        Iterables.transform(value, new Function<Object, WindowedValue<?>>() {
          @Override
          public WindowedValue<?> apply(Object input) {
            return WindowedValue.valueInGlobalWindow(input);
          }
        }));
  }


  /**
   * Registers the list of {@code TupleTag}s that can be used by the
   * {@code DoFn} under test to output to side output
   * {@code PCollection}s.
   *
   * <p>If needed, first creates a fresh instance of the DoFn under test.
   *
   * <p>If this isn't called, {@code DoFnTester} assumes the
   * {@code DoFn} doesn't emit to any side outputs.
   */
  public void setSideOutputTags(TupleTagList sideOutputTags) {
    this.sideOutputTags = sideOutputTags.getAll();
    resetState();
  }

  /**
   * A convenience operation that first calls {@link #startBundle},
   * then calls {@link #processElement} on each of the input elements, then
   * calls {@link #finishBundle}, then returns the result of
   * {@link #takeOutputElements}.
   */
  public List<OutputT> processBatch(Iterable <? extends InputT> inputElements) {
    startBundle();
    for (InputT inputElement : inputElements) {
      processElement(inputElement);
    }
    finishBundle();
    return takeOutputElements();
  }

  /**
   * A convenience method for testing {@link DoFn DoFns} with bundles of elements.
   * Logic proceeds as follows:
   *
   * <ol>
   *   <li>Calls {@link #startBundle}.</li>
   *   <li>Calls {@link #processElement} on each of the arguments.<li>
   *   <li>Calls {@link #finishBundle}.</li>
   *   <li>Returns the result of {@link #takeOutputElements}.</li>
   * </ol>
   */
  @SafeVarargs
  public final List<OutputT> processBatch(InputT... inputElements) {
    return processBatch(Arrays.asList(inputElements));
  }

  /**
   * Calls {@link DoFn#startBundle} on the {@code DoFn} under test.
   *
   * <p>If needed, first creates a fresh instance of the DoFn under test.
   */
  public void startBundle() {
    resetState();
    initializeState();
    fnRunner.startBundle();
    state = State.STARTED;
  }

  /**
   * Calls {@link DoFn#processElement} on the {@code DoFn} under test, in a
   * context where {@link DoFn.ProcessContext#element} returns the
   * given element.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   *
   * @throws IllegalStateException if the {@code DoFn} under test has already
   * been finished
   */
  public void processElement(InputT element) {
    if (state == State.FINISHED) {
      throw new IllegalStateException("finishBundle() has already been called");
    }
    if (state == State.UNSTARTED) {
      startBundle();
    }
    fnRunner.processElement(WindowedValue.valueInGlobalWindow(element));
  }

  /**
   * Calls {@link DoFn#finishBundle} of the {@code DoFn} under test.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   *
   * @throws IllegalStateException if the {@code DoFn} under test has already
   * been finished
   */
  public void finishBundle() {
    if (state == State.FINISHED) {
      throw new IllegalStateException("finishBundle() has already been called");
    }
    if (state == State.UNSTARTED) {
      startBundle();
    }
    fnRunner.finishBundle();
    state = State.FINISHED;
  }

  /**
   * Returns the elements output so far to the main output.  Does not
   * clear them, so subsequent calls will continue to include these
   * elements.
   *
   * @see #takeOutputElements
   * @see #clearOutputElements
   *
   */
  public List<OutputT> peekOutputElements() {
    // TODO: Should we return an unmodifiable list?
    return Lists.transform(
        peekOutputElementsWithTimestamp(),
        new Function<OutputElementWithTimestamp<OutputT>, OutputT>() {
          @Override
          @SuppressWarnings("unchecked")
          public OutputT apply(OutputElementWithTimestamp<OutputT> input) {
            return input.getValue();
          }
        });
  }

  /**
   * Returns the elements output so far to the main output with associated timestamps.  Does not
   * clear them, so subsequent calls will continue to include these.
   * elements.
   *
   * @see #takeOutputElementsWithTimestamp
   * @see #clearOutputElements
   */
  @Experimental
  public List<OutputElementWithTimestamp<OutputT>> peekOutputElementsWithTimestamp() {
    // TODO: Should we return an unmodifiable list?
    return Lists.transform(
        outputManager.getOutput(mainOutputTag),
        new Function<Object, OutputElementWithTimestamp<OutputT>>() {
          @Override
          @SuppressWarnings("unchecked")
          public OutputElementWithTimestamp<OutputT> apply(Object input) {
            return new OutputElementWithTimestamp<OutputT>(
                ((WindowedValue<OutputT>) input).getValue(),
                ((WindowedValue<OutputT>) input).getTimestamp());
          }
        });
  }

  /**
   * Clears the record of the elements output so far to the main output.
   *
   * @see #peekOutputElements
   */
  public void clearOutputElements() {
    peekOutputElements().clear();
  }

  /**
   * Returns the elements output so far to the main output.
   * Clears the list so these elements don't appear in future calls.
   *
   * @see #peekOutputElements
   */
  public List<OutputT> takeOutputElements() {
    List<OutputT> resultElems = new ArrayList<>(peekOutputElements());
    clearOutputElements();
    return resultElems;
  }

  /**
   * Returns the elements output so far to the main output with associated timestamps.
   * Clears the list so these elements don't appear in future calls.
   *
   * @see #peekOutputElementsWithTimestamp
   * @see #takeOutputElements
   * @see #clearOutputElements
   */
  @Experimental
  public List<OutputElementWithTimestamp<OutputT>> takeOutputElementsWithTimestamp() {
    List<OutputElementWithTimestamp<OutputT>> resultElems =
        new ArrayList<>(peekOutputElementsWithTimestamp());
    clearOutputElements();
    return resultElems;
  }

  /**
   * Returns the elements output so far to the side output with the
   * given tag.  Does not clear them, so subsequent calls will
   * continue to include these elements.
   *
   * @see #takeSideOutputElements
   * @see #clearSideOutputElements
   */
  public <T> List<T> peekSideOutputElements(TupleTag<T> tag) {
    // TODO: Should we return an unmodifiable list?
    return Lists.transform(
        outputManager.getOutput(tag),
        new Function<WindowedValue<T>, T>() {
          @SuppressWarnings("unchecked")
          @Override
          public T apply(WindowedValue<T> input) {
            return input.getValue();
          }});
  }

  /**
   * Clears the record of the elements output so far to the side
   * output with the given tag.
   *
   * @see #peekSideOutputElements
   */
  public <T> void clearSideOutputElements(TupleTag<T> tag) {
    peekSideOutputElements(tag).clear();
  }

  /**
   * Returns the elements output so far to the side output with the given tag.
   * Clears the list so these elements don't appear in future calls.
   *
   * @see #peekSideOutputElements
   */
  public <T> List<T> takeSideOutputElements(TupleTag<T> tag) {
    List<T> resultElems = new ArrayList<>(peekSideOutputElements(tag));
    clearSideOutputElements(tag);
    return resultElems;
  }

  /**
   * Returns the value of the provided {@link Aggregator}.
   */
  public <AggregateT> AggregateT getAggregatorValue(Aggregator<?, AggregateT> agg) {
    @SuppressWarnings("unchecked")
    Counter<AggregateT> counter =
        (Counter<AggregateT>)
            counterSet.getExistingCounter("user-" + STEP_NAME + "-" + agg.getName());
    return counter.getAggregate();
  }

  /**
   * Holder for an OutputElement along with its associated timestamp.
   */
  @Experimental
  public static class OutputElementWithTimestamp<OutputT> {
    private final OutputT value;
    private final Instant timestamp;

    OutputElementWithTimestamp(OutputT value, Instant timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    OutputT getValue() {
      return value;
    }

    Instant getTimestamp() {
      return timestamp;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof OutputElementWithTimestamp)) {
        return false;
      }
      OutputElementWithTimestamp<?> other = (OutputElementWithTimestamp<?>) obj;
      return Objects.equal(other.value, value) && Objects.equal(other.timestamp, timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value, timestamp);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** The possible states of processing a DoFn. */
  enum State {
    UNSTARTED,
    STARTED,
    FINISHED
  }

  /** The name of the step of a DoFnTester. */
  static final String STEP_NAME = "stepName";
  /** The name of the enclosing DoFn PTransform for a DoFnTester. */
  static final String TRANSFORM_NAME = "transformName";

  final PipelineOptions options = PipelineOptionsFactory.create();

  /** The original DoFn under test. */
  final DoFn<InputT, OutputT> origFn;

  /** The side input values to provide to the DoFn under test. */
  private Map<PCollectionView<?>, Iterable<WindowedValue<?>>> sideInputs =
      new HashMap<>();

  /** The output tags used by the DoFn under test. */
  TupleTag<OutputT> mainOutputTag = new TupleTag<>();
  List<TupleTag<?>> sideOutputTags = new ArrayList<>();

  /** The original DoFn under test, if started. */
  DoFn<InputT, OutputT> fn;

  /** The ListOutputManager to examine the outputs. */
  DoFnRunnerBase.ListOutputManager outputManager;

  /** The DoFnRunner if processing is in progress. */
  DoFnRunner<InputT, OutputT> fnRunner;

  /** Counters for user-defined Aggregators if processing is in progress. */
  CounterSet counterSet;

  /** The state of processing of the DoFn under test. */
  State state;

  DoFnTester(DoFn<InputT, OutputT> origFn) {
    this.origFn = origFn;
    resetState();
  }

  void resetState() {
    fn = null;
    outputManager = null;
    fnRunner = null;
    counterSet = null;
    state = State.UNSTARTED;
  }

  @SuppressWarnings("unchecked")
  void initializeState() {
    fn = (DoFn<InputT, OutputT>)
        SerializableUtils.deserializeFromByteArray(
            SerializableUtils.serializeToByteArray(origFn),
            origFn.toString());
    counterSet = new CounterSet();
    PTuple runnerSideInputs = PTuple.empty();
    for (Map.Entry<PCollectionView<?>, Iterable<WindowedValue<?>>> entry
        : sideInputs.entrySet()) {
      runnerSideInputs = runnerSideInputs.and(entry.getKey().getTagInternal(), entry.getValue());
    }
    outputManager = new DoFnRunnerBase.ListOutputManager();
    fnRunner = DoFnRunners.createDefault(
        options,
        fn,
        DirectSideInputReader.of(runnerSideInputs),
        outputManager,
        mainOutputTag,
        sideOutputTags,
        DirectModeExecutionContext.create().getOrCreateStepContext(STEP_NAME, TRANSFORM_NAME, null),
        counterSet.getAddCounterMutator(),
        WindowingStrategy.globalDefault());
  }
}
