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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

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
 * // Set arguments shared across all bundles:
 * fnTester.setSideInputs(...);      // If fn takes side inputs.
 * fnTester.setSideOutputTags(...);  // If fn writes to side outputs.
 *
 * // Process a bundle containing a single input element:
 * Input testInput = ...;
 * List<OutputT> testOutputs = fnTester.processBundle(testInput);
 * Assert.assertThat(testOutputs, Matchers.hasItems(...));
 *
 * // Process a bigger bundle:
 * Assert.assertThat(fnTester.processBundle(i1, i2, ...), Matchers.hasItems(...));
 * } </pre>
 *
 * @param <InputT> the type of the {@link DoFn}'s (main) input elements
 * @param <OutputT> the type of the {@link DoFn}'s (main) output elements
 */
public class DoFnTester<InputT, OutputT> implements AutoCloseable {
  /**
   * Returns a {@code DoFnTester} supporting unit-testing of the given
   * {@link DoFn}. By default, uses {@link CloningBehavior#CLONE_ONCE}.
   */
  @SuppressWarnings("unchecked")
  public static <InputT, OutputT> DoFnTester<InputT, OutputT> of(DoFn<InputT, OutputT> fn) {
    return new DoFnTester<>(DoFnAdapters.toOldDoFn(fn));
  }

  /**
   * Returns a {@code DoFnTester} supporting unit-testing of the given
   * {@link OldDoFn}.
   *
   * @see #of(DoFn)
   */
  @SuppressWarnings("unchecked")
   public static <InputT, OutputT> DoFnTester<InputT, OutputT>
      of(OldDoFn<InputT, OutputT> fn) {
    return new DoFnTester<>(fn);
  }

  /**
   * Registers the tuple of values of the side input {@link PCollectionView}s to
   * pass to the {@link DoFn} under test.
   *
   * <p>Resets the state of this {@link DoFnTester}.
   *
   * <p>If this isn't called, {@code DoFnTester} assumes the
   * {@link DoFn} takes no side inputs.
   */
  public void setSideInputs(Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs) {
    checkState(
        state == State.UNINITIALIZED,
        "Can't add side inputs: DoFnTester is already initialized, in state %s",
        state);
    this.sideInputs = sideInputs;
  }

  /**
   * Registers the values of a side input {@link PCollectionView} to pass to the {@link DoFn}
   * under test.
   *
   * <p>The provided value is the final value of the side input in the specified window, not
   * the value of the input PCollection in that window.
   *
   * <p>If this isn't called, {@code DoFnTester} will return the default value for any side input
   * that is used.
   */
  public <T> void setSideInput(PCollectionView<T> sideInput, BoundedWindow window, T value) {
    checkState(
        state == State.UNINITIALIZED,
        "Can't add side inputs: DoFnTester is already initialized, in state %s",
        state);
    Map<BoundedWindow, T> windowValues = (Map<BoundedWindow, T>) sideInputs.get(sideInput);
    if (windowValues == null) {
      windowValues = new HashMap<>();
      sideInputs.put(sideInput, windowValues);
    }
    windowValues.put(window, value);
  }

  /**
   * When a {@link DoFnTester} should clone the {@link DoFn} under test and how it should manage
   * the lifecycle of the {@link DoFn}.
   */
  public enum CloningBehavior {
    /**
     * Clone the {@link DoFn} and call {@link DoFn.Setup} every time a bundle starts; call {@link
     * DoFn.Teardown} every time a bundle finishes.
     */
    CLONE_PER_BUNDLE,
    /**
     * Clone the {@link DoFn} and call {@link DoFn.Setup} on the first access; call {@link
     * DoFn.Teardown} only explicitly.
     */
    CLONE_ONCE,
    /**
     * Do not clone the {@link DoFn}; call {@link DoFn.Setup} on the first access; call {@link
     * DoFn.Teardown} only explicitly.
     */
    DO_NOT_CLONE
  }

  /**
   * Instruct this {@link DoFnTester} whether or not to clone the {@link DoFn} under test.
   */
  public void setCloningBehavior(CloningBehavior newValue) {
    checkState(state == State.UNINITIALIZED, "Wrong state: %s", state);
    this.cloningBehavior = newValue;
  }

  /**
   *  Indicates whether this {@link DoFnTester} will clone the {@link DoFn} under test.
   */
  public CloningBehavior getCloningBehavior() {
    return cloningBehavior;
  }

  /**
   * A convenience operation that first calls {@link #startBundle},
   * then calls {@link #processElement} on each of the input elements, then
   * calls {@link #finishBundle}, then returns the result of
   * {@link #takeOutputElements}.
   */
  public List<OutputT> processBundle(Iterable <? extends InputT> inputElements) throws Exception {
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
   *   <li>Calls {@link #processElement} on each of the arguments.</li>
   *   <li>Calls {@link #finishBundle}.</li>
   *   <li>Returns the result of {@link #takeOutputElements}.</li>
   * </ol>
   */
  @SafeVarargs
  public final List<OutputT> processBundle(InputT... inputElements) throws Exception {
    return processBundle(Arrays.asList(inputElements));
  }

  /**
   * Calls the {@link DoFn.StartBundle} method on the {@link DoFn} under test.
   *
   * <p>If needed, first creates a fresh instance of the {@link DoFn} under test and calls
   * {@link DoFn.Setup}.
   */
  public void startBundle() throws Exception {
    checkState(
        state == State.UNINITIALIZED || state == State.BUNDLE_FINISHED,
        "Wrong state during startBundle: %s",
        state);
    if (state == State.UNINITIALIZED) {
      initializeState();
    }
    TestContext<InputT, OutputT> context = createContext(fn);
    context.setupDelegateAggregators();
    try {
      fn.startBundle(context);
    } catch (UserCodeException e) {
      unwrapUserCodeException(e);
    }
    state = State.BUNDLE_STARTED;
  }

  private static void unwrapUserCodeException(UserCodeException e) throws Exception {
    if (e.getCause() instanceof Exception) {
      throw (Exception) e.getCause();
    } else if (e.getCause() instanceof Error) {
      throw (Error) e.getCause();
    } else {
      throw e;
    }
  }

  /**
   * Calls the {@link DoFn.ProcessElement} method on the {@link DoFn} under test, in a
   * context where {@link DoFn.ProcessContext#element} returns the
   * given element.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   *
   * @throws IllegalStateException if the {@code DoFn} under test has already
   * been finished
   */
  public void processElement(InputT element) throws Exception {
    processTimestampedElement(TimestampedValue.atMinimumTimestamp(element));
  }

  /**
   * Calls {@link OldDoFn#processElement} on the {@code OldDoFn} under test, in a
   * context where {@link OldDoFn.ProcessContext#element} returns the
   * given element and timestamp.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   *
   * <p>If the input timestamp is {@literal null}, the minimum timestamp will be used.
   */
  public void processTimestampedElement(TimestampedValue<InputT> element) throws Exception {
    checkNotNull(element, "Timestamped element cannot be null");
    if (state != State.BUNDLE_STARTED) {
      startBundle();
    }
    try {
      fn.processElement(createProcessContext(fn, element));
    } catch (UserCodeException e) {
      unwrapUserCodeException(e);
    }
  }

  /**
   * Calls the {@link DoFn.FinishBundle} method of the {@link DoFn} under test.
   *
   * <p>If {@link #setCloningBehavior} was called with {@link CloningBehavior#CLONE_PER_BUNDLE},
   * then also calls {@link DoFn.Teardown} on the {@link DoFn}, and it will be cloned and
   * {@link DoFn.Setup} again when processing the next bundle.
   *
   * @throws IllegalStateException if {@link DoFn.FinishBundle} has already been called
   * for this bundle.
   */
  public void finishBundle() throws Exception {
    checkState(
        state == State.BUNDLE_STARTED,
        "Must be inside bundle to call finishBundle, but was: %s",
        state);
    try {
      fn.finishBundle(createContext(fn));
    } catch (UserCodeException e) {
      unwrapUserCodeException(e);
    }
    if (cloningBehavior == CloningBehavior.CLONE_PER_BUNDLE) {
      fn.teardown();
      fn = null;
      state = State.UNINITIALIZED;
    } else {
      state = State.BUNDLE_FINISHED;
    }
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
        new Function<TimestampedValue<OutputT>, OutputT>() {
          @Override
          @SuppressWarnings("unchecked")
          public OutputT apply(TimestampedValue<OutputT> input) {
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
  public List<TimestampedValue<OutputT>> peekOutputElementsWithTimestamp() {
    // TODO: Should we return an unmodifiable list?
    return Lists.transform(getOutput(mainOutputTag),
        new Function<WindowedValue<OutputT>, TimestampedValue<OutputT>>() {
          @Override
          @SuppressWarnings("unchecked")
          public TimestampedValue<OutputT> apply(WindowedValue<OutputT> input) {
            return TimestampedValue.of(input.getValue(), input.getTimestamp());
          }
        });
  }

  /**
   * Returns the elements output so far to the main output in the provided window with associated
   * timestamps.
   */
  public List<TimestampedValue<OutputT>> peekOutputElementsInWindow(BoundedWindow window) {
    return peekOutputElementsInWindow(mainOutputTag, window);
  }

  /**
   * Returns the elements output so far to the specified output in the provided window with
   * associated timestamps.
   */
  public List<TimestampedValue<OutputT>> peekOutputElementsInWindow(
      TupleTag<OutputT> tag,
      BoundedWindow window) {
    ImmutableList.Builder<TimestampedValue<OutputT>> valuesBuilder = ImmutableList.builder();
    for (WindowedValue<OutputT> value : getOutput(tag)) {
      if (value.getWindows().contains(window)) {
        valuesBuilder.add(TimestampedValue.of(value.getValue(), value.getTimestamp()));
      }
    }
    return valuesBuilder.build();
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
  public List<TimestampedValue<OutputT>> takeOutputElementsWithTimestamp() {
    List<TimestampedValue<OutputT>> resultElems =
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
    return Lists.transform(getOutput(tag),
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
    return extractAggregatorValue(agg.getName(), agg.getCombineFn());
  }

  private <AccumT, AggregateT> AggregateT extractAggregatorValue(
      String name, CombineFn<?, AccumT, AggregateT> combiner) {
    @SuppressWarnings("unchecked")
    AccumT accumulator = (AccumT) accumulators.get(name);
    if (accumulator == null) {
      accumulator = combiner.createAccumulator();
    }
    return combiner.extractOutput(accumulator);
  }

  private <T> List<WindowedValue<T>> getOutput(TupleTag<T> tag) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<WindowedValue<T>> elems = (List) outputs.get(tag);
    return MoreObjects.firstNonNull(elems, Collections.<WindowedValue<T>>emptyList());
  }

  private TestContext<InputT, OutputT> createContext(OldDoFn<InputT, OutputT> fn) {
    return new TestContext<>(fn, options, mainOutputTag, outputs, accumulators);
  }

  private static class TestContext<InT, OutT> extends OldDoFn<InT, OutT>.Context {
    private final PipelineOptions opts;
    private final TupleTag<OutT> mainOutputTag;
    private final Map<TupleTag<?>, List<WindowedValue<?>>> outputs;
    private final Map<String, Object> accumulators;

    public TestContext(
        OldDoFn<InT, OutT> fn,
        PipelineOptions opts,
        TupleTag<OutT> mainOutputTag,
        Map<TupleTag<?>, List<WindowedValue<?>>> outputs,
        Map<String, Object> accumulators) {
      fn.super();
      this.opts = opts;
      this.mainOutputTag = mainOutputTag;
      this.outputs = outputs;
      this.accumulators = accumulators;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return opts;
    }

    @Override
    public void output(OutT output) {
      sideOutput(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutT output, Instant timestamp) {
      sideOutputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    protected <AggInT, AggOutT> Aggregator<AggInT, AggOutT> createAggregatorInternal(
        final String name, final CombineFn<AggInT, ?, AggOutT> combiner) {
      return aggregator(name, combiner);
    }

    private <AinT, AccT, AoutT> Aggregator<AinT, AoutT> aggregator(
        final String name,
        final CombineFn<AinT, AccT, AoutT> combiner) {
      Aggregator<AinT, AoutT> aggregator = new Aggregator<AinT, AoutT>() {
        @Override
        public void addValue(AinT value) {
          AccT accum = (AccT) accumulators.get(name);
          AccT newAccum = combiner.addInput(accum, value);
          accumulators.put(name, newAccum);
        }

        @Override
        public String getName() {
          return name;
        }

        @Override
        public CombineFn<AinT, ?, AoutT> getCombineFn() {
          return combiner;
        }
      };
      accumulators.put(name, combiner.createAccumulator());
      return aggregator;
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {

    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      sideOutputWithTimestamp(tag, output, BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    public <T> void noteOutput(TupleTag<T> tag, WindowedValue<T> output) {
      getOutputList(tag).add(output);
    }

    private <T> List<WindowedValue<T>> getOutputList(TupleTag<T> tag) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<WindowedValue<T>> outputList = (List) outputs.get(tag);
      if (outputList == null) {
        outputList = new ArrayList<>();
        outputs.put(tag, (List) outputList);
      }
      return outputList;
    }
  }

  private TestProcessContext<InputT, OutputT> createProcessContext(
      OldDoFn<InputT, OutputT> fn,
      TimestampedValue<InputT> elem) {
    WindowedValue<InputT> windowedValue = WindowedValue.timestampedValueInGlobalWindow(
        elem.getValue(), elem.getTimestamp());

    return new TestProcessContext<>(fn,
        createContext(fn),
        windowedValue,
        mainOutputTag,
        sideInputs);
  }

  private static class TestProcessContext<InT, OutT> extends OldDoFn<InT, OutT>.ProcessContext {
    private final TestContext<InT, OutT> context;
    private final TupleTag<OutT> mainOutputTag;
    private final WindowedValue<InT> element;
    private final Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs;

    private TestProcessContext(
        OldDoFn<InT, OutT> fn,
        TestContext<InT, OutT> context,
        WindowedValue<InT> element,
        TupleTag<OutT> mainOutputTag,
        Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs) {
      fn.super();
      this.context = context;
      this.element = element;
      this.mainOutputTag = mainOutputTag;
      this.sideInputs = sideInputs;
    }

    @Override
    public InT element() {
      return element.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      Map<BoundedWindow, ?> viewValues = sideInputs.get(view);
      if (viewValues != null) {
        BoundedWindow sideInputWindow =
            view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(window());
        @SuppressWarnings("unchecked")
        T windowValue = (T) viewValues.get(sideInputWindow);
        if (windowValue != null) {
          return windowValue;
        }
      }
      return view.getViewFn().apply(Collections.<WindowedValue<?>>emptyList());
    }

    @Override
    public Instant timestamp() {
      return element.getTimestamp();
    }

    @Override
    public BoundedWindow window() {
      return Iterables.getOnlyElement(element.getWindows());
    }

    @Override
    public PaneInfo pane() {
      return element.getPane();
    }

    @Override
    public WindowingInternals<InT, OutT> windowingInternals() {
      return new WindowingInternals<InT, OutT>() {
        StateInternals<?> stateInternals = InMemoryStateInternals.forKey(new Object());

        @Override
        public StateInternals<?> stateInternals() {
          return stateInternals;
        }

        @Override
        public void outputWindowedValue(
            OutT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          context.noteOutput(mainOutputTag, WindowedValue.of(output, timestamp, windows, pane));
        }

        @Override
        public TimerInternals timerInternals() {
          throw
              new UnsupportedOperationException("Timer Internals are not supported in DoFnTester");
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return element.getWindows();
        }

        @Override
        public PaneInfo pane() {
          return element.getPane();
        }

        @Override
        public <T> void writePCollectionViewData(
            TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder)
            throws IOException {
          throw new UnsupportedOperationException(
              "WritePCollectionViewData is not supported in in the context of DoFnTester");
        }

        @Override
        public <T> T sideInput(
            PCollectionView<T> view, BoundedWindow mainInputWindow) {
          throw new UnsupportedOperationException(
              "SideInput from WindowingInternals is not supported in in the context of DoFnTester");
        }
      };
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public void output(OutT output) {
      sideOutput(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutT output, Instant timestamp) {
      sideOutputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      sideOutputWithTimestamp(tag, output, element.getTimestamp());
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.noteOutput(tag,
          WindowedValue.of(output, timestamp, element.getWindows(), element.getPane()));
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      throw new IllegalStateException("Aggregators should not be created within ProcessContext. "
          + "Instead, create an aggregator at OldDoFn construction time with"
          + " createAggregator, and ensure they are set up by the time startBundle is"
          + " called with setupDelegateAggregators.");
    }
  }

  @Override
  public void close() throws Exception {
    if (state == State.BUNDLE_STARTED) {
      finishBundle();
    }
    if (state == State.BUNDLE_FINISHED) {
      fn.teardown();
      fn = null;
    }
    state = State.TORN_DOWN;
  }

  /////////////////////////////////////////////////////////////////////////////

  /** The possible states of processing a {@link DoFn}. */
  private enum State {
    UNINITIALIZED,
    BUNDLE_STARTED,
    BUNDLE_FINISHED,
    TORN_DOWN
  }

  private final PipelineOptions options = PipelineOptionsFactory.create();

  /** The original {@link OldDoFn} under test. */
  private final OldDoFn<InputT, OutputT> origFn;

  /**
   * Whether to clone the original {@link DoFn} or just use it as-is.
   *
   * <p>Worker-side {@link DoFn DoFns} may not be serializable, and are not required to be.
   */
  private CloningBehavior cloningBehavior = CloningBehavior.CLONE_ONCE;

  /** The side input values to provide to the {@link DoFn} under test. */
  private Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs =
      new HashMap<>();

  private Map<String, Object> accumulators;

  /** The output tags used by the {@link DoFn} under test. */
  private TupleTag<OutputT> mainOutputTag = new TupleTag<>();

  /** The original OldDoFn under test, if started. */
  OldDoFn<InputT, OutputT> fn;

  /** The ListOutputManager to examine the outputs. */
  private Map<TupleTag<?>, List<WindowedValue<?>>> outputs;

  /** The state of processing of the {@link DoFn} under test. */
  private State state = State.UNINITIALIZED;

  private DoFnTester(OldDoFn<InputT, OutputT> origFn) {
    this.origFn = origFn;
  }

  @SuppressWarnings("unchecked")
  private void initializeState() throws Exception {
    checkState(state == State.UNINITIALIZED, "Already initialized");
    checkState(fn == null, "Uninitialized but fn != null");
    if (cloningBehavior.equals(CloningBehavior.DO_NOT_CLONE)) {
      fn = origFn;
    } else {
      fn = (OldDoFn<InputT, OutputT>)
          SerializableUtils.deserializeFromByteArray(
              SerializableUtils.serializeToByteArray(origFn),
              origFn.toString());
    }
    fn.setup();
    outputs = new HashMap<>();
    accumulators = new HashMap<>();
  }
}
