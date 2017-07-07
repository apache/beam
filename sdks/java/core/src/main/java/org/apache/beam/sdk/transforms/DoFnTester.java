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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
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
 * fnTester.setOutputTags(...);  // If fn writes to more than one output.
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
   *
   * <p>The only supported extra parameter of the {@link DoFn.ProcessElement} method is
   * {@link BoundedWindow}.
   */
  @SuppressWarnings("unchecked")
  public static <InputT, OutputT> DoFnTester<InputT, OutputT> of(DoFn<InputT, OutputT> fn) {
    checkNotNull(fn, "fn can't be null");
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

  public PipelineOptions getPipelineOptions() {
    return options;
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
    try {
      fnInvoker.invokeStartBundle(new TestStartBundleContext());
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
   * given element and the element is in the global window.
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
   * Calls {@link DoFn.ProcessElement} on the {@code DoFn} under test, in a
   * context where {@link DoFn.ProcessContext#element} returns the
   * given element and timestamp and the element is in the global window.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   */
  public void processTimestampedElement(TimestampedValue<InputT> element) throws Exception {
    checkNotNull(element, "Timestamped element cannot be null");
    processWindowedElement(
        element.getValue(), element.getTimestamp(), GlobalWindow.INSTANCE);
  }

  /**
   * Calls {@link DoFn.ProcessElement} on the {@code DoFn} under test, in a
   * context where {@link DoFn.ProcessContext#element} returns the
   * given element and timestamp and the element is in the given window.
   *
   * <p>Will call {@link #startBundle} automatically, if it hasn't
   * already been called.
   */
  public void processWindowedElement(
      InputT element, Instant timestamp, final BoundedWindow window) throws Exception {
    if (state != State.BUNDLE_STARTED) {
      startBundle();
    }
    try {
      final DoFn<InputT, OutputT>.ProcessContext processContext =
          createProcessContext(
              ValueInSingleWindow.of(element, timestamp, window, PaneInfo.NO_FIRING));
      fnInvoker.invokeProcessElement(
          new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
            @Override
            public BoundedWindow window() {
              return window;
            }

            @Override
            public PipelineOptions pipelineOptions() {
              return getPipelineOptions();
            }

            @Override
            public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
                DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException(
                  "Not expected to access DoFn.StartBundleContext from @ProcessElement");
            }

            @Override
            public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
                DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException(
                  "Not expected to access DoFn.FinishBundleContext from @ProcessElement");
            }

            @Override
            public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
              return processContext;
            }

            @Override
            public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException("DoFnTester doesn't support timers yet.");
            }

            @Override
            public RestrictionTracker<?> restrictionTracker() {
              throw new UnsupportedOperationException(
                  "Not expected to access RestrictionTracker from a regular DoFn in DoFnTester");
            }

            @Override
            public org.apache.beam.sdk.state.State state(String stateId) {
              throw new UnsupportedOperationException("DoFnTester doesn't support state yet");
            }

            @Override
            public Timer timer(String timerId) {
              throw new UnsupportedOperationException("DoFnTester doesn't support timers yet");
            }
          });
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
      fnInvoker.invokeFinishBundle(new TestFinishBundleContext());
    } catch (UserCodeException e) {
      unwrapUserCodeException(e);
    }
    if (cloningBehavior == CloningBehavior.CLONE_PER_BUNDLE) {
      fnInvoker.invokeTeardown();
      fn = null;
      fnInvoker = null;
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
    return Lists.transform(getImmutableOutput(mainOutputTag),
        new Function<ValueInSingleWindow<OutputT>, TimestampedValue<OutputT>>() {
          @Override
          @SuppressWarnings("unchecked")
          public TimestampedValue<OutputT> apply(ValueInSingleWindow<OutputT> input) {
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
    for (ValueInSingleWindow<OutputT> value : getImmutableOutput(tag)) {
      if (value.getWindow().equals(window)) {
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
    getMutableOutput(mainOutputTag).clear();
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
   * Returns the elements output so far to the output with the
   * given tag.  Does not clear them, so subsequent calls will
   * continue to include these elements.
   *
   * @see #takeOutputElements
   * @see #clearOutputElements
   */
  public <T> List<T> peekOutputElements(TupleTag<T> tag) {
    // TODO: Should we return an unmodifiable list?
    return Lists.transform(getImmutableOutput(tag),
        new Function<ValueInSingleWindow<T>, T>() {
          @SuppressWarnings("unchecked")
          @Override
          public T apply(ValueInSingleWindow<T> input) {
            return input.getValue();
          }});
  }

  /**
   * Clears the record of the elements output so far to the output with the given tag.
   *
   * @see #peekOutputElements
   */
  public <T> void clearOutputElements(TupleTag<T> tag) {
    getMutableOutput(tag).clear();
  }

  /**
   * Returns the elements output so far to the output with the given tag.
   * Clears the list so these elements don't appear in future calls.
   *
   * @see #peekOutputElements
   */
  public <T> List<T> takeOutputElements(TupleTag<T> tag) {
    List<T> resultElems = new ArrayList<>(peekOutputElements(tag));
    clearOutputElements(tag);
    return resultElems;
  }

  private <T> List<ValueInSingleWindow<T>> getImmutableOutput(TupleTag<T> tag) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<ValueInSingleWindow<T>> elems = (List) outputs.get(tag);
    return ImmutableList.copyOf(
        MoreObjects.firstNonNull(elems, Collections.<ValueInSingleWindow<T>>emptyList()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> List<ValueInSingleWindow<T>> getMutableOutput(TupleTag<T> tag) {
    List<ValueInSingleWindow<T>> outputList = (List) outputs.get(tag);
    if (outputList == null) {
      outputList = new ArrayList<>();
      outputs.put(tag, (List) outputList);
    }
    return outputList;
  }

  public TupleTag<OutputT> getMainOutputTag() {
    return mainOutputTag;
  }

  private class TestStartBundleContext extends DoFn<InputT, OutputT>.StartBundleContext {

    private TestStartBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }
  }

  private class TestFinishBundleContext extends DoFn<InputT, OutputT>.FinishBundleContext {

    private TestFinishBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(
        OutputT output, Instant timestamp, BoundedWindow window) {
      output(mainOutputTag, output, timestamp, window);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      getMutableOutput(tag)
          .add(ValueInSingleWindow.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }
  }

  public DoFn<InputT, OutputT>.ProcessContext createProcessContext(
      ValueInSingleWindow<InputT> element) {
    return new TestProcessContext(element);
  }

  private class TestProcessContext extends DoFn<InputT, OutputT>.ProcessContext {
    private final ValueInSingleWindow<InputT> element;

    private TestProcessContext(ValueInSingleWindow<InputT> element) {
      fn.super();
      this.element = element;
    }

    @Override
    public InputT element() {
      return element.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      Map<BoundedWindow, ?> viewValues = sideInputs.get(view);
      if (viewValues != null) {
        BoundedWindow sideInputWindow =
            view.getWindowMappingFn()
                .getSideInputWindow(element.getWindow());
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
    public PaneInfo pane() {
      return element.getPane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(OutputT output) {
      output(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      outputWithTimestamp(tag, output, element.getTimestamp());
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      getMutableOutput(tag)
          .add(ValueInSingleWindow.of(output, timestamp, element.getWindow(), element.getPane()));
    }
  }

  @Override
  public void close() throws Exception {
    if (state == State.BUNDLE_STARTED) {
      finishBundle();
    }
    if (state == State.BUNDLE_FINISHED) {
      fnInvoker.invokeTeardown();
      fn = null;
      fnInvoker = null;
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

  /** The original {@link DoFn} under test. */
  private final DoFn<InputT, OutputT> origFn;

  /**
   * Whether to clone the original {@link DoFn} or just use it as-is.
   *
   * <p>Worker-side {@link DoFn DoFns} may not be serializable, and are not required to be.
   */
  private CloningBehavior cloningBehavior = CloningBehavior.CLONE_ONCE;

  /** The side input values to provide to the {@link DoFn} under test. */
  private Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs =
      new HashMap<>();

  /** The output tags used by the {@link DoFn} under test. */
  private TupleTag<OutputT> mainOutputTag = new TupleTag<>();

  /** The original DoFn under test, if started. */
  private DoFn<InputT, OutputT> fn;
  private DoFnInvoker<InputT, OutputT> fnInvoker;

  /** The outputs from the {@link DoFn} under test. */
  private Map<TupleTag<?>, List<ValueInSingleWindow<?>>> outputs;

  /** The state of processing of the {@link DoFn} under test. */
  private State state = State.UNINITIALIZED;

  private DoFnTester(DoFn<InputT, OutputT> origFn) {
    this.origFn = origFn;
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(origFn);
    for (DoFnSignature.Parameter param : signature.processElement().extraParameters()) {
      param.match(
          new DoFnSignature.Parameter.Cases.WithDefault<Void>() {
            @Override
            public Void dispatch(DoFnSignature.Parameter.ProcessContextParameter p) {
              // ProcessContext parameter is obviously supported.
              return null;
            }

            @Override
            public Void dispatch(DoFnSignature.Parameter.WindowParameter p) {
              // We also support the BoundedWindow parameter.
              return null;
            }

            @Override
            protected Void dispatchDefault(DoFnSignature.Parameter p) {
              throw new UnsupportedOperationException(
                  "Parameter " + p + " not supported by DoFnTester");
            }
          });
    }
  }

  @SuppressWarnings("unchecked")
  private void initializeState() throws Exception {
    checkState(state == State.UNINITIALIZED, "Already initialized");
    checkState(fn == null, "Uninitialized but fn != null");
    if (cloningBehavior.equals(CloningBehavior.DO_NOT_CLONE)) {
      fn = origFn;
    } else {
      fn = (DoFn<InputT, OutputT>)
          SerializableUtils.deserializeFromByteArray(
              SerializableUtils.serializeToByteArray(origFn),
              origFn.toString());
    }
    fnInvoker = DoFnInvokers.invokerFor(fn);
    fnInvoker.invokeSetup();
    outputs = new HashMap<>();
  }
}
