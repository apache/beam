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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StartBundleContext;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.BaseArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
@Deprecated
public class DoFnTester<InputT, OutputT> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DoFnTester.class);

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @SuppressWarnings("unchecked")
  @Deprecated
  public static <InputT, OutputT> DoFnTester<InputT, OutputT> of(DoFn<InputT, OutputT> fn) {
    checkNotNull(fn, "fn can't be null");
    LOG.warn(
        "Your tests use DoFnTester, which may not exercise DoFns correctly. "
            + "Please use TestPipeline instead.");
    return new DoFnTester<>(fn);
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void setSideInputs(Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs) {
    checkState(
        state == State.UNINITIALIZED,
        "Can't add side inputs: DoFnTester is already initialized, in state %s",
        state);
    this.sideInputs = sideInputs;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public PipelineOptions getPipelineOptions() {
    return options;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void setCloningBehavior(CloningBehavior newValue) {
    checkState(state == State.UNINITIALIZED, "Wrong state: %s", state);
    this.cloningBehavior = newValue;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public CloningBehavior getCloningBehavior() {
    return cloningBehavior;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<OutputT> processBundle(Iterable<? extends InputT> inputElements) throws Exception {
    startBundle();
    for (InputT inputElement : inputElements) {
      processElement(inputElement);
    }
    finishBundle();
    return takeOutputElements();
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  @SafeVarargs
  public final List<OutputT> processBundle(InputT... inputElements) throws Exception {
    return processBundle(Arrays.asList(inputElements));
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void processElement(InputT element) throws Exception {
    processTimestampedElement(TimestampedValue.atMinimumTimestamp(element));
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void processTimestampedElement(TimestampedValue<InputT> element) throws Exception {
    checkNotNull(element, "Timestamped element cannot be null");
    processWindowedElement(element.getValue(), element.getTimestamp(), GlobalWindow.INSTANCE);
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void processWindowedElement(InputT element, Instant timestamp, final BoundedWindow window)
      throws Exception {
    if (state != State.BUNDLE_STARTED) {
      startBundle();
    }
    try {
      final DoFn<InputT, OutputT>.ProcessContext processContext =
          createProcessContext(
              ValueInSingleWindow.of(element, timestamp, window, PaneInfo.NO_FIRING));
      fnInvoker.invokeProcessElement(
          new DoFnInvoker.BaseArgumentProvider<InputT, OutputT>() {

            @Override
            public String getErrorContext() {
              return "DoFnTester";
            }

            @Override
            public BoundedWindow window() {
              return window;
            }

            @Override
            public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
              return processContext.pane();
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
            public InputT element(DoFn<InputT, OutputT> doFn) {
              return processContext.element();
            }

            @Override
            public Object key() {
              throw new UnsupportedOperationException(
                  "Cannot access key as parameter outside of @OnTimer method.");
            }

            @Override
            public Instant timestamp(DoFn<InputT, OutputT> doFn) {
              return processContext.timestamp();
            }

            @Override
            public String timerId(DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException(
                  "Cannot access timerId as parameter outside of @OnTimer method.");
            }

            @Override
            public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException(
                  "Not expected to access TimeDomain from @ProcessElement");
            }

            @Override
            public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
              return DoFnOutputReceivers.windowedReceiver(processContext, null);
            }

            @Override
            public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
              return DoFnOutputReceivers.windowedMultiReceiver(processContext, null);
            }

            @Override
            public Object restriction() {
              throw new UnsupportedOperationException(
                  "Not expected to access Restriction from a regular DoFn in DoFnTester");
            }

            @Override
            public RestrictionTracker<?, ?> restrictionTracker() {
              throw new UnsupportedOperationException(
                  "Not expected to access RestrictionTracker from a regular DoFn in DoFnTester");
            }
          });
    } catch (UserCodeException e) {
      unwrapUserCodeException(e);
    }
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<OutputT> peekOutputElements() {
    return peekOutputElementsWithTimestamp().stream()
        .map(TimestampedValue::getValue)
        .collect(Collectors.toList());
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<TimestampedValue<OutputT>> peekOutputElementsWithTimestamp() {
    // TODO: Should we return an unmodifiable list?
    return getImmutableOutput(mainOutputTag).stream()
        .map(input -> TimestampedValue.of(input.getValue(), input.getTimestamp()))
        .collect(Collectors.toList());
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<TimestampedValue<OutputT>> peekOutputElementsInWindow(BoundedWindow window) {
    return peekOutputElementsInWindow(mainOutputTag, window);
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<TimestampedValue<OutputT>> peekOutputElementsInWindow(
      TupleTag<OutputT> tag, BoundedWindow window) {
    ImmutableList.Builder<TimestampedValue<OutputT>> valuesBuilder = ImmutableList.builder();
    for (ValueInSingleWindow<OutputT> value : getImmutableOutput(tag)) {
      if (value.getWindow().equals(window)) {
        valuesBuilder.add(TimestampedValue.of(value.getValue(), value.getTimestamp()));
      }
    }
    return valuesBuilder.build();
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public void clearOutputElements() {
    getMutableOutput(mainOutputTag).clear();
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<OutputT> takeOutputElements() {
    List<OutputT> resultElems = new ArrayList<>(peekOutputElements());
    clearOutputElements();
    return resultElems;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public List<TimestampedValue<OutputT>> takeOutputElementsWithTimestamp() {
    List<TimestampedValue<OutputT>> resultElems =
        new ArrayList<>(peekOutputElementsWithTimestamp());
    clearOutputElements();
    return resultElems;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public <T> List<T> peekOutputElements(TupleTag<T> tag) {
    // TODO: Should we return an unmodifiable list?
    return getImmutableOutput(tag).stream()
        .map(ValueInSingleWindow::getValue)
        .collect(Collectors.toList());
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public <T> void clearOutputElements(TupleTag<T> tag) {
    getMutableOutput(tag).clear();
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public <T> List<T> takeOutputElements(TupleTag<T> tag) {
    List<T> resultElems = new ArrayList<>(peekOutputElements(tag));
    clearOutputElements(tag);
    return resultElems;
  }

  private <T> List<ValueInSingleWindow<T>> getImmutableOutput(TupleTag<T> tag) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<ValueInSingleWindow<T>> elems = (List) getOutputs().get(tag);
    return ImmutableList.copyOf(MoreObjects.firstNonNull(elems, Collections.emptyList()));
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> List<ValueInSingleWindow<T>> getMutableOutput(TupleTag<T> tag) {
    List<ValueInSingleWindow<T>> outputList = (List) getOutputs().get(tag);
    if (outputList == null) {
      outputList = new ArrayList<>();
      getOutputs().put(tag, (List) outputList);
    }
    return outputList;
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
  public TupleTag<OutputT> getMainOutputTag() {
    return mainOutputTag;
  }

  private class TestStartBundleContext extends BaseArgumentProvider<InputT, OutputT> {
    @Override
    public StartBundleContext startBundleContext(DoFn doFn) {
      return fn.new StartBundleContext() {
        @Override
        public PipelineOptions getPipelineOptions() {
          return options;
        }
      };
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public String getErrorContext() {
      return "DoFnTester/StartBundle";
    }
  }

  private class TestFinishBundleContext extends BaseArgumentProvider<InputT, OutputT> {
    @Override
    public FinishBundleContext finishBundleContext(DoFn doFn) {
      return fn.new FinishBundleContext() {
        @Override
        public PipelineOptions getPipelineOptions() {
          return options;
        }

        @Override
        public void output(OutputT output, Instant timestamp, BoundedWindow window) {
          output(mainOutputTag, output, timestamp, window);
        }

        @Override
        public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
          getMutableOutput(tag)
              .add(ValueInSingleWindow.of(output, timestamp, window, PaneInfo.NO_FIRING));
        }
      };
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public String getErrorContext() {
      return "DoFnTester/FinishBundle";
    }
  }

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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
            view.getWindowMappingFn().getSideInputWindow(element.getWindow());
        @SuppressWarnings("unchecked")
        T windowValue = (T) viewValues.get(sideInputWindow);
        if (windowValue != null) {
          return windowValue;
        }
      }
      // Fallback to returning the default materialization if no data was supplied.
      // This is really to support singleton views with default values.
      switch (view.getViewFn().getMaterialization().getUrn()) {
        case Materializations.ITERABLE_MATERIALIZATION_URN:
          return ((ViewFn<Materializations.IterableView, T>) view.getViewFn())
              .apply(() -> Collections.emptyList());
        case Materializations.MULTIMAP_MATERIALIZATION_URN:
          return ((ViewFn<Materializations.MultimapView, T>) view.getViewFn())
              .apply(
                  new MultimapView() {
                    @Override
                    public Iterable get() {
                      return Collections.emptyList();
                    }

                    @Override
                    public Iterable get(@Nullable Object o) {
                      return Collections.emptyList();
                    }
                  });
        default:
          throw new IllegalStateException(
              String.format(
                  "Only materializations of type %s supported, received %s",
                  Arrays.asList(
                      Materializations.ITERABLE_MATERIALIZATION_URN,
                      Materializations.MULTIMAP_MATERIALIZATION_URN),
                  view.getViewFn().getMaterialization().getUrn()));
      }
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

  /** @deprecated Use {@link TestPipeline} with the {@code DirectRunner}. */
  @Deprecated
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
  private Map<PCollectionView<?>, Map<BoundedWindow, ?>> sideInputs = new HashMap<>();

  /** The output tags used by the {@link DoFn} under test. */
  private TupleTag<OutputT> mainOutputTag = new TupleTag<>();

  /** The original DoFn under test, if started. */
  private @Nullable DoFn<InputT, OutputT> fn;

  private @Nullable DoFnInvoker<InputT, OutputT> fnInvoker;

  /** The outputs from the {@link DoFn} under test. Access via {@link #getOutputs()}. */
  @CheckForNull private Map<TupleTag<?>, List<ValueInSingleWindow<?>>> outputs;

  /** The state of processing of the {@link DoFn} under test. */
  private State state = State.UNINITIALIZED;

  private DoFnTester(DoFn<InputT, OutputT> origFn) {
    this.origFn = origFn;
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(origFn);
    for (DoFnSignature.Parameter param : signature.processElement().extraParameters()) {
      param.match(
          new DoFnSignature.Parameter.Cases.WithDefault<Void>() {
            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.ProcessContextParameter p) {
              // ProcessContext parameter is obviously supported.
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.WindowParameter p) {
              // We also support the BoundedWindow parameter.
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.ElementParameter p) {
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.TimestampParameter p) {
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.TimeDomainParameter p) {
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.OutputReceiverParameter p) {
              return null;
            }

            @Override
            public @Nullable Void dispatch(
                DoFnSignature.Parameter.TaggedOutputReceiverParameter p) {
              return null;
            }

            @Override
            public @Nullable Void dispatch(DoFnSignature.Parameter.PaneInfoParameter p) {
              return null;
            }

            @Override
            public Void dispatch(DoFnSignature.Parameter.TimerIdParameter p) {
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
      fn =
          (DoFn<InputT, OutputT>)
              SerializableUtils.deserializeFromByteArray(
                  SerializableUtils.serializeToByteArray(origFn), origFn.toString());
    }
    fnInvoker = DoFnInvokers.invokerFor(fn);
    fnInvoker.invokeSetup();
  }

  private Map getOutputs() {
    if (outputs == null) {
      outputs = new HashMap<>();
    }
    return outputs;
  }
}
