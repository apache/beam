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

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresKeyedState;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Runs a DoFn by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the DoFn's (main) input elements
 * @param <OutputT> the type of the DoFn's (main) output elements
 * @param <ReceiverT> the type of object that receives outputs
 */
public class DoFnRunner<InputT, OutputT, ReceiverT> {

  /** Information about how to create output receivers and output to them. */
  public interface OutputManager<ReceiverT> {

    /** Returns the receiver to use for a given tag. */
    public ReceiverT initialize(TupleTag<?> tag);

    /** Outputs a single element to the provided receiver. */
    public void output(ReceiverT receiver, WindowedValue<?> output);

  }

  /** The DoFn being run. */
  public final DoFn<InputT, OutputT> fn;

  /** The context used for running the DoFn. */
  public final DoFnContext<InputT, OutputT, ReceiverT> context;

  DoFnRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      PTuple sideInputs,
      OutputManager<ReceiverT> outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = fn;
    this.context = new DoFnContext<>(
        options, fn, sideInputs, outputManager, mainOutputTag, sideOutputTags, stepContext,
        addCounterMutator, windowingStrategy == null ? null : windowingStrategy.getWindowFn());
  }

  public static <InputT, OutputT, ReceiverT> DoFnRunner<InputT, OutputT, ReceiverT> create(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      PTuple sideInputs,
      OutputManager<ReceiverT> outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new DoFnRunner<>(
        options, fn, sideInputs, outputManager,
        mainOutputTag, sideOutputTags, stepContext, addCounterMutator, windowingStrategy);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT, List> createWithListOutputs(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      PTuple sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    return create(
        options, fn, sideInputs,
        new OutputManager<List>() {
          @Override
          public List initialize(TupleTag<?> tag) {
            return new ArrayList<>();
          }
          @Override
          public void output(List list, WindowedValue<?> output) {
            list.add(output);
          }
        },
        mainOutputTag, sideOutputTags, stepContext, addCounterMutator, windowingStrategy);
  }

  /** Calls {@link DoFn#startBundle}. */
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.startBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /**
   * Calls {@link DoFn#processElement} with a ProcessContext containing
   * the current element.
   */
  public void processElement(WindowedValue<InputT> elem) {
    if (elem.getWindows().size() <= 1
        || (!RequiresWindowAccess.class.isAssignableFrom(fn.getClass())
            && context.sideInputs.getAll().isEmpty())) {
      invokeProcessElement(elem);
    } else {
      // We could modify the windowed value (and the processContext) to
      // avoid repeated allocations, but this is more straightforward.
      for (BoundedWindow window : elem.getWindows()) {
        invokeProcessElement(WindowedValue.of(
            elem.getValue(), elem.getTimestamp(), window));
      }
    }
  }

  protected void invokeProcessElement(WindowedValue<InputT> elem) {
    DoFn<InputT, OutputT>.ProcessContext processContext = createProcessContext(elem);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /** Calls {@link DoFn#finishBundle}. */
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.finishBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /** Returns the receiver who gets outputs with the provided tag. */
  public ReceiverT getReceiver(TupleTag<?> tag) {
    return context.getReceiver(tag);
  }

  /**
   * A concrete implementation of {@link DoFn<InputT, OutputT>.Context} used for running
   * a {@link DoFn}.
   *
   * @param <InputT> the type of the DoFn's (main) input elements
   * @param <OutputT> the type of the DoFn's (main) output elements
   * @param <R> the type of object that receives outputs
   */
  private static class DoFnContext<InputT, OutputT, ReceiverT>
      extends DoFn<InputT, OutputT>.Context {
    private static final int MAX_SIDE_OUTPUTS = 1000;

    final PipelineOptions options;
    final DoFn<InputT, OutputT> fn;
    final PTuple sideInputs;
    final Map<TupleTag<?>, Map<BoundedWindow, Object>> sideInputCache;
    final OutputManager<ReceiverT> outputManager;
    final Map<TupleTag<?>, ReceiverT> outputMap;
    final TupleTag<OutputT> mainOutputTag;
    final StepContext stepContext;
    final CounterSet.AddCounterMutator addCounterMutator;
    final WindowFn windowFn;

    public DoFnContext(PipelineOptions options,
                       DoFn<InputT, OutputT> fn,
                       PTuple sideInputs,
                       OutputManager<ReceiverT> outputManager,
                       TupleTag<OutputT> mainOutputTag,
                       List<TupleTag<?>> sideOutputTags,
                       StepContext stepContext,
                       CounterSet.AddCounterMutator addCounterMutator,
                       WindowFn windowFn) {
      fn.super();
      this.options = options;
      this.fn = fn;
      this.sideInputs = sideInputs;
      this.sideInputCache = new HashMap<>();
      this.outputManager = outputManager;
      this.mainOutputTag = mainOutputTag;
      this.outputMap = new HashMap<>();
      outputMap.put(mainOutputTag, outputManager.initialize(mainOutputTag));
      for (TupleTag<?> sideOutputTag : sideOutputTags) {
        outputMap.put(sideOutputTag, outputManager.initialize(sideOutputTag));
      }
      this.stepContext = stepContext;
      this.addCounterMutator = addCounterMutator;
      this.windowFn = windowFn;
      super.setupDelegateAggregators();
    }

    public ReceiverT getReceiver(TupleTag<?> tag) {
      ReceiverT receiver = outputMap.get(tag);
      if (receiver == null) {
        throw new IllegalArgumentException(
            "calling getReceiver() with unknown tag " + tag);
      }
      return receiver;
    }

    //////////////////////////////////////////////////////////////////////////////

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @SuppressWarnings("unchecked")
    <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
      return stepContext.getExecutionContext().getSideInput(view, mainInputWindow, sideInputs);
    }

    <T> WindowedValue<T> makeWindowedValue(
        T output, Instant timestamp, Collection<? extends BoundedWindow> windows) {
      final Instant inputTimestamp = timestamp;

      if (timestamp == null) {
        timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      if (windows == null) {
        try {
          windows = windowFn.assignWindows(windowFn.new AssignContext() {
            @Override
            public Object element() {
              throw new UnsupportedOperationException(
                  "WindowFn attempted to access input element when none was available");
            }

            @Override
            public Instant timestamp() {
              if (inputTimestamp == null) {
                throw new UnsupportedOperationException(
                    "WindowFn attempted to access input timestamp when none was available");
              }
              return inputTimestamp;
            }

            @Override
            public Collection<? extends BoundedWindow> windows() {
              throw new UnsupportedOperationException(
                  "WindowFn attempted to access input windows when none were available");
            }
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      return WindowedValue.of(output, timestamp, windows);
    }

    void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
      WindowedValue<OutputT> windowedElem = makeWindowedValue(output, timestamp, windows);
      outputManager.output(outputMap.get(mainOutputTag), windowedElem);
      if (stepContext != null) {
        stepContext.noteOutput(windowedElem);
      }
    }

    protected <T> void sideOutputWindowedValue(TupleTag<T> tag,
                                               T output,
                                               Instant timestamp,
                                               Collection<? extends BoundedWindow> windows) {
      ReceiverT receiver = outputMap.get(tag);
      if (receiver == null) {
        // This tag wasn't declared nor was it seen before during this execution.
        // Thus, this must be a new, undeclared and unconsumed output.

        // To prevent likely user errors, enforce the limit on the number of side
        // outputs.
        if (outputMap.size() >= MAX_SIDE_OUTPUTS) {
          throw new IllegalArgumentException(
              "the number of side outputs has exceeded a limit of "
              + MAX_SIDE_OUTPUTS);
        }

        // Register the new TupleTag with outputManager and add an entry for it in
        // the outputMap.
        receiver = outputManager.initialize(tag);
        outputMap.put(tag, receiver);
      }

      WindowedValue<T> windowedElem = makeWindowedValue(output, timestamp, windows);
      outputManager.output(receiver, windowedElem);
      if (stepContext != null) {
        stepContext.noteSideOutput(tag, windowedElem);
      }
    }

    // Following implementations of output, outputWithTimestamp, and sideOutput
    // are only accessible in DoFn.startBundle and DoFn.finishBundle, and will be shadowed by
    // ProcessContext's versions in DoFn.processElement.
    @Override
    public void output(OutputT output) {
      outputWindowedValue(output, null, null);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWindowedValue(output, timestamp, null);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      Preconditions.checkNotNull(tag, "TupleTag passed to sideOutput cannot be null");
      sideOutputWindowedValue(tag, output, null, null);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Preconditions.checkNotNull(tag, "TupleTag passed to sideOutputWithTimestamp cannot be null");
      sideOutputWindowedValue(tag, output, timestamp, null);
    }

    private String generateInternalAggregatorName(String userName) {
      return "user-" + stepContext.getStepName() + "-" + userName;
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      Preconditions.checkNotNull(combiner,
          "Combiner passed to createAggregator cannot be null");
      return new CounterAggregator<>(generateInternalAggregatorName(name),
          combiner, addCounterMutator);
    }

  }

  /**
   * Returns a new {@link DoFn.ProcessContext} for the given element.
   */
  protected DoFn<InputT, OutputT>.ProcessContext createProcessContext(WindowedValue<InputT> elem) {
    return new DoFnProcessContext<InputT, OutputT>(fn, context, elem);
  }

  /**
   * A concrete implementation of {@link DoFn<InputT, OutputT>.ProcessContext} used for running
   * a {@link DoFn} over a single element.
   *
   * @param <InputT> the type of the DoFn's (main) input elements
   * @param <OutputT> the type of the DoFn's (main) output elements
   */
  private static class DoFnProcessContext<InputT, OutputT>
      extends DoFn<InputT, OutputT>.ProcessContext {


    final DoFn<InputT, OutputT> fn;
    final DoFnContext<InputT, OutputT, ?> context;
    final WindowedValue<InputT> windowedValue;

    public DoFnProcessContext(DoFn<InputT, OutputT> fn,
                              DoFnContext<InputT, OutputT, ?> context,
                              WindowedValue<InputT> windowedValue) {
      fn.super();
      this.fn = fn;
      this.context = context;
      this.windowedValue = windowedValue;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public InputT element() {
      return windowedValue.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      Preconditions.checkNotNull(view, "View passed to sideInput cannot be null");
      Iterator<? extends BoundedWindow> windowIter = windows().iterator();
      BoundedWindow window;
      if (!windowIter.hasNext()) {
        if (context.windowFn instanceof GlobalWindows) {
          // TODO: Remove this once GroupByKeyOnly no longer outputs elements
          // without windows
          window = GlobalWindow.INSTANCE;
        } else {
          throw new IllegalStateException(
              "sideInput called when main input element is not in any windows");
        }
      } else {
        window = windowIter.next();
        if (windowIter.hasNext()) {
          throw new IllegalStateException(
              "sideInput called when main input element is in multiple windows");
        }
      }
      return context.sideInput(view, window);
    }

    @Override
    public KeyedState keyedState() {
      if (!(fn instanceof RequiresKeyedState)
          || !equivalentToKV(element())) {
        throw new UnsupportedOperationException(
            "Keyed state is only available in the context of a keyed DoFn "
            + "marked as requiring state");
      }

      return context.stepContext;
    }

    @Override
    public BoundedWindow window() {
      if (!(fn instanceof RequiresWindowAccess)) {
        throw new UnsupportedOperationException(
            "window() is only available in the context of a DoFn marked as RequiresWindow.");
      }
      return Iterables.getOnlyElement(windows());
    }

    @Override
    public void output(OutputT output) {
      context.outputWindowedValue(output, windowedValue.getTimestamp(), windowedValue.getWindows());
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkTimestamp(timestamp);
      context.outputWindowedValue(output, timestamp, windowedValue.getWindows());
    }

    void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
      context.outputWindowedValue(output, timestamp, windows);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      Preconditions.checkNotNull(tag, "Tag passed to sideOutput cannot be null");
      context.sideOutputWindowedValue(tag,
                                      output,
                                      windowedValue.getTimestamp(),
                                      windowedValue.getWindows());
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Preconditions.checkNotNull(tag, "Tag passed to sideOutputWithTimestamp cannot be null");
      checkTimestamp(timestamp);
      context.sideOutputWindowedValue(tag, output, timestamp, windowedValue.getWindows());
    }

    @Override
    public Instant timestamp() {
      return windowedValue.getTimestamp();
    }

    public Collection<? extends BoundedWindow> windows() {
      return windowedValue.getWindows();
    }

    private void checkTimestamp(Instant timestamp) {
      Preconditions.checkArgument(
          !timestamp.isBefore(windowedValue.getTimestamp().minus(fn.getAllowedTimestampSkew())),
          "Timestamp %s exceeds allowed maximum skew.", timestamp);
    }

    private boolean equivalentToKV(InputT input) {
      if (input == null) {
        return true;
      } else if (input instanceof KV) {
        return true;
      } else if (input instanceof TimerOrElement) {
        return ((TimerOrElement) input).isTimer()
            || ((TimerOrElement) input).element() instanceof KV;
      }
      return false;
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      return new WindowingInternals<InputT, OutputT>() {
        @Override
        public void outputWindowedValue(OutputT output, Instant timestamp,
            Collection<? extends BoundedWindow> windows) {
          context.outputWindowedValue(output, timestamp, windows);
        }

        @Override
        public <T> void writeToTagList(CodedTupleTag<T> tag, T value)
            throws IOException {
          context.stepContext.writeToTagList(tag, value);
        }

        @Override
        public <T> void deleteTagList(CodedTupleTag<T> tag) {
          context.stepContext.deleteTagList(tag);
        }

        @Override
        public <T> Iterable<T> readTagList(CodedTupleTag<T> tag)
            throws IOException {
          return context.stepContext.readTagList(tag);
        }

        @Override
        public <T> Map<CodedTupleTag<T>, Iterable<T>> readTagList(List<CodedTupleTag<T>> tags)
            throws IOException {
          return context.stepContext.readTagLists(tags);
        }

        @Override
        public void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain) {
          context.stepContext.getExecutionContext().setTimer(timer, timestamp, domain);
        }

        @Override
        public void deleteTimer(String timer, Trigger.TimeDomain domain) {
          context.stepContext.getExecutionContext().deleteTimer(timer, domain);
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return windowedValue.getWindows();
        }

        @Override
        public <T> void writePCollectionViewData(
            TupleTag<?> tag,
            Iterable<WindowedValue<T>> data,
            Coder<T> elemCoder) throws IOException {
          Coder<BoundedWindow> windowCoder = context.windowFn.windowCoder();

          context.stepContext.getExecutionContext().writePCollectionViewData(
              tag, data, IterableCoder.of(WindowedValue.getFullCoder(elemCoder, windowCoder)),
              window(), windowCoder);
        }

        @Override
        public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp) throws IOException {
          context.stepContext.store(tag, value, timestamp);
        }
      };
    }

    @Override
    protected <InputT, OutputT> Aggregator<InputT, OutputT> createAggregatorInternal(String name,
        CombineFn<InputT, ?, OutputT> combiner) {
      return context.createAggregatorInternal(name, combiner);
    }
  }
}
