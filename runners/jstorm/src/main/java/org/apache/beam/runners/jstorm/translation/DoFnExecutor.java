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
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.metric.MetricClient;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.jstorm.JStormPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JStorm {@link Executor} for {@link DoFn}.
 * @param <InputT> input type
 * @param <OutputT> output type
 */
class DoFnExecutor<InputT, OutputT> implements Executor {
  private static final long serialVersionUID = 5297603063991078668L;

  private static final Logger LOG = LoggerFactory.getLogger(DoFnExecutor.class);

  /**
   * Implements {@link OutputManager} in a DoFn executor.
   */
  public class DoFnExecutorOutputManager implements OutputManager, Serializable {
    private static final long serialVersionUID = -661113364735206170L;

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      executorsBolt.processExecutorElem(tag, output);
    }
  }

  protected transient DoFnRunner<InputT, OutputT> runner = null;
  protected transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackRunner = null;

  protected final String stepName;

  protected int internalDoFnExecutorId;

  protected final String description;

  protected final TupleTag<OutputT> mainTupleTag;
  protected final List<TupleTag<?>> sideOutputTags;

  protected SerializedPipelineOptions serializedOptions;
  protected transient JStormPipelineOptions pipelineOptions;

  protected DoFn<InputT, OutputT> doFn;
  protected final Coder<WindowedValue<InputT>> inputCoder;
  protected DoFnInvoker<InputT, OutputT> doFnInvoker;
  protected OutputManager outputManager;
  protected WindowingStrategy<?, ?> windowingStrategy;
  protected final TupleTag<InputT> mainInputTag;
  protected Collection<PCollectionView<?>> sideInputs;
  protected SideInputHandler sideInputHandler;
  protected final Map<TupleTag, PCollectionView<?>> sideInputTagToView;

  // Initialize during runtime
  protected ExecutorContext executorContext;
  protected ExecutorsBolt executorsBolt;
  protected TimerInternals timerInternals;
  protected transient StateInternals pushbackStateInternals;
  protected transient StateTag<BagState<WindowedValue<InputT>>> pushedBackTag;
  protected transient StateTag<WatermarkHoldState> watermarkHoldTag;
  protected transient IKvStoreManager kvStoreManager;
  protected DefaultStepContext stepContext;
  protected transient MetricClient metricClient;

  public DoFnExecutor(
      String stepName,
      String description,
      JStormPipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      Coder<WindowedValue<InputT>> inputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      TupleTag<InputT> mainInputTag,
      Collection<PCollectionView<?>> sideInputs,
      Map<TupleTag, PCollectionView<?>> sideInputTagToView,
      TupleTag<OutputT> mainTupleTag,
      List<TupleTag<?>> sideOutputTags) {
    this.stepName = checkNotNull(stepName, "stepName");
    this.description = checkNotNull(description, "description");
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputManager = new DoFnExecutorOutputManager();
    this.windowingStrategy = windowingStrategy;
    this.mainInputTag = mainInputTag;
    this.sideInputs = sideInputs;
    this.mainTupleTag = mainTupleTag;
    this.sideOutputTags = sideOutputTags;
    this.sideInputTagToView = sideInputTagToView;
  }

  protected DoFnRunner<InputT, OutputT> getDoFnRunner() {
    return new DoFnRunnerWithMetrics<>(
        stepName,
        DoFnRunners.simpleRunner(
            this.pipelineOptions,
            this.doFn,
            this.sideInputHandler == null ? NullSideInputReader.empty() : sideInputHandler,
            this.outputManager,
            this.mainTupleTag,
            this.sideOutputTags,
            this.stepContext,
            this.windowingStrategy),
        MetricsReporter.create(metricClient));
  }

  protected void initService(ExecutorContext context) {
    // TODO: what should be set for key in here?
    timerInternals = new JStormTimerInternals(
        null /* key */, this, context.getExecutorsBolt().timerService());
    kvStoreManager = context.getKvStoreManager();
    stepContext = new DefaultStepContext(timerInternals,
        new JStormStateInternals(
            null, kvStoreManager, executorsBolt.timerService(), internalDoFnExecutorId));
    metricClient = new MetricClient(executorContext.getTopologyContext());
  }

  @Override
  public void init(ExecutorContext context) {
    this.executorContext = context;
    this.executorsBolt = context.getExecutorsBolt();
    this.pipelineOptions =
        this.serializedOptions.getPipelineOptions().as(JStormPipelineOptions.class);

    initService(context);

    // Side inputs setup
    if (sideInputs != null && !sideInputs.isEmpty()) {
      pushedBackTag = StateTags.bag("pushed-back-values", inputCoder);
      watermarkHoldTag =
          StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);
      pushbackStateInternals = new JStormStateInternals(
          null, kvStoreManager, executorsBolt.timerService(), internalDoFnExecutorId);
      sideInputHandler = new SideInputHandler(sideInputs, pushbackStateInternals);
      runner = getDoFnRunner();
      pushbackRunner =
          SimplePushbackSideInputDoFnRunner.create(runner, sideInputs, sideInputHandler);
    } else {
      runner = getDoFnRunner();
    }

    // Process user's setup
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();
  }

  @Override
  public <T> void process(TupleTag<T> tag, WindowedValue<T> elem) {
    LOG.debug(String.format("process: elemTag=%s, mainInputTag=%s, sideInputs=%s, elem={}",
        tag, mainInputTag, sideInputs, elem.getValue()));
    if (mainInputTag.equals(tag)) {
      processMainInput(elem);
    } else {
      processSideInput(tag, elem);
    }
  }

  protected <T> void processMainInput(WindowedValue<T> elem) {
    if (sideInputs.isEmpty()) {
      runner.processElement((WindowedValue<InputT>) elem);
    } else {
      Iterable<WindowedValue<InputT>> justPushedBack =
          pushbackRunner.processElementInReadyWindows((WindowedValue<InputT>) elem);
      BagState<WindowedValue<InputT>> pushedBack =
          pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

      Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
        if (pushedBackValue.getTimestamp().isBefore(min)) {
          min = pushedBackValue.getTimestamp();
        }
        min = earlier(min, pushedBackValue.getTimestamp());
        pushedBack.add(pushedBackValue);
      }
      pushbackStateInternals.state(StateNamespaces.global(), watermarkHoldTag).add(min);
    }
  }

  protected void processSideInput(TupleTag tag, WindowedValue elem) {
    LOG.debug(String.format("side inputs: %s, %s.", tag, elem));

    PCollectionView<?> sideInputView = sideInputTagToView.get(tag);
    sideInputHandler.addSideInputValue(sideInputView, elem);

    BagState<WindowedValue<InputT>> pushedBack =
        pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

    List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

    Iterable<WindowedValue<InputT>> pushedBackInputs = pushedBack.read();
    if (pushedBackInputs != null) {
      for (WindowedValue<InputT> input : pushedBackInputs) {

        Iterable<WindowedValue<InputT>> justPushedBack =
            pushbackRunner.processElementInReadyWindows(input);
        Iterables.addAll(newPushedBack, justPushedBack);
      }
    }
    pushedBack.clear();

    Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
      min = earlier(min, pushedBackValue.getTimestamp());
      pushedBack.add(pushedBackValue);
    }

    WatermarkHoldState watermarkHold =
        pushbackStateInternals.state(StateNamespaces.global(), watermarkHoldTag);
    // TODO: clear-then-add is not thread-safe.
    watermarkHold.clear();
    watermarkHold.add(min);
  }

  /**
   * Process all pushed back elements when receiving watermark with max timestamp.
   */
  public void processAllPushBackElements() {
    if (sideInputs != null && !sideInputs.isEmpty()) {
      BagState<WindowedValue<InputT>> pushedBackElements =
          pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);
      if (pushedBackElements != null) {
        for (WindowedValue<InputT> elem : pushedBackElements.read()) {
          LOG.info("Process pushback elem={}", elem);
          runner.processElement(elem);
        }
        pushedBackElements.clear();
      }

      WatermarkHoldState watermarkHold =
          pushbackStateInternals.state(StateNamespaces.global(), watermarkHoldTag);
      watermarkHold.clear();
      watermarkHold.add(BoundedWindow.TIMESTAMP_MAX_VALUE);
    }
  }

  public void onTimer(Object key, TimerInternals.TimerData timerData) {
    StateNamespace namespace = timerData.getNamespace();
    checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
    BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    if (pushbackRunner != null) {
      pushbackRunner.onTimer(
          timerData.getTimerId(), window, timerData.getTimestamp(), timerData.getDomain());
    } else {
      runner.onTimer(
          timerData.getTimerId(), window, timerData.getTimestamp(), timerData.getDomain());
    }
  }

  @Override
  public void cleanup() {
    doFnInvoker.invokeTeardown();
  }

  @Override
  public String toString() {
    return description;
  }

  private Instant earlier(Instant left, Instant right) {
    return left.isBefore(right) ? left : right;
  }

  public void startBundle() {
    if (pushbackRunner != null) {
      pushbackRunner.startBundle();
    } else {
      runner.startBundle();
    }
  }

  public void finishBundle() {
    if (pushbackRunner != null) {
      pushbackRunner.finishBundle();
    } else {
      runner.finishBundle();
    }
  }

  public void setInternalDoFnExecutorId(int id) {
    this.internalDoFnExecutorId = id;
  }

  public int getInternalDoFnExecutorId() {
    return internalDoFnExecutorId;
  }
}
