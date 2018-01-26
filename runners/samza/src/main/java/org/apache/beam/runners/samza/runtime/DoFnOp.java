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

package org.apache.beam.runners.samza.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samza operator for {@link DoFn}.
 */
public class DoFnOp<InT, FnOutT, OutT> implements Op<InT, OutT> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnOp.class);

  private final TupleTag<FnOutT> mainOutputTag;
  private final DoFn<InT, FnOutT> doFn;
  private final Collection<PCollectionView<?>> sideInputs;
  private final List<TupleTag<?>> sideOutputTags;
  private final WindowingStrategy windowingStrategy;
  private final OutputManagerFactory<OutT> outputManagerFactory;
  // NOTE: we use HashMap here to guarantee Serializability
  private final HashMap<String, PCollectionView<?>> idToViewMap;
  private final String stepName;

  private transient StateInternalsFactory<Void> stateInternalsFactory;
  private transient SamzaTimerInternalsFactory<?> timerInternalsFactory;
  private transient DoFnRunner<InT, FnOutT> fnRunner;
  private transient PushbackSideInputDoFnRunner<InT, FnOutT> pushbackFnRunner;
  private transient SideInputHandler sideInputHandler;
  private transient DoFnInvoker<InT, FnOutT> doFnInvoker;

  // This is derivable from pushbackValues which is persisted to a store.
  // TODO: eagerly initialize the hold in init
  private transient Instant pushbackWatermarkHold;

  // TODO: add this to checkpointable state
  private transient Instant inputWatermark;
  private transient List<WindowedValue<InT>> pushbackValues;

  public DoFnOp(TupleTag<FnOutT> mainOutputTag,
                DoFn<InT, FnOutT> doFn,
                Collection<PCollectionView<?>> sideInputs,
                List<TupleTag<?>> sideOutputTags,
                WindowingStrategy windowingStrategy,
                Map<String, PCollectionView<?>> idToViewMap,
                OutputManagerFactory<OutT> outputManagerFactory,
                String stepName) {
    this.mainOutputTag = mainOutputTag;
    this.doFn = doFn;
    this.sideInputs = sideInputs;
    this.sideOutputTags = sideOutputTags;
    this.windowingStrategy = windowingStrategy;
    this.idToViewMap = new HashMap<>(idToViewMap);
    this.outputManagerFactory = outputManagerFactory;
    this.stepName = stepName;
  }

  @Override
  public void open(Config config,
                   TaskContext taskContext,
                   SamzaExecutionContext executionContext,
                   OpEmitter<OutT> emitter) {
    this.inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;

    this.timerInternalsFactory = new SamzaTimerInternalsFactory<>(null);

    @SuppressWarnings("unchecked")
    final KeyValueStore<byte[], byte[]> store =
        (KeyValueStore<byte[], byte[]>) taskContext.getStore("beamStore");

    this.stateInternalsFactory =
        new SamzaStoreStateInternals.Factory<>(
            mainOutputTag.getId(),
            store,
            VoidCoder.of());

    this.sideInputHandler = new SideInputHandler(
        sideInputs,
        stateInternalsFactory.stateInternalsForKey(null));

    final DoFnRunner<InT, FnOutT> doFnRunner = DoFnRunners.simpleRunner(
        Base64Serializer.deserializeUnchecked(
            config.get("beamPipelineOptions"),
            SerializablePipelineOptions.class).get(),
        doFn,
        sideInputHandler,
        outputManagerFactory.create(emitter),
        mainOutputTag,
        sideOutputTags,
        createStepContext(stateInternalsFactory, timerInternalsFactory),
        windowingStrategy);

    this.fnRunner = DoFnRunnerWithMetrics.wrap(
        doFnRunner, executionContext.getMetricsContainer(), stepName);

    this.pushbackFnRunner =
        SimplePushbackSideInputDoFnRunner.create(
            fnRunner,
            sideInputs,
            sideInputHandler);

    this.pushbackValues = new ArrayList<>();

    doFnInvoker = DoFnInvokers.invokerFor(doFn);

    doFnInvoker.invokeSetup();
  }

  @Override
  public void processElement(WindowedValue<InT> inputElement, OpEmitter<OutT> emitter) {
    pushbackFnRunner.startBundle();
    final Iterable<WindowedValue<InT>> rejectedValues =
        pushbackFnRunner.processElementInReadyWindows(inputElement);
    for (WindowedValue<InT> rejectedValue : rejectedValues) {
      if (rejectedValue.getTimestamp().compareTo(pushbackWatermarkHold) < 0) {
        pushbackWatermarkHold = rejectedValue.getTimestamp();
      }
      pushbackValues.add(rejectedValue);
    }
    pushbackFnRunner.finishBundle();
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    // TODO: currently when we merge main and side inputs, we also use the min for
    // the watermarks coming from either side. This might cause delay/stop in main
    // input watermark as side input might not emit watermarks regularly (e.g. BoundedSource).
    // Need to figure out a way to distinguish these two so the main input watermark can continue.
    this.inputWatermark = watermark;

    if (watermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // this means we will never see any more side input
      emitAllPushbackValues();
      LOG.info("Step {} completed.", stepName);
    }

    final Instant actualInputWatermark = pushbackWatermarkHold.isBefore(inputWatermark)
        ? pushbackWatermarkHold
        : inputWatermark;

    timerInternalsFactory.setInputWatermark(actualInputWatermark);

    pushbackFnRunner.startBundle();
    for (KeyedTimerData<?> keyedTimerData : timerInternalsFactory.removeReadyTimers()) {
      fireTimer(keyedTimerData.getTimerData());
    }
    pushbackFnRunner.finishBundle();

    if (timerInternalsFactory.getOutputWatermark() == null
        || timerInternalsFactory.getOutputWatermark().isBefore(actualInputWatermark)) {
      timerInternalsFactory.setOutputWatermark(actualInputWatermark);
      emitter.emitWatermark(timerInternalsFactory.getOutputWatermark());
    }
  }

  @Override
  public void processSideInput(String id,
                               WindowedValue<? extends Iterable<?>> elements,
                               OpEmitter<OutT> emitter) {
    @SuppressWarnings("unchecked")
    final WindowedValue<Iterable<?>> retypedElements = (WindowedValue<Iterable<?>>) elements;

    final PCollectionView<?> view = idToViewMap.get(id);
    if (view == null) {
      throw new IllegalArgumentException("No mapping of id " + id + " to view.");
    }

    sideInputHandler.addSideInputValue(view, retypedElements);

    final List<WindowedValue<InT>> previousPushbackValues = new ArrayList<>(pushbackValues);
    pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
    pushbackValues.clear();

    previousPushbackValues.forEach(element -> processElement(element, emitter));

    // We may be able to advance the output watermark since we may have played some pushed back
    // events.
    processWatermark(this.inputWatermark, emitter);
  }

  private void fireTimer(TimerInternals.TimerData timer) {
    LOG.debug("Firing timer {}", timer);

    final StateNamespace namespace = timer.getNamespace();
    // NOTE: not sure why this is safe, but DoFnOperator makes this assumption
    final BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    pushbackFnRunner.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
  }

  private void emitAllPushbackValues() {
    if (!pushbackValues.isEmpty()) {
      pushbackFnRunner.startBundle();

      final List<WindowedValue<InT>> previousPushbackValues = new ArrayList<>(pushbackValues);
      pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
      pushbackValues.clear();
      previousPushbackValues.forEach(value -> fnRunner.processElement(value));

      pushbackFnRunner.finishBundle();
    }
  }

  public void close() {
    doFnInvoker.invokeTeardown();
  }

  /**
   * Creates a {@link StepContext} that allows accessing state and timer internals.
   */
  public static StepContext createStepContext(
      StateInternalsFactory stateInternalsFactory,
      TimerInternalsFactory timerInternalsFactory) {
    return new StepContext() {
      @Override
      public StateInternals stateInternals() {
        return stateInternalsFactory.stateInternalsForKey(null);
      }
      @Override
      public TimerInternals timerInternals() {
        return timerInternalsFactory.timerInternalsForKey(null);
      }
    };
  }

  /**
   * Factory class to create an {@link org.apache.beam.runners.core.DoFnRunners.OutputManager}
   * that emits values to the main output only, which is a single
   * {@link org.apache.beam.sdk.values.PCollection}.
   * @param <OutT> type of the output element.
   */
  public static class SingleOutputManagerFactory<OutT> implements OutputManagerFactory<OutT> {
    @Override
    public DoFnRunners.OutputManager create(OpEmitter<OutT> emitter) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tupleTag, WindowedValue<T> windowedValue) {
          // With only one input we know that T is of type OutT.
          @SuppressWarnings("unchecked")
          final WindowedValue<OutT> retypedWindowedValue = (WindowedValue<OutT>) windowedValue;
          emitter.emitElement(retypedWindowedValue);
        }
      };
    }
  }

  /**
   * Factory class to create an {@link org.apache.beam.runners.core.DoFnRunners.OutputManager}
   * that emits values to the main output as well as the side outputs via union type
   * {@link RawUnionValue}.
   */
  public static class MultiOutputManagerFactory implements OutputManagerFactory<RawUnionValue> {
    private final Map<TupleTag<?>, Integer> tagToIdMap;

    public MultiOutputManagerFactory(Map<TupleTag<?>, Integer> tagToIdMap) {
      this.tagToIdMap = tagToIdMap;
    }

    @Override
    public DoFnRunners.OutputManager create(OpEmitter<RawUnionValue> emitter) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tupleTag, WindowedValue<T> windowedValue) {
          final int id = tagToIdMap.get(tupleTag);
          final T rawValue = windowedValue.getValue();
          final RawUnionValue rawUnionValue = new RawUnionValue(id, rawValue);
          emitter.emitElement(windowedValue.withValue(rawUnionValue));
        }
      };
    }
  }
}
