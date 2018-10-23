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
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samza operator for {@link DoFn}. */
public class DoFnOp<InT, FnOutT, OutT> implements Op<InT, OutT, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnOp.class);

  private final TupleTag<FnOutT> mainOutputTag;
  private final DoFn<InT, FnOutT> doFn;
  private final Coder<?> keyCoder;
  private final Collection<PCollectionView<?>> sideInputs;
  private final List<TupleTag<?>> sideOutputTags;
  private final WindowingStrategy windowingStrategy;
  private final OutputManagerFactory<OutT> outputManagerFactory;
  // NOTE: we use HashMap here to guarantee Serializability
  private final HashMap<String, PCollectionView<?>> idToViewMap;
  private final String stepName;

  private transient SamzaTimerInternalsFactory<?> timerInternalsFactory;
  private transient DoFnRunner<InT, FnOutT> fnRunner;
  private transient PushbackSideInputDoFnRunner<InT, FnOutT> pushbackFnRunner;
  private transient SideInputHandler sideInputHandler;
  private transient DoFnInvoker<InT, FnOutT> doFnInvoker;

  // This is derivable from pushbackValues which is persisted to a store.
  // TODO: eagerly initialize the hold in init
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    justification = "No bug",
    value = "SE_TRANSIENT_FIELD_NOT_RESTORED"
  )
  private transient Instant pushbackWatermarkHold;

  // TODO: add this to checkpointable state
  private transient Instant inputWatermark;
  private transient Instant sideInputWatermark;
  private transient List<WindowedValue<InT>> pushbackValues;
  private transient DoFnSignature signature;
  private transient TaskContext context;
  private transient SamzaPipelineOptions pipelineOptions;
  private Coder<InT> inputCoder;
  private Map<TupleTag<?>, Coder<?>> outputCoders;

  public DoFnOp(
      TupleTag<FnOutT> mainOutputTag,
      DoFn<InT, FnOutT> doFn,
      Coder<?> keyCoder,
      Coder<InT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
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
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.windowingStrategy = windowingStrategy;
    this.idToViewMap = new HashMap<>(idToViewMap);
    this.outputManagerFactory = outputManagerFactory;
    this.stepName = stepName;
    this.keyCoder = keyCoder;
  }

  @Override
  public void open(
      Config config,
      TaskContext context,
      TimerRegistry<TimerKey<Void>> timerRegistry,
      OpEmitter<OutT> emitter) {
    this.pipelineOptions =
        Base64Serializer.deserializeUnchecked(
                config.get("beamPipelineOptions"), SerializablePipelineOptions.class)
            .get()
            .as(SamzaPipelineOptions.class);
    this.inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.sideInputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
    this.timerInternalsFactory = createTimerInternalsFactory(keyCoder, timerRegistry);
    this.context = context;
    this.signature = DoFnSignatures.getSignature(doFn.getClass());

    // use non-keyed StateInternals for side input
    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        createStateInternalFactory(null);
    this.sideInputHandler =
        new SideInputHandler(sideInputs, nonKeyedStateInternalsFactory.stateInternalsForKey(null));

    final SamzaExecutionContext executionContext = (SamzaExecutionContext) context.getUserContext();
    final SamzaStoreStateInternals.Factory<?> stateInternalsFactory =
        createStateInternalFactory(keyCoder);

    this.fnRunner =
        DoFnRunnerWithKeyedInternals.of(
            pipelineOptions,
            doFn,
            sideInputHandler,
            outputManagerFactory.create(emitter),
            mainOutputTag,
            sideOutputTags,
            inputCoder,
            outputCoders,
            stateInternalsFactory,
            timerInternalsFactory,
            windowingStrategy,
            executionContext.getMetricsContainer(),
            stepName);

    this.pushbackFnRunner =
        SimplePushbackSideInputDoFnRunner.create(fnRunner, sideInputs, sideInputHandler);

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
    this.inputWatermark = watermark;

    if (sideInputWatermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // this means we will never see any more side input
      emitAllPushbackValues();
    }

    final Instant actualInputWatermark =
        pushbackWatermarkHold.isBefore(inputWatermark) ? pushbackWatermarkHold : inputWatermark;

    timerInternalsFactory.setInputWatermark(actualInputWatermark);

    pushbackFnRunner.startBundle();
    for (KeyedTimerData<?> keyedTimerData : timerInternalsFactory.removeReadyTimers()) {
      fireTimer(keyedTimerData);
    }
    pushbackFnRunner.finishBundle();

    if (timerInternalsFactory.getOutputWatermark() == null
        || timerInternalsFactory.getOutputWatermark().isBefore(actualInputWatermark)) {
      timerInternalsFactory.setOutputWatermark(actualInputWatermark);
      emitter.emitWatermark(timerInternalsFactory.getOutputWatermark());
    }
  }

  @Override
  public void processSideInput(
      String id, WindowedValue<? extends Iterable<?>> elements, OpEmitter<OutT> emitter) {
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

    for (final WindowedValue<InT> value : previousPushbackValues) {
      processElement(value, emitter);
    }

    // We may be able to advance the output watermark since we may have played some pushed back
    // events.
    processWatermark(this.inputWatermark, emitter);
  }

  @Override
  public void processSideInputWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    sideInputWatermark = watermark;

    if (sideInputWatermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // this means we will never see any more side input
      processWatermark(this.inputWatermark, emitter);
    }
  }

  @Override
  public void processTimer(KeyedTimerData<Void> keyedTimerData) {
    pushbackFnRunner.startBundle();
    fireTimer(keyedTimerData);
    pushbackFnRunner.finishBundle();
  }

  @Override
  public void close() {
    doFnInvoker.invokeTeardown();
  }

  @SuppressWarnings("unchecked")
  private SamzaStoreStateInternals.Factory createStateInternalFactory(Coder<?> keyCoder) {
    final int batchGetSize = pipelineOptions.getStoreBatchGetSize();
    final Map<String, KeyValueStore<byte[], byte[]>> stores =
        new HashMap<>(SamzaStoreStateInternals.getBeamStore(context));

    final Coder stateKeyCoder;
    if (keyCoder != null) {
      signature
          .stateDeclarations()
          .keySet()
          .forEach(
              stateId ->
                  stores.put(stateId, (KeyValueStore<byte[], byte[]>) context.getStore(stateId)));
      stateKeyCoder = keyCoder;
    } else {
      stateKeyCoder = VoidCoder.of();
    }
    return new SamzaStoreStateInternals.Factory<>(
        mainOutputTag.getId(), stores, stateKeyCoder, batchGetSize);
  }

  @SuppressWarnings("unchecked")
  private SamzaTimerInternalsFactory createTimerInternalsFactory(
      Coder<?> keyCoder, TimerRegistry timerRegistry) {
    return new SamzaTimerInternalsFactory<>(keyCoder, timerRegistry);
  }

  private void fireTimer(KeyedTimerData<?> keyedTimerData) {
    final TimerInternals.TimerData timer = keyedTimerData.getTimerData();
    LOG.debug("Firing timer {}", timer);

    final StateNamespace namespace = timer.getNamespace();
    // NOTE: not sure why this is safe, but DoFnOperator makes this assumption
    final BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();

    if (fnRunner instanceof DoFnRunnerWithKeyedInternals) {
      // Need to pass in the keyed TimerData here
      ((DoFnRunnerWithKeyedInternals) fnRunner).onTimer(keyedTimerData, window);
    } else {
      pushbackFnRunner.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
    }
  }

  private void emitAllPushbackValues() {
    if (!pushbackValues.isEmpty()) {
      pushbackFnRunner.startBundle();

      final List<WindowedValue<InT>> previousPushbackValues = new ArrayList<>(pushbackValues);
      pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
      pushbackValues.clear();

      for (final WindowedValue<InT> value : previousPushbackValues) {
        fnRunner.processElement(value);
      }

      pushbackFnRunner.finishBundle();
    }
  }

  /**
   * Factory class to create an {@link org.apache.beam.runners.core.DoFnRunners.OutputManager} that
   * emits values to the main output only, which is a single {@link
   * org.apache.beam.sdk.values.PCollection}.
   *
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
   * Factory class to create an {@link org.apache.beam.runners.core.DoFnRunners.OutputManager} that
   * emits values to the main output as well as the side outputs via union type {@link
   * RawUnionValue}.
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
