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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samza operator for {@link DoFn}. */
public class DoFnOp<InT, FnOutT, OutT> implements Op<InT, OutT, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnOp.class);
  private static final long MIN_BUNDLE_CHECK_TIME_MS = 10L;

  private final TupleTag<FnOutT> mainOutputTag;
  private final DoFn<InT, FnOutT> doFn;
  private final Coder<?> keyCoder;
  private final Collection<PCollectionView<?>> sideInputs;
  private final List<TupleTag<?>> sideOutputTags;
  private final WindowingStrategy windowingStrategy;
  private final OutputManagerFactory<OutT> outputManagerFactory;
  // NOTE: we use HashMap here to guarantee Serializability
  private final HashMap<String, PCollectionView<?>> idToViewMap;
  private final String transformFullName;
  private final String transformId;
  private final Coder<InT> inputCoder;
  private final Coder<WindowedValue<InT>> windowedValueCoder;
  private final HashMap<TupleTag<?>, Coder<?>> outputCoders;
  private final PCollection.IsBounded isBounded;
  private final String bundleCheckTimerId;
  private final String bundleStateId;

  // portable api related
  private final boolean isPortable;
  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final HashMap<String, TupleTag<?>> idToTupleTagMap;

  private transient SamzaTimerInternalsFactory<?> timerInternalsFactory;
  private transient DoFnRunner<InT, FnOutT> fnRunner;
  private transient PushbackSideInputDoFnRunner<InT, FnOutT> pushbackFnRunner;
  private transient SideInputHandler sideInputHandler;
  private transient DoFnInvoker<InT, FnOutT> doFnInvoker;
  private transient SamzaPipelineOptions samzaPipelineOptions;

  // This is derivable from pushbackValues which is persisted to a store.
  // TODO: eagerly initialize the hold in init
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      justification = "No bug",
      value = "SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient Instant pushbackWatermarkHold;

  // TODO: add this to checkpointable state
  private transient Instant inputWatermark;
  private transient Instant bundleWatermarkHold;
  private transient Instant sideInputWatermark;
  private transient List<WindowedValue<InT>> pushbackValues;
  private transient StageBundleFactory stageBundleFactory;
  private transient long maxBundleSize;
  private transient long maxBundleTimeMs;
  private transient AtomicLong currentBundleElementCount;
  private transient AtomicLong bundleStartTime;
  private transient AtomicBoolean isBundleStarted;
  private transient Scheduler<KeyedTimerData<Void>> bundleTimerScheduler;
  private DoFnSchemaInformation doFnSchemaInformation;
  private Map<String, PCollectionView<?>> sideInputMapping;

  public DoFnOp(
      TupleTag<FnOutT> mainOutputTag,
      DoFn<InT, FnOutT> doFn,
      Coder<?> keyCoder,
      Coder<InT> inputCoder,
      Coder<WindowedValue<InT>> windowedValueCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      Collection<PCollectionView<?>> sideInputs,
      List<TupleTag<?>> sideOutputTags,
      WindowingStrategy windowingStrategy,
      Map<String, PCollectionView<?>> idToViewMap,
      OutputManagerFactory<OutT> outputManagerFactory,
      String transformFullName,
      String transformId,
      PCollection.IsBounded isBounded,
      boolean isPortable,
      RunnerApi.ExecutableStagePayload stagePayload,
      Map<String, TupleTag<?>> idToTupleTagMap,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.mainOutputTag = mainOutputTag;
    this.doFn = doFn;
    this.sideInputs = sideInputs;
    this.sideOutputTags = sideOutputTags;
    this.inputCoder = inputCoder;
    this.windowedValueCoder = windowedValueCoder;
    this.outputCoders = new HashMap<>(outputCoders);
    this.windowingStrategy = windowingStrategy;
    this.idToViewMap = new HashMap<>(idToViewMap);
    this.outputManagerFactory = outputManagerFactory;
    this.transformFullName = transformFullName;
    this.transformId = transformId;
    this.keyCoder = keyCoder;
    this.isBounded = isBounded;
    this.isPortable = isPortable;
    this.stagePayload = stagePayload;
    this.idToTupleTagMap = new HashMap<>(idToTupleTagMap);
    this.bundleCheckTimerId = "_samza_bundle_check_" + transformId;
    this.bundleStateId = "_samza_bundle_" + transformId;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<OutT> emitter) {
    this.inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.sideInputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
    this.currentBundleElementCount = new AtomicLong(0L);
    this.bundleStartTime = new AtomicLong(Long.MAX_VALUE);
    this.isBundleStarted = new AtomicBoolean(false);
    this.bundleWatermarkHold = null;

    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final SamzaExecutionContext samzaExecutionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    this.samzaPipelineOptions = samzaExecutionContext.getPipelineOptions();
    this.maxBundleSize = samzaPipelineOptions.getMaxBundleSize();
    this.maxBundleTimeMs = samzaPipelineOptions.getMaxBundleTimeMs();
    this.bundleTimerScheduler = timerRegistry;

    if (this.maxBundleSize > 1) {
      scheduleNextBundleCheck();
    }

    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        SamzaStoreStateInternals.createStateInternalFactory(
            transformId, null, context.getTaskContext(), samzaPipelineOptions, signature);

    this.timerInternalsFactory =
        SamzaTimerInternalsFactory.createTimerInternalFactory(
            keyCoder,
            (Scheduler) timerRegistry,
            getTimerStateId(signature),
            nonKeyedStateInternalsFactory,
            windowingStrategy,
            isBounded,
            samzaPipelineOptions);

    this.sideInputHandler =
        new SideInputHandler(sideInputs, nonKeyedStateInternalsFactory.stateInternalsForKey(null));

    if (isPortable) {
      // storing events within a bundle in states
      final BagState<WindowedValue<InT>> bundledEventsBagState =
          nonKeyedStateInternalsFactory
              .stateInternalsForKey(null)
              .state(StateNamespaces.global(), StateTags.bag(bundleStateId, windowedValueCoder));
      final ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
      stageBundleFactory = samzaExecutionContext.getJobBundleFactory().forStage(executableStage);
      this.fnRunner =
          SamzaDoFnRunners.createPortable(
              samzaPipelineOptions,
              bundledEventsBagState,
              outputManagerFactory.create(emitter),
              stageBundleFactory,
              mainOutputTag,
              idToTupleTagMap,
              context,
              transformFullName);
    } else {
      this.fnRunner =
          SamzaDoFnRunners.create(
              samzaPipelineOptions,
              doFn,
              windowingStrategy,
              transformFullName,
              transformId,
              context,
              mainOutputTag,
              sideInputHandler,
              timerInternalsFactory,
              keyCoder,
              outputManagerFactory.create(emitter),
              inputCoder,
              sideOutputTags,
              outputCoders,
              doFnSchemaInformation,
              sideInputMapping);
    }

    this.pushbackFnRunner =
        SimplePushbackSideInputDoFnRunner.create(fnRunner, sideInputs, sideInputHandler);
    this.pushbackValues = new ArrayList<>();

    final Iterator<SamzaDoFnInvokerRegistrar> invokerReg =
        ServiceLoader.load(SamzaDoFnInvokerRegistrar.class).iterator();
    if (!invokerReg.hasNext()) {
      // use the default invoker here
      doFnInvoker = DoFnInvokers.invokerFor(doFn);
    } else {
      doFnInvoker = Iterators.getOnlyElement(invokerReg).invokerFor(doFn, context);
    }

    doFnInvoker.invokeSetup();
  }

  /*
   * Schedule in processing time to check whether the current bundle should be closed. Note that
   * we only approximately achieve max bundle time by checking as frequent as half of the max bundle
   * time set by users. This would violate the max bundle time by up to half of it but should
   * acceptable in most cases (and cheaper than scheduling a timer at the beginning of every bundle).
   */
  private void scheduleNextBundleCheck() {
    final Instant nextBundleCheckTime =
        Instant.now().plus(Duration.millis(maxBundleTimeMs / 2 + MIN_BUNDLE_CHECK_TIME_MS));
    final TimerInternals.TimerData timerData =
        TimerInternals.TimerData.of(
            bundleCheckTimerId,
            StateNamespaces.global(),
            nextBundleCheckTime,
            nextBundleCheckTime,
            TimeDomain.PROCESSING_TIME);
    bundleTimerScheduler.schedule(
        new KeyedTimerData<>(new byte[0], null, timerData), nextBundleCheckTime.getMillis());
  }

  private String getTimerStateId(DoFnSignature signature) {
    final StringBuilder builder = new StringBuilder("timer");
    if (signature.usesTimers()) {
      signature.timerDeclarations().keySet().forEach(key -> builder.append(key));
    }
    return builder.toString();
  }

  private void attemptStartBundle() {
    if (isBundleStarted.compareAndSet(false, true)) {
      currentBundleElementCount.set(0L);
      bundleStartTime.set(System.currentTimeMillis());
      pushbackFnRunner.startBundle();
    }
  }

  private void finishBundle(OpEmitter<OutT> emitter) {
    if (isBundleStarted.compareAndSet(true, false)) {
      currentBundleElementCount.set(0L);
      bundleStartTime.set(Long.MAX_VALUE);
      pushbackFnRunner.finishBundle();
      if (bundleWatermarkHold != null) {
        doProcessWatermark(bundleWatermarkHold, emitter);
      }
      bundleWatermarkHold = null;
    }
  }

  private void attemptFinishBundle(OpEmitter<OutT> emitter) {
    if (!isBundleStarted.get()) {
      return;
    }
    if (currentBundleElementCount.get() >= maxBundleSize
        || System.currentTimeMillis() - bundleStartTime.get() > maxBundleTimeMs) {
      finishBundle(emitter);
    }
  }

  @Override
  public void processElement(WindowedValue<InT> inputElement, OpEmitter<OutT> emitter) {
    attemptStartBundle();

    final Iterable<WindowedValue<InT>> rejectedValues =
        pushbackFnRunner.processElementInReadyWindows(inputElement);
    for (WindowedValue<InT> rejectedValue : rejectedValues) {
      if (rejectedValue.getTimestamp().compareTo(pushbackWatermarkHold) < 0) {
        pushbackWatermarkHold = rejectedValue.getTimestamp();
      }
      pushbackValues.add(rejectedValue);
    }

    currentBundleElementCount.incrementAndGet();
    attemptFinishBundle(emitter);
  }

  private void doProcessWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    this.inputWatermark = watermark;

    if (sideInputWatermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // this means we will never see any more side input
      emitAllPushbackValues();
    }

    final Instant actualInputWatermark =
        pushbackWatermarkHold.isBefore(inputWatermark) ? pushbackWatermarkHold : inputWatermark;

    timerInternalsFactory.setInputWatermark(actualInputWatermark);

    Collection<? extends KeyedTimerData<?>> readyTimers = timerInternalsFactory.removeReadyTimers();
    if (!readyTimers.isEmpty()) {
      pushbackFnRunner.startBundle();
      for (KeyedTimerData<?> keyedTimerData : readyTimers) {
        fireTimer(keyedTimerData);
      }
      pushbackFnRunner.finishBundle();
    }

    if (timerInternalsFactory.getOutputWatermark() == null
        || timerInternalsFactory.getOutputWatermark().isBefore(actualInputWatermark)) {
      timerInternalsFactory.setOutputWatermark(actualInputWatermark);
      emitter.emitWatermark(timerInternalsFactory.getOutputWatermark());
    }
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    if (!isBundleStarted.get()) {
      doProcessWatermark(watermark, emitter);
    } else {
      // if there is a bundle in progress, hold back the watermark until end of the bundle
      this.bundleWatermarkHold = watermark;
      if (watermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        // for batch mode, the max watermark should force the bundle to close
        finishBundle(emitter);
      }
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
  public void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter) {
    // this is internal timer in processing time to check whether a bundle should be closed
    if (bundleCheckTimerId.equals(keyedTimerData.getTimerData().getTimerId())) {
      attemptFinishBundle(emitter);
      scheduleNextBundleCheck();
      return;
    }

    pushbackFnRunner.startBundle();
    fireTimer(keyedTimerData);
    pushbackFnRunner.finishBundle();

    this.timerInternalsFactory.removeProcessingTimer((KeyedTimerData) keyedTimerData);
  }

  @Override
  public void close() {
    bundleWatermarkHold = null;
    doFnInvoker.invokeTeardown();
    try (AutoCloseable closer = stageBundleFactory) {
      // do nothing
    } catch (Exception e) {
      LOG.error("Failed to close stage bundle factory", e);
    }
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
      pushbackFnRunner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          null,
          window,
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain());
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
    private final Map<TupleTag<?>, Integer> tagToIndexMap;

    public MultiOutputManagerFactory(Map<TupleTag<?>, Integer> tagToIndexMap) {
      this.tagToIndexMap = tagToIndexMap;
    }

    @Override
    public DoFnRunners.OutputManager create(OpEmitter<RawUnionValue> emitter) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tupleTag, WindowedValue<T> windowedValue) {
          final int index = tagToIndexMap.get(tupleTag);
          final T rawValue = windowedValue.getValue();
          final RawUnionValue rawUnionValue = new RawUnionValue(index, rawValue);
          emitter.emitElement(windowedValue.withValue(rawUnionValue));
        }
      };
    }
  }
}
