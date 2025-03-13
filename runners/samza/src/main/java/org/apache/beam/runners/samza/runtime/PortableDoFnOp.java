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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.util.DoFnUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samza operator for {@link DoFn}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PortableDoFnOp<InT, FnOutT, OutT> implements Op<InT, OutT, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(PortableDoFnOp.class);

  private final TupleTag<FnOutT> mainOutputTag;
  private final DoFn<InT, FnOutT> doFn;
  private final Coder<?> keyCoder;
  private final Collection<PCollectionView<?>> sideInputs;
  private final List<TupleTag<?>> sideOutputTags;
  private final WindowingStrategy windowingStrategy;
  private final OutputManagerFactory<OutT> outputManagerFactory;
  // NOTE: we use HashMap here to guarantee Serializability
  // Mapping from view id to a view
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
  private final JobInfo jobInfo;
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
  private transient BundleManager<OutT> bundleManager;
  private transient Instant sideInputWatermark;
  private transient List<WindowedValue<InT>> pushbackValues;
  private transient ExecutableStageContext stageContext;
  private transient StageBundleFactory stageBundleFactory;
  private transient boolean bundleDisabled;

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<?, PCollectionView<?>> sideInputMapping;
  private final Map<String, String> stateIdToStoreMapping;

  public PortableDoFnOp(
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
      JobInfo jobInfo,
      Map<String, TupleTag<?>> idToTupleTagMap,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<?, PCollectionView<?>> sideInputMapping,
      Map<String, String> stateIdToStoreMapping) {
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
    this.jobInfo = jobInfo;
    this.idToTupleTagMap = new HashMap<>(idToTupleTagMap);
    this.bundleCheckTimerId = "_samza_bundle_check_" + transformId;
    this.bundleStateId = "_samza_bundle_" + transformId;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
    this.stateIdToStoreMapping = stateIdToStoreMapping;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<OutT> emitter) {
    this.inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.sideInputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.pushbackWatermarkHold = BoundedWindow.TIMESTAMP_MAX_VALUE;

    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final SamzaExecutionContext samzaExecutionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    this.samzaPipelineOptions = samzaExecutionContext.getPipelineOptions();
    this.bundleDisabled = samzaPipelineOptions.getMaxBundleSize() <= 1;

    final String stateId = "pardo-" + transformId;
    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        SamzaStoreStateInternals.createNonKeyedStateInternalsFactory(
            stateId, context.getTaskContext(), samzaPipelineOptions);
    final FutureCollector<OutT> outputFutureCollector = createFutureCollector();

    this.bundleManager =
        new ClassicBundleManager<>(
            createBundleProgressListener(),
            outputFutureCollector,
            samzaPipelineOptions.getMaxBundleSize(),
            samzaPipelineOptions.getMaxBundleTimeMs(),
            timerRegistry,
            bundleCheckTimerId);

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
      final ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
      stageContext = SamzaExecutableStageContextFactory.getInstance().get(jobInfo);
      stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
      this.fnRunner =
          SamzaDoFnRunners.createPortable(
              transformId,
              DoFnUtils.toStepName(executableStage),
              bundleStateId,
              windowedValueCoder,
              executableStage,
              sideInputMapping,
              sideInputHandler,
              nonKeyedStateInternalsFactory,
              timerInternalsFactory,
              samzaPipelineOptions,
              outputManagerFactory.create(emitter, outputFutureCollector),
              stageBundleFactory,
              samzaExecutionContext,
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
              stateId,
              context,
              mainOutputTag,
              sideInputHandler,
              timerInternalsFactory,
              keyCoder,
              outputManagerFactory.create(emitter, outputFutureCollector),
              inputCoder,
              sideOutputTags,
              outputCoders,
              doFnSchemaInformation,
              (Map<String, PCollectionView<?>>) sideInputMapping,
              stateIdToStoreMapping,
              emitter,
              outputFutureCollector);
    }

    this.pushbackFnRunner =
        SimplePushbackSideInputDoFnRunner.create(fnRunner, sideInputs, sideInputHandler);
    this.pushbackValues = new ArrayList<>();

    final Iterator<SamzaDoFnInvokerRegistrar> invokerReg =
        ServiceLoader.load(SamzaDoFnInvokerRegistrar.class).iterator();
    if (!invokerReg.hasNext()) {
      // use the default invoker here
      doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn, samzaPipelineOptions);
    } else {
      doFnInvoker =
          Iterators.getOnlyElement(invokerReg).invokerSetupFor(doFn, samzaPipelineOptions, context);
    }
  }

  FutureCollector<OutT> createFutureCollector() {
    return new FutureCollectorImpl<>();
  }

  private String getTimerStateId(DoFnSignature signature) {
    final StringBuilder builder = new StringBuilder("timer");
    if (signature.usesTimers()) {
      signature.timerDeclarations().keySet().forEach(builder::append);
    }
    return builder.toString();
  }

  @Override
  public void processElement(WindowedValue<InT> inputElement, OpEmitter<OutT> emitter) {
    try {
      bundleManager.tryStartBundle();
      final Iterable<WindowedValue<InT>> rejectedValues =
          pushbackFnRunner.processElementInReadyWindows(inputElement);
      for (WindowedValue<InT> rejectedValue : rejectedValues) {
        if (rejectedValue.getTimestamp().compareTo(pushbackWatermarkHold) < 0) {
          pushbackWatermarkHold = rejectedValue.getTimestamp();
        }
        pushbackValues.add(rejectedValue);
      }

      bundleManager.tryFinishBundle(emitter);
    } catch (Throwable t) {
      LOG.error("Encountered error during process element", t);
      bundleManager.signalFailure(t);
      throw t;
    }
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
    bundleManager.processWatermark(watermark, emitter);
  }

  @Override
  public void processSideInput(
      String id, WindowedValue<? extends Iterable<?>> elements, OpEmitter<OutT> emitter) {
    checkState(
        bundleDisabled, "Side input not supported in bundling mode. Please disable bundling.");
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
    checkState(
        bundleDisabled, "Side input not supported in bundling mode. Please disable bundling.");
    sideInputWatermark = watermark;

    if (sideInputWatermark.isEqual(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // this means we will never see any more side input
      processWatermark(this.inputWatermark, emitter);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter) {
    // this is internal timer in processing time to check whether a bundle should be closed
    if (bundleCheckTimerId.equals(keyedTimerData.getTimerData().getTimerId())) {
      bundleManager.processTimer(keyedTimerData, emitter);
      return;
    }

    pushbackFnRunner.startBundle();
    fireTimer(keyedTimerData);
    pushbackFnRunner.finishBundle();

    this.timerInternalsFactory.removeProcessingTimer((KeyedTimerData) keyedTimerData);
  }

  @Override
  public void close() {
    doFnInvoker.invokeTeardown();
    try (AutoCloseable factory = stageBundleFactory;
        AutoCloseable context = stageContext) {
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

    fnRunner.onTimer(
        timer.getTimerId(),
        timer.getTimerFamilyId(),
        keyedTimerData.getKey(),
        window,
        timer.getTimestamp(),
        timer.getOutputTimestamp(),
        timer.getDomain());
  }

  // todo: should this go through bundle manager to start and finish the bundle?
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

  private BundleManager.BundleProgressListener<OutT> createBundleProgressListener() {
    return new BundleManager.BundleProgressListener<OutT>() {
      @Override
      public void onBundleStarted() {
        pushbackFnRunner.startBundle();
      }

      @Override
      public void onBundleFinished(OpEmitter<OutT> emitter) {
        pushbackFnRunner.finishBundle();
      }

      @Override
      public void onWatermark(Instant watermark, OpEmitter<OutT> emitter) {
        doProcessWatermark(watermark, emitter);
      }
    };
  }
}
