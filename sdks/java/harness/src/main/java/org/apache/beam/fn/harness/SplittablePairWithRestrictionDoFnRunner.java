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
package org.apache.beam.fn.harness;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.fn.harness.state.SideInputSpec;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PCollectionViewTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A runner for the PTransform step that invokes GetInitialRestriction and pairs it with the
 * incoming element.
 *
 * <p>The DoFn will have type DoFn<InputT, OutputT> but this transform's Fn API type is from {@code
 * WindowedValue<InputT>} to {@code WindowedValue<KV<InputT, KV<RestrictionT,
 * WatermarkEstimatorStateT>>>}
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SplittablePairWithRestrictionDoFnRunner<
    InputT, WindowT extends BoundedWindow, RestrictionT, WatermarkEstimatorStateT, OutputT> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      Factory factory = new Factory();
      return ImmutableMap.<String, PTransformRunnerFactory>builder()
          .put(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN, factory)
          .build();
    }
  }

  static class Factory implements PTransformRunnerFactory {

    @Override
    public final void addRunnerForPTransform(Context context) throws IOException {
      // The constructor itself registers consumption
      new SplittablePairWithRestrictionDoFnRunner<>(
          context.getPipelineOptions(),
          context.getRunnerCapabilities(),
          context.getBeamFnStateClient(),
          context.getPTransformId(),
          context.getPTransform(),
          context.getProcessBundleInstructionIdSupplier(),
          context.getCacheTokensSupplier(),
          context.getBundleCacheSupplier(),
          context.getProcessWideCache(),
          context.getComponents(),
          context::addTearDownFunction,
          context::getPCollectionConsumer,
          context::addPCollectionConsumer);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final PipelineOptions pipelineOptions;

  private final FnDataReceiver<
          WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>>
      mainOutputConsumer;

  private final FnApiStateAccessor<?> stateAccessor;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final PairWithRestrictionArgumentProvider mutableArgumentProvider;

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  SplittablePairWithRestrictionDoFnRunner(
      PipelineOptions pipelineOptions,
      Set<String> runnerCapabilities,
      BeamFnStateClient beamFnStateClient,
      String pTransformId,
      PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      Supplier<List<BeamFnApi.ProcessBundleRequest.CacheToken>> cacheTokens,
      Supplier<Cache<?, ?>> bundleCache,
      Cache<?, ?> processWideCache,
      RunnerApi.Components components,
      Consumer<ThrowingRunnable> addTearDownFunction,
      Function<String, FnDataReceiver<WindowedValue<?>>> getPCollectionConsumer,
      BiConsumer<String, FnDataReceiver<WindowedValue<InputT>>> addPCollectionConsumer)
      throws IOException {
    this.pipelineOptions = pipelineOptions;

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components).withPipeline(Pipeline.create());
    ParDoPayload parDoPayload = ParDoPayload.parseFrom(pTransform.getSpec().getPayload());

    // DoFn and metadata
    DoFn<InputT, OutputT> doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(parDoPayload);
    DoFnSignature doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
    this.doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn, pipelineOptions);
    this.doFnSchemaInformation = ParDoTranslation.getSchemaInformation(parDoPayload);

    // Main output
    checkArgument(
        pTransform.getOutputsMap().size() == 1,
        "PairWithRestriction expects exact one output, but got: ",
        pTransform.getOutputsMap().size());
    TupleTag<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>> mainOutputTag =
        new TupleTag<>(Iterables.getOnlyElement(pTransform.getOutputsMap().keySet()));
    @SuppressWarnings("rawtypes") // cannot do this multi-level cast without rawtypes
    FnDataReceiver<WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>>
        mainOutputConsumer =
            (FnDataReceiver)
                getPCollectionConsumer.apply(pTransform.getOutputsMap().get(mainOutputTag.getId()));
    this.mainOutputConsumer = mainOutputConsumer;

    // Main input
    String mainInputTag =
        Iterables.getOnlyElement(
            Sets.difference(
                pTransform.getInputsMap().keySet(), parDoPayload.getSideInputsMap().keySet()));
    String mainInputName = pTransform.getInputsOrThrow(mainInputTag);
    PCollection mainInput =
        components.getPcollectionsMap().get(pTransform.getInputsOrThrow(mainInputTag));

    // Side inputs
    this.sideInputMapping = ParDoTranslation.getSideInputMapping(parDoPayload);
    @SuppressWarnings("rawtypes") // passed to FnApiStateAccessor which uses rawtypes
    ImmutableMap.Builder<TupleTag<?>, SideInputSpec> tagToSideInputSpecMapBuilder =
        ImmutableMap.builder();
    for (Map.Entry<String, RunnerApi.SideInput> entry :
        parDoPayload.getSideInputsMap().entrySet()) {
      String sideInputTag = entry.getKey();
      RunnerApi.SideInput sideInput = entry.getValue();
      PCollection sideInputPCollection =
          components.getPcollectionsMap().get(pTransform.getInputsOrThrow(sideInputTag));
      WindowingStrategy<?, ?> sideInputWindowingStrategy =
          rehydratedComponents.getWindowingStrategy(sideInputPCollection.getWindowingStrategyId());
      tagToSideInputSpecMapBuilder.put(
          new TupleTag<>(entry.getKey()),
          SideInputSpec.create(
              sideInput.getAccessPattern().getUrn(),
              rehydratedComponents.getCoder(sideInputPCollection.getCoderId()),
              (Coder<WindowT>) sideInputWindowingStrategy.getWindowFn().windowCoder(),
              PCollectionViewTranslation.viewFnFromProto(entry.getValue().getViewFn()),
              (WindowMappingFn<WindowT>)
                  PCollectionViewTranslation.windowMappingFnFromProto(
                      entry.getValue().getWindowMappingFn())));
    }
    @SuppressWarnings("rawtypes") // passed to FnApiStateAccessor which uses rawtypes
    Map<TupleTag<?>, SideInputSpec> tagToSideInputSpecMap = tagToSideInputSpecMapBuilder.build();

    // Register processing methods
    if (doFnSignature.getInitialRestriction().observesWindow() || !sideInputMapping.isEmpty()) {
      addPCollectionConsumer.accept(
          mainInputName, this::processElementForWindowObservingPairWithRestriction);
      this.mutableArgumentProvider = new WindowObservingProcessBundleContext();
    } else {
      addPCollectionConsumer.accept(mainInputName, this::processElementForPairWithRestriction);
      this.mutableArgumentProvider = new NonWindowObservingProcessBundleContext();
    }
    addTearDownFunction.accept(this::tearDown);

    // State accessor for supporting side inputs, requires window coder and lots of metadata
    WindowingStrategy<?, ?> windowingStrategy =
        rehydratedComponents.getWindowingStrategy(mainInput.getWindowingStrategyId());
    Coder<WindowT> windowCoder = (Coder<WindowT>) windowingStrategy.getWindowFn().windowCoder();
    this.stateAccessor =
        new FnApiStateAccessor<>(
            pipelineOptions,
            runnerCapabilities,
            pTransformId,
            processBundleInstructionId,
            cacheTokens,
            bundleCache,
            processWideCache,
            tagToSideInputSpecMap,
            beamFnStateClient,
            null,
            (Coder<BoundedWindow>) windowCoder,
            null,
            mutableArgumentProvider::getCurrentWindow);
  }

  private void processElementForPairWithRestriction(WindowedValue<InputT> elem) {
    mutableArgumentProvider.currentElement = elem;
    try {
      RestrictionT currentRestriction =
          doFnInvoker.invokeGetInitialRestriction(mutableArgumentProvider);
      outputTo(
          mainOutputConsumer,
          elem.withValue(
              KV.of(
                  elem.getValue(),
                  KV.of(
                      currentRestriction,
                      doFnInvoker.invokeGetInitialWatermarkEstimatorState(
                          mutableArgumentProvider)))));
    } finally {
      mutableArgumentProvider.currentElement = null;
    }

    this.stateAccessor.finalizeState();
  }

  private void processElementForWindowObservingPairWithRestriction(WindowedValue<InputT> elem) {
    mutableArgumentProvider.currentElement = elem;
    try {
      for (BoundedWindow boundedWindow : elem.getWindows()) {
        mutableArgumentProvider.currentWindow = boundedWindow;
        RestrictionT currentRestriction =
            doFnInvoker.invokeGetInitialRestriction(mutableArgumentProvider);
        outputTo(
            mainOutputConsumer,
            WindowedValue.of(
                KV.of(
                    elem.getValue(),
                    KV.of(
                        currentRestriction,
                        doFnInvoker.invokeGetInitialWatermarkEstimatorState(
                            mutableArgumentProvider))),
                elem.getTimestamp(),
                boundedWindow,
                elem.getPane()));
      }
    } finally {
      mutableArgumentProvider.currentElement = null;
      mutableArgumentProvider.currentWindow = null;
    }

    this.stateAccessor.finalizeState();
  }

  private void tearDown() {
    doFnInvoker.invokeTeardown();
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(FnDataReceiver<WindowedValue<T>> consumer, WindowedValue<T> output) {
    try {
      consumer.accept(output);
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  /** Base implementation that does not override methods which need to be window aware. */
  private abstract class PairWithRestrictionArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {

    // The member variables below are only valid for the lifetime of certain methods.
    /** Only valid during {@code processElement...} methods, null otherwise. */
    private @Nullable WindowedValue<InputT> currentElement;

    private @Nullable BoundedWindow currentWindow;

    protected WindowedValue<InputT> getCurrentElement() {
      return checkStateNotNull(
          this.currentElement, "Attempt to access element outside element processing context.");
    }

    protected BoundedWindow getCurrentWindow() {
      return checkStateNotNull(
          this.currentWindow, "Attempt to access window outside element processing context.");
    }

    @Override
    public String getErrorContext() {
      return "GetInitialRestriction";
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getValue();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getPane();
    }

    @Override
    public Object schemaElement(int index) {
      SerializableFunction<InputT, Object> converter =
          (SerializableFunction<InputT, Object>)
              doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(getCurrentElement().getValue());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getTimestamp();
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }
  }

  private class WindowObservingProcessBundleContext extends PairWithRestrictionArgumentProvider {
    @Override
    public BoundedWindow window() {
      return getCurrentWindow();
    }

    @Override
    public Object sideInput(String tagId) {
      return stateAccessor.get(sideInputMapping.get(tagId), getCurrentWindow());
    }
  }

  /** Provides arguments for a {@link DoFnInvoker} for a non-window observing method. */
  private class NonWindowObservingProcessBundleContext extends PairWithRestrictionArgumentProvider {
    @Override
    public BoundedWindow window() {
      throw new IllegalStateException(
          "Attempt to access window in non-window-observing context. This is an internal error.");
    }

    @Override
    public Object sideInput(String tagId) {
      throw new IllegalStateException(
          "Attempt to access side input in non-window-observing context. This is an internal error.");
    }
  }
}
