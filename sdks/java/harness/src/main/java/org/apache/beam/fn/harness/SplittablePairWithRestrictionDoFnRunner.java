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
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
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
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
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
public class SplittablePairWithRestrictionDoFnRunner<
        InputT, RestrictionT, WatermarkEstimatorStateT, OutputT>
    implements FnApiStateAccessor.MutatingStateContext<Void, BoundedWindow> {
  private final boolean observesWindow;

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
      addRunnerForPairWithRestriction(context);
    }

    private void addRunnerForPairWithRestriction(Context context) throws IOException {
      FnApiStateAccessor<Void> stateAccessor =
          FnApiStateAccessor.Factory.<Void>factoryForPTransformContext(context).create();

      SplittablePairWithRestrictionDoFnRunner<?, ?, ?, ?> runner =
          new SplittablePairWithRestrictionDoFnRunner<>(
              context.getPipelineOptions(),
              context.getPTransform(),
              context::getPCollectionConsumer,
              stateAccessor);

      stateAccessor.setKeyAndWindowContext(runner);

      // Register processing methods
      context.addPCollectionConsumer(
          context
              .getPTransform()
              .getInputsOrThrow(ParDoTranslation.getMainInputName(context.getPTransform())),
          runner::processElement);
      context.addTearDownFunction(runner::tearDown);
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
      PTransform pTransform,
      Function<String, FnDataReceiver<WindowedValue<?>>> getPCollectionConsumer,
      FnApiStateAccessor<Void> stateAccessor)
      throws IOException {
    this.pipelineOptions = pipelineOptions;

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
                getPCollectionConsumer.apply(pTransform.getOutputsOrThrow(mainOutputTag.getId()));
    this.mainOutputConsumer = mainOutputConsumer;

    // Side inputs
    this.sideInputMapping = ParDoTranslation.getSideInputMapping(parDoPayload);

    // Register processing methods
    this.observesWindow =
        (doFnSignature.getInitialRestriction() != null
                && doFnSignature.getInitialRestriction().observesWindow())
            || !sideInputMapping.isEmpty();

    if (observesWindow) {
      this.mutableArgumentProvider = new WindowObservingProcessBundleContext();
    } else {
      this.mutableArgumentProvider = new NonWindowObservingProcessBundleContext();
    }
    this.stateAccessor = stateAccessor;
  }

  private void processElement(WindowedValue<InputT> elem) {
    if (observesWindow) {
      processElementForWindowObservingPairWithRestriction(elem);
    } else {
      processElementForPairWithRestriction(elem);
    }
  }

  private void processElementForPairWithRestriction(WindowedValue<InputT> elem) {
    mutableArgumentProvider.currentElement = elem;
    try {
      RestrictionT currentRestriction =
          doFnInvoker.invokeGetInitialRestriction(mutableArgumentProvider);
      WatermarkEstimatorStateT watermarkEstimatorState =
          doFnInvoker.invokeGetInitialWatermarkEstimatorState(mutableArgumentProvider);
      outputTo(
          mainOutputConsumer,
          elem.withValue(
              KV.of(elem.getValue(), KV.of(currentRestriction, watermarkEstimatorState))));
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
        WatermarkEstimatorStateT watermarkEstimatorState =
            doFnInvoker.invokeGetInitialWatermarkEstimatorState(mutableArgumentProvider);
        outputTo(
            mainOutputConsumer,
            WindowedValues.of(
                KV.of(elem.getValue(), KV.of(currentRestriction, watermarkEstimatorState)),
                elem.getTimestamp(),
                boundedWindow,
                elem.getPaneInfo()));
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

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public BoundedWindow getCurrentWindow() {
    return mutableArgumentProvider.getCurrentWindowOrFail();
  }

  /** Base implementation that does not override methods which need to be window aware. */
  private abstract class PairWithRestrictionArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {

    // The member variables below are only valid for the lifetime of certain methods.
    /** Only valid during {@code processElement...} methods, null otherwise. */
    private @Nullable WindowedValue<InputT> currentElement;

    private @Nullable BoundedWindow currentWindow;

    protected WindowedValue<InputT> getCurrentElementOrFail() {
      return checkStateNotNull(
          this.currentElement, "Attempt to access element outside element processing context.");
    }

    protected BoundedWindow getCurrentWindowOrFail() {
      return checkStateNotNull(
          this.currentWindow, "Attempt to access window outside element processing context.");
    }

    @Override
    public String getErrorContext() {
      return "GetInitialRestriction";
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return getCurrentElementOrFail().getValue();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return getCurrentElementOrFail().getPaneInfo();
    }

    @Override
    public Object schemaElement(int index) {
      SerializableFunction<InputT, Object> converter =
          (SerializableFunction<InputT, Object>)
              doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(getCurrentElementOrFail().getValue());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return getCurrentElementOrFail().getTimestamp();
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }
  }

  private class WindowObservingProcessBundleContext extends PairWithRestrictionArgumentProvider {
    @Override
    public BoundedWindow window() {
      return getCurrentWindowOrFail();
    }

    @Override
    public Object sideInput(String tagId) {
      PCollectionView<Object> pCollectionView =
          (PCollectionView<Object>)
              checkStateNotNull(sideInputMapping.get(tagId), "Side input tag not found: %s", tagId);

      return stateAccessor.get(pCollectionView, getCurrentWindow());
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
