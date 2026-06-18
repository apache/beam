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

import static org.apache.beam.sdk.values.WindowedValues.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittableTruncateSizedRestrictionsDoFnRunner}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class SplittableTruncateSizedRestrictionsDoFnRunnerTest implements Serializable {

  @RunWith(JUnit4.class)
  public static class ExecutionTest implements Serializable {
    @Rule public transient ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

    public static final String TEST_TRANSFORM_ID = "pTransformId";

    /** @return a test MetricUpdate for expected metrics to compare against */
    public MetricUpdate create(String stepName, MetricName name, long value) {
      return MetricUpdate.create(MetricKey.create(stepName, name), value);
    }

    private <T> WindowedValue<T> valueInWindows(
        T value, BoundedWindow window, BoundedWindow... windows) {
      return WindowedValues.of(
          value,
          window.maxTimestamp(),
          ImmutableList.<BoundedWindow>builder().add(window).add(windows).build(),
          PaneInfo.NO_FIRING);
    }

    @Test
    public void testRegistration() {
      for (PTransformRunnerFactory.Registrar registrar :
          ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
        if (registrar instanceof SplittableTruncateSizedRestrictionsDoFnRunner.Registrar) {
          assertThat(
              registrar.getPTransformRunnerFactories(),
              IsMapContaining.hasKey(
                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN));
          return;
        }
      }
      fail("Expected registrar not found.");
    }

    private static SplitResult createSplitResult(double fractionOfRemainder) {
      ByteStringOutputStream primaryBytes = new ByteStringOutputStream();
      ByteStringOutputStream residualBytes = new ByteStringOutputStream();
      try {
        DoubleCoder.of().encode(fractionOfRemainder, primaryBytes);
        DoubleCoder.of().encode(1 - fractionOfRemainder, residualBytes);
      } catch (Exception e) {
        // No-op.
      }
      return SplitResult.of(
          ImmutableList.of(
              BundleApplication.newBuilder()
                  .setElement(primaryBytes.toByteString())
                  .setInputId("mainInputId-process")
                  .setTransformId("processPTransfromId")
                  .build()),
          ImmutableList.of(
              DelayedBundleApplication.newBuilder()
                  .setApplication(
                      BundleApplication.newBuilder()
                          .setElement(residualBytes.toByteString())
                          .setInputId("mainInputId-process")
                          .setTransformId("processPTransfromId")
                          .build())
                  .build()));
    }

    private static class SplittableFnDataReceiver
        implements HandlesSplits, FnDataReceiver<WindowedValue> {
      SplittableFnDataReceiver(
          List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues) {
        this.mainOutputValues = mainOutputValues;
      }

      private final List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues;

      @Override
      public SplitResult trySplit(double fractionOfRemainder) {
        return createSplitResult(fractionOfRemainder);
      }

      @Override
      public double getProgress() {
        return 0.7;
      }

      @Override
      public void accept(WindowedValue input) throws Exception {
        mainOutputValues.add(input);
      }
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWhenObservingWindows()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          WindowObservingTestSplittableDoFn.forSplitAtTruncate(singletonSideInputView);
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValues.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));

      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              // .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .components(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .putAllEnvironments(Collections.emptyMap())
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .build())
              // .coders(pProto.getComponents().getCodersMap())
              // .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new SplittableTruncateSizedRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      mainOutputValues.clear();
      BoundedWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      BoundedWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      BoundedWindow window3 = new IntervalWindow(new Instant(7), new Instant(12));
      // Setup and launch the trySplit thread.
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<SplitResult> trySplitFuture =
          executorService.submit(
              () -> {
                try {
                  doFn.waitForSplitElementToBeProcessed();
                  SplitResult result = ((HandlesSplits) mainInput).trySplit(0);
                  Assert.assertNotNull(result);
                  return result;
                } finally {
                  doFn.trySplitPerformed();
                }
              });

      WindowedValue<?> splitValue =
          valueInWindows(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              window1,
              window2,
              window3);
      mainInput.accept(splitValue);
      SplitResult trySplitResult = trySplitFuture.get();

      // We expect that there are outputs from window1 and window2
      assertThat(
          mainOutputValues,
          contains(
              WindowedValues.of(
                  KV.of(
                      KV.of("7", KV.of(new OffsetRange(0, 3), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  splitValue.getTimestamp(),
                  window1,
                  splitValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      KV.of("7", KV.of(new OffsetRange(0, 3), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  splitValue.getTimestamp(),
                  window2,
                  splitValue.getPaneInfo())));

      SplitResult expectedElementSplit = createSplitResult(0);
      BundleApplication expectedElementSplitPrimary =
          Iterables.getOnlyElement(expectedElementSplit.getPrimaryRoots());
      ByteStringOutputStream primaryBytes = new ByteStringOutputStream();
      inputCoder.encode(
          WindowedValues.of(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              splitValue.getTimestamp(),
              window1,
              splitValue.getPaneInfo()),
          primaryBytes);
      BundleApplication expectedWindowedPrimary =
          BundleApplication.newBuilder()
              .setElement(primaryBytes.toByteString())
              .setInputId(ParDoTranslation.getMainInputName(pTransform))
              .setTransformId(TEST_TRANSFORM_ID)
              .build();
      DelayedBundleApplication expectedElementSplitResidual =
          Iterables.getOnlyElement(expectedElementSplit.getResidualRoots());
      ByteStringOutputStream residualBytes = new ByteStringOutputStream();
      inputCoder.encode(
          WindowedValues.of(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              splitValue.getTimestamp(),
              window3,
              splitValue.getPaneInfo()),
          residualBytes);
      DelayedBundleApplication expectedWindowedResidual =
          DelayedBundleApplication.newBuilder()
              .setApplication(
                  BundleApplication.newBuilder()
                      .setElement(residualBytes.toByteString())
                      .setInputId(ParDoTranslation.getMainInputName(pTransform))
                      .setTransformId(TEST_TRANSFORM_ID)
                      .build())
              .build();
      assertThat(
          trySplitResult.getPrimaryRoots(),
          containsInAnyOrder(expectedWindowedPrimary, expectedElementSplitPrimary));
      assertThat(
          trySplitResult.getResidualRoots(),
          containsInAnyOrder(expectedWindowedResidual, expectedElementSplitResidual));
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWithoutObservingWindow()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .components(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllEnvironments(Collections.emptyMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .build())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new SplittableTruncateSizedRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      assertEquals(0.7, ((HandlesSplits) mainInput).getProgress(), 0.0);
      assertEquals(createSplitResult(0.4), ((HandlesSplits) mainInput).trySplit(0.4));
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .components(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllEnvironments(Collections.emptyMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .build())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new SplittableTruncateSizedRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  5.0)));
      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  2.0)));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0)),
              valueInGlobalWindow(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0))));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedTruncateAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
                  .withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .components(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllEnvironments(Collections.emptyMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .build())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new SplittableTruncateSizedRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      assertThat(
          mainOutputValues,
          contains(
              WindowedValues.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPaneInfo())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void
        testProcessElementForWindowedTruncateAndSizeRestrictionWithNonWindowObservingOptimization()
            throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .components(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllEnvironments(Collections.emptyMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .build())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new SplittableTruncateSizedRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValues.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPaneInfo())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }
  }

  @RunWith(JUnit4.class)
  public static class SplitTest {
    @Rule public final ExpectedException expected = ExpectedException.none();
    private IntervalWindow window1;
    private IntervalWindow window2;
    private IntervalWindow window3;
    private WindowedValue<String> currentElement;
    private OffsetRange currentRestriction;
    private Instant currentWatermarkEstimatorState;
    KV<Instant, Instant> watermarkAndState;

    private KV<WindowedValue, WindowedValue> createSplitAcrossWindows(
        List<BoundedWindow> primaryWindows, List<BoundedWindow> residualWindows) {
      return KV.of(
          primaryWindows.isEmpty()
              ? null
              : WindowedValues.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(currentRestriction, currentWatermarkEstimatorState)),
                  currentElement.getTimestamp(),
                  primaryWindows,
                  currentElement.getPaneInfo()),
          residualWindows.isEmpty()
              ? null
              : WindowedValues.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(currentRestriction, currentWatermarkEstimatorState)),
                  currentElement.getTimestamp(),
                  residualWindows,
                  currentElement.getPaneInfo()));
    }

    @Before
    public void setUp() {
      window1 = new IntervalWindow(Instant.ofEpochMilli(0), Instant.ofEpochMilli(10));
      window2 = new IntervalWindow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(20));
      window3 = new IntervalWindow(Instant.ofEpochMilli(20), Instant.ofEpochMilli(30));
      currentElement =
          WindowedValues.of(
              "a",
              Instant.ofEpochMilli(57),
              ImmutableList.of(window1, window2, window3),
              PaneInfo.NO_FIRING);
      currentRestriction = new OffsetRange(0L, 100L);
      currentWatermarkEstimatorState = Instant.ofEpochMilli(21);
      watermarkAndState = KV.of(Instant.ofEpochMilli(42), Instant.ofEpochMilli(42));
    }

    private static final String PROCESS_TRANSFORM_ID = "processPTransformId";
    private static final String TRUNCATE_TRANSFORM_ID = "truncatePTransformId";
    private static final String PROCESS_INPUT_ID = "processInputId";
    private static final String TRUNCATE_INPUT_ID = "truncateInputId";
    private static final String TRUNCATE_OUTPUT_ID = "truncateOutputId";

    private HandlesSplits.SplitResult getProcessElementSplit(String transformId, String inputId) {
      return SplitResult.of(
          ImmutableList.of(
              BundleApplication.newBuilder()
                  .setTransformId(transformId)
                  .setInputId(inputId)
                  .build()),
          ImmutableList.of(
              DelayedBundleApplication.newBuilder()
                  .setApplication(
                      BundleApplication.newBuilder()
                          .setTransformId(transformId)
                          .setInputId(inputId)
                          .build())
                  .setRequestedTimeDelay(Durations.fromMillis(1000L))
                  .build()));
    }

    @Test
    public void testConstructSplitResultWithElementSplitFromDelegate() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      HandlesSplits.SplitResult elementSplit =
          getProcessElementSplit(PROCESS_TRANSFORM_ID, PROCESS_INPUT_ID);
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              null,
              elementSplit,
              fullInputCoder,
              null,
              null,
              TRUNCATE_TRANSFORM_ID,
              TRUNCATE_INPUT_ID,
              ImmutableList.of(TRUNCATE_OUTPUT_ID),
              null);
      assertEquals(elementSplit.getPrimaryRoots(), result.getPrimaryRoots());
      assertEquals(elementSplit.getResidualRoots(), result.getResidualRoots());
    }

    private HandlesSplits createSplitDelegate(
        double progress, double expectedFraction, SplitResult result) {
      return new HandlesSplits() {
        @Override
        public SplitResult trySplit(double fractionOfRemainder) {
          checkArgument(fractionOfRemainder == expectedFraction);
          return result;
        }

        @Override
        public double getProgress() {
          return progress;
        }
      };
    }

    @Test
    public void testTrySplitForTruncateCheckpointOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, splitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              splitDelegate,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateCheckpointOnFirstWindowAfterOneSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, splitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              splitDelegate,
              0,
              2);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.54, splitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              splitDelegate,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnMiddleWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.34, splitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              splitDelegate,
              1,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.2, splitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              splitDelegate,
              2,
              3);
      assertEquals(3, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of());
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnFirstWindowFallback() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(1.0, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              splitDelegate,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnLastWindowWhenNoElementSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      HandlesSplits splitDelegate = createSplitDelegate(1.0, 0.0, null);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              splitDelegate,
              2,
              3);
      assertNull(result);
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundUp() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.6,
              splitDelegate,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundDown() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.3,
              splitDelegate,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundDownOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          SplittableTruncateSizedRestrictionsDoFnRunner.computeSplitForTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.6,
              splitDelegate,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    private Coder getFullInputCoder(
        Coder elementCoder, Coder restrictionCoder, Coder watermarkStateCoder, Coder windowCoder) {
      Coder inputCoder =
          KvCoder.of(
              KvCoder.of(elementCoder, KvCoder.of(restrictionCoder, watermarkStateCoder)),
              DoubleCoder.of());
      return WindowedValues.getFullCoder(inputCoder, windowCoder);
    }
  }
}
