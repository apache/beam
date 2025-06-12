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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.range.OffsetRange;
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
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittablePairWithRestrictionDoFnRunner}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class SplittablePairWithRestrictionDoFnRunnerTest implements Serializable {

  @RunWith(JUnit4.class)
  public static class ExecutionTest implements Serializable {
    @Rule public transient ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

    public static final String TEST_TRANSFORM_ID = "pTransformId";

    private <K> org.apache.beam.sdk.util.construction.Timer<K> timerInGlobalWindow(
        K userKey, Instant holdTimestamp, Instant fireTimestamp) {
      return dynamicTimerInGlobalWindow(userKey, "", holdTimestamp, fireTimestamp);
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> clearedTimerInGlobalWindow(
        K userKey) {
      return clearedTimerInGlobalWindow(userKey, "");
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> clearedTimerInGlobalWindow(
        K userKey, String dynamicTimerTag) {
      return org.apache.beam.sdk.util.construction.Timer.cleared(
          userKey, dynamicTimerTag, Collections.singletonList(GlobalWindow.INSTANCE));
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> dynamicTimerInGlobalWindow(
        K userKey, String dynamicTimerTag, Instant holdTimestamp, Instant fireTimestamp) {
      return org.apache.beam.sdk.util.construction.Timer.of(
          userKey,
          dynamicTimerTag,
          Collections.singletonList(GlobalWindow.INSTANCE),
          fireTimestamp,
          holdTimestamp,
          PaneInfo.NO_FIRING);
    }

    private <T> WindowedValue<T> valueInWindows(
        T value, BoundedWindow window, BoundedWindow... windows) {
      return WindowedValues.of(
          value,
          window.maxTimestamp(),
          ImmutableList.<BoundedWindow>builder().add(window).add(windows).build(),
          PaneInfo.NO_FIRING);
    }

    /**
     * Produces an iterable side input {@link StateKey} for the test PTransform id in the global
     * window.
     */
    private StateKey iterableSideInputKey(String sideInputId) throws IOException {
      return iterableSideInputKey(
          sideInputId,
          ByteString.copyFrom(
              CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)));
    }

    /**
     * Produces an iterable side input {@link StateKey} for the test PTransform id in the supplied
     * window.
     */
    private StateKey iterableSideInputKey(String sideInputId, ByteString windowKey) {
      return StateKey.newBuilder()
          .setIterableSideInput(
              StateKey.IterableSideInput.newBuilder()
                  .setTransformId(TEST_TRANSFORM_ID)
                  .setSideInputId(sideInputId)
                  .setWindow(windowKey))
          .build();
    }

    private ByteString encode(String... values) throws IOException {
      ByteStringOutputStream out = new ByteStringOutputStream();
      for (String value : values) {
        StringUtf8Coder.of().encode(value, out);
      }
      return out.toByteString();
    }

    @Test
    public void testRegistration() {
      for (PTransformRunnerFactory.Registrar registrar :
          ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
        if (registrar instanceof SplittablePairWithRestrictionDoFnRunner.Registrar) {
          assertThat(
              registrar.getPTransformRunnerFactories(),
              IsMapContaining.hasKey(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN));
          return;
        }
      }
      fail("Expected registrar not found.");
    }

    private static void assertNoReportedProgress(List<BundleProgressReporter> reporters) {
      Map<String, ByteString> monitoringData = new HashMap<>();
      for (BundleProgressReporter reporter : reporters) {
        reporter.updateIntermediateMonitoringData(monitoringData);
      }
      assertThat(monitoringData.entrySet(), empty());
    }

    private static <T> T decode(Coder<T> coder, ByteString value) {
      try {
        return coder.decode(value.newInput());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Test
    public void testProcessElementForPairWithRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection.apply(
          TEST_TRANSFORM_ID,
          ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
              .withSideInputs(singletonSideInputView));
      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
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
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .build())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new SplittablePairWithRestrictionDoFnRunner.Factory().addRunnerForPTransform(context);

      assertThat(context.getStartBundleFunctions(), Matchers.empty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow("5"));
      mainInput.accept(valueInGlobalWindow("2"));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))))),
              valueInGlobalWindow(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))))));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedPairWithRestriction() throws Exception {
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
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
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
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .build())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new SplittablePairWithRestrictionDoFnRunner.Factory().addRunnerForPTransform(context);

      assertThat(context.getStartBundleFunctions(), Matchers.empty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue = valueInWindows("5", window1, window2);
      WindowedValue<?> secondValue = valueInWindows("2", window1, window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      assertThat(
          mainOutputValues,
          contains(
              WindowedValues.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  window1,
                  secondValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  window2,
                  secondValue.getPaneInfo())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedPairWithRestrictionWithNonWindowObservingOptimization()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
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
                      .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                      .putAllCoders(pProto.getComponents().getCodersMap())
                      .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                      .build())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new SplittablePairWithRestrictionDoFnRunner.Factory().addRunnerForPTransform(context);

      assertThat(context.getStartBundleFunctions(), Matchers.empty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue = valueInWindows("5", window1, window2);
      WindowedValue<?> secondValue = valueInWindows("2", window1, window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValues.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPaneInfo()),
              WindowedValues.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  secondValue.getPaneInfo())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
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
  }
}
