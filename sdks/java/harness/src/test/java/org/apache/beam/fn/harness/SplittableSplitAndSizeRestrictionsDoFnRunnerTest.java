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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
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
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SplittableSplitAndSizeRestrictionsDoFnRunner}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class SplittableSplitAndSizeRestrictionsDoFnRunnerTest implements Serializable {
  public static final String TEST_TRANSFORM_ID = "pTransformId";

  private <T> WindowedValue<T> valueInWindows(
      T value, BoundedWindow window, BoundedWindow... windows) {
    return WindowedValues.of(
        value,
        window.maxTimestamp(),
        ImmutableList.<BoundedWindow>builder().add(window).add(windows).build(),
        PaneInfo.NO_FIRING);
  }

  private static final MonitoringInfo WORK_COMPLETED_MI =
      MonitoringInfo.newBuilder()
          .setUrn(Urns.WORK_COMPLETED)
          .setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE)
          .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID)
          .build();

  private static final MonitoringInfo WORK_REMAINING_MI =
      MonitoringInfo.newBuilder()
          .setUrn(Urns.WORK_REMAINING)
          .setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE)
          .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID)
          .build();

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
  public void testRegistration() {
    for (PTransformRunnerFactory.Registrar registrar :
        ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
      if (registrar instanceof SplittableSplitAndSizeRestrictionsDoFnRunner.Registrar) {
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(
                PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }

  @Test
  public void testProcessElementForSplitAndSizeRestriction() throws Exception {
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
                            .equals(
                                PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
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
    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    Coder coder =
        KvCoder.of(
            KvCoder.of(StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
            DoubleCoder.of());
    context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

    new SplittableSplitAndSizeRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

    assertTrue(context.getStartBundleFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput = context.getPCollectionConsumer(inputPCollectionId);
    mainInput.accept(
        valueInGlobalWindow(
            KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE))));
    mainInput.accept(
        valueInGlobalWindow(
            KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE))));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0))));
    mainOutputValues.clear();

    assertTrue(context.getFinishBundleFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(context.getTearDownFunctions()).run();
    assertThat(mainOutputValues, empty());
  }

  @Test
  public void testProcessElementForWindowedSplitAndSizeRestriction() throws Exception {
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
                            .equals(
                                PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
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
    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    Coder coder =
        KvCoder.of(
            KvCoder.of(StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
            DoubleCoder.of());
    context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

    new SplittableSplitAndSizeRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

    assertTrue(context.getStartBundleFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput = context.getPCollectionConsumer(inputPCollectionId);
    IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    WindowedValue<?> firstValue =
        valueInWindows(
            KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
            window1,
            window2);
    WindowedValue<?> secondValue =
        WindowedValues.of(
            KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
            firstValue.getTimestamp().plus(Duration.standardSeconds(1)),
            ImmutableList.of(window1, window2),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    mainInput.accept(firstValue);
    mainInput.accept(secondValue);

    // Since the DoFn observes the window and it may affect the output, each input is processed
    // separately and each
    // output is per-window.
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
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
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
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                secondValue.getTimestamp(),
                window1,
                secondValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                secondValue.getTimestamp(),
                window1,
                secondValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                secondValue.getTimestamp(),
                window2,
                secondValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
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
  public void
      testProcessElementForWindowedSplitAndSizeRestrictionWithNonWindowObservingOptimization()
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
            SplittableParDoExpander.createSizedReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(
                                PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
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
                    .putAllPcollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
                    .putAllCoders(pProto.getComponents().getCodersMap())
                    .putAllWindowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
                    .build())
            .build();
    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    Coder coder =
        KvCoder.of(
            KvCoder.of(StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
            DoubleCoder.of());
    context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

    new SplittableSplitAndSizeRestrictionsDoFnRunner.Factory().addRunnerForPTransform(context);

    assertTrue(context.getStartBundleFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput = context.getPCollectionConsumer(inputPCollectionId);
    IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    WindowedValue<?> firstValue =
        valueInWindows(
            KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
            window1,
            window2);
    WindowedValue<?> secondValue =
        valueInWindows(
            KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
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
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPaneInfo()),
            WindowedValues.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
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
