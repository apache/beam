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

import static org.apache.beam.sdk.options.ExperimentalOptions.addExperiment;
import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.PTransformRunnerFactory.ProgressRequestCallback;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.FakeBeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.SplittableParDoExpander;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link FnApiDoFnRunner}. */
@RunWith(JUnit4.class)
public class FnApiDoFnRunnerTest implements Serializable {

  @Rule public transient ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

  private static final Logger LOG = LoggerFactory.getLogger(FnApiDoFnRunnerTest.class);

  public static final String TEST_TRANSFORM_ID = "pTransformId";

  private static class ConcatCombineFn extends CombineFn<String, String, String> {
    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String accumulator, String input) {
      return accumulator.concat(input);
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumulators) {
      StringBuilder builder = new StringBuilder();
      for (String value : accumulators) {
        builder.append(value);
      }
      return builder.toString();
    }

    @Override
    public String extractOutput(String accumulator) {
      return accumulator;
    }
  }

  private static class TestStatefulDoFn extends DoFn<KV<String, String>, String> {
    private static final TupleTag<String> mainOutput = new TupleTag<>("mainOutput");
    private static final TupleTag<String> additionalOutput = new TupleTag<>("output");

    @StateId("value")
    private final StateSpec<ValueState<String>> valueStateSpec =
        StateSpecs.value(StringUtf8Coder.of());

    @StateId("bag")
    private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

    @StateId("combine")
    private final StateSpec<CombiningState<String, String, String>> combiningStateSpec =
        StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFn());

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("value") ValueState<String> valueState,
        @StateId("bag") BagState<String> bagState,
        @StateId("combine") CombiningState<String, String, String> combiningState) {
      context.output("value:" + valueState.read());
      valueState.write(context.element().getValue());

      context.output("bag:" + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());

      context.output("combine:" + combiningState.read());
      combiningState.add(context.element().getValue());
    }
  }

  @Test
  public void testUsingUserState() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> valuePCollection =
        p.apply(Create.of(KV.of("unused", "unused")));
    PCollection<String> outputPCollection =
        valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestStatefulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("value", "X"), encode("X0"),
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("combine", "X"), encode("X0")));

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X1")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y1")));
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X2")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y2")));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow("value:X0"),
            valueInGlobalWindow("bag:[X0]"),
            valueInGlobalWindow("combine:X0"),
            valueInGlobalWindow("value:null"),
            valueInGlobalWindow("bag:[]"),
            valueInGlobalWindow("combine:"),
            valueInGlobalWindow("value:X1"),
            valueInGlobalWindow("bag:[X0, X1]"),
            valueInGlobalWindow("combine:X0X1"),
            valueInGlobalWindow("value:Y1"),
            valueInGlobalWindow("bag:[Y1]"),
            valueInGlobalWindow("combine:Y1")));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("value", "X"), encode("X2"))
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2"))
            .put(bagUserStateKey("combine", "X"), encode("X0X1X2"))
            .put(bagUserStateKey("value", "Y"), encode("Y2"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2"))
            .put(bagUserStateKey("combine", "Y"), encode("Y1Y2"))
            .build(),
        fakeClient.getData());
  }

  /** Produces a bag user {@link StateKey} for the test PTransform id in the global window. */
  private StateKey bagUserStateKey(String userStateId, String key) throws IOException {
    return StateKey.newBuilder()
        .setBagUserState(
            StateKey.BagUserState.newBuilder()
                .setTransformId(TEST_TRANSFORM_ID)
                .setUserStateId(userStateId)
                .setKey(encode(key))
                .setWindow(
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE))))
        .build();
  }

  private static class TestSideInputDoFn extends DoFn<String, String> {
    private final PCollectionView<String> defaultSingletonSideInput;
    private final PCollectionView<String> singletonSideInput;
    private final PCollectionView<Iterable<String>> iterableSideInput;
    private final TupleTag<String> additionalOutput;

    private TestSideInputDoFn(
        PCollectionView<String> defaultSingletonSideInput,
        PCollectionView<String> singletonSideInput,
        PCollectionView<Iterable<String>> iterableSideInput,
        TupleTag<String> additionalOutput) {
      this.defaultSingletonSideInput = defaultSingletonSideInput;
      this.singletonSideInput = singletonSideInput;
      this.iterableSideInput = iterableSideInput;
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element() + ":" + context.sideInput(defaultSingletonSideInput));
      context.output(context.element() + ":" + context.sideInput(singletonSideInput));
      for (String sideInputValue : context.sideInput(iterableSideInput)) {
        context.output(context.element() + ":" + sideInputValue);
      }
      context.output(additionalOutput, context.element() + ":additional");
    }
  }

  @Test
  public void testProcessElementWithSideInputsAndOutputs() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> defaultSingletonSideInputView =
        valuePCollection.apply(
            View.<String>asSingleton().withDefaultValue("defaultSingletonValue"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    TupleTag<String> mainOutput = new TupleTag<String>("main") {};
    TupleTag<String> additionalOutput = new TupleTag<String>("additional") {};
    PCollectionTuple outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(
                    new TestSideInputDoFn(
                        defaultSingletonSideInputView,
                        singletonSideInputView,
                        iterableSideInputView,
                        additionalOutput))
                .withSideInputs(
                    defaultSingletonSideInputView, singletonSideInputView, iterableSideInputView)
                .withOutputTags(mainOutput, TupleTagList.of(additionalOutput)));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(mainOutput));
    String additionalPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(additionalOutput));

    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID);

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            iterableSideInputKey(singletonSideInputView.getTagInternal().getId()),
            encode("singletonValue"),
            iterableSideInputKey(iterableSideInputView.getTagInternal().getId()),
            encode("iterableValue1", "iterableValue2", "iterableValue3"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.register(
        additionalPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(
        consumers.keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId, additionalPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow("X"));
    mainInput.accept(valueInGlobalWindow("Y"));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow("X:defaultSingletonValue"),
            valueInGlobalWindow("X:singletonValue"),
            valueInGlobalWindow("X:iterableValue1"),
            valueInGlobalWindow("X:iterableValue2"),
            valueInGlobalWindow("X:iterableValue3"),
            valueInGlobalWindow("Y:defaultSingletonValue"),
            valueInGlobalWindow("Y:singletonValue"),
            valueInGlobalWindow("Y:iterableValue1"),
            valueInGlobalWindow("Y:iterableValue2"),
            valueInGlobalWindow("Y:iterableValue3")));
    assertThat(
        additionalOutputValues,
        contains(valueInGlobalWindow("X:additional"), valueInGlobalWindow("Y:additional")));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  private static class TestNonWindowObservingDoFn extends DoFn<String, String> {
    private final TupleTag<String> additionalOutput;

    private TestNonWindowObservingDoFn(TupleTag<String> additionalOutput) {
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element() + ":main");
      context.output(additionalOutput, context.element() + ":additional");
    }
  }

  @Test
  public void testProcessElementWithNonWindowObservingOptimization() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused"))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));
    TupleTag<String> mainOutput = new TupleTag<String>("main") {};
    TupleTag<String> additionalOutput = new TupleTag<String>("additional") {};
    PCollectionTuple outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(new TestNonWindowObservingDoFn(additionalOutput))
                .withOutputTags(mainOutput, TupleTagList.of(additionalOutput)));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(mainOutput));
    String additionalPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(additionalOutput));

    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.register(
        additionalPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(
        consumers.keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId, additionalPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(
        valueInWindows(
            "X",
            new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
            new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))));
    mainInput.accept(
        valueInWindows(
            "Y",
            new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
            new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1))));
    // Ensure that each output element is in all the windows and not one per window.
    assertThat(
        mainOutputValues,
        contains(
            valueInWindows(
                "X:main",
                new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
                new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))),
            valueInWindows(
                "Y:main",
                new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
                new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1)))));
    assertThat(
        additionalOutputValues,
        contains(
            valueInWindows(
                "X:additional",
                new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
                new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))),
            valueInWindows(
                "Y:additional",
                new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
                new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1)))));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  private static class TestSideInputIsAccessibleForDownstreamCallersDoFn
      extends DoFn<String, Iterable<String>> {
    public static final String USER_COUNTER_NAME = "userCountedElems";
    private final Counter countedElements =
        Metrics.counter(TestSideInputIsAccessibleForDownstreamCallersDoFn.class, USER_COUNTER_NAME);

    private final PCollectionView<Iterable<String>> iterableSideInput;

    private TestSideInputIsAccessibleForDownstreamCallersDoFn(
        PCollectionView<Iterable<String>> iterableSideInput) {
      this.iterableSideInput = iterableSideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      countedElements.inc();
      context.output(context.sideInput(iterableSideInput));
    }
  }

  @Test
  public void testSideInputIsAccessibleForDownstreamCallers() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
    IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
    IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
    ByteString encodedWindowA =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
    ByteString encodedWindowB =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused")).apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                .withSideInputs(iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindows("X", windowA));
    mainInput.accept(valueInWindows("Y", windowB));
    assertThat(mainOutputValues, hasSize(2));
    assertThat(
        mainOutputValues.get(0).getValue(),
        contains("iterableValue1A", "iterableValue2A", "iterableValue3A"));
    assertThat(
        mainOutputValues.get(1).getValue(),
        contains("iterableValue1B", "iterableValue2B", "iterableValue3B"));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  /** @return a test MetricUpdate for expected metrics to compare against */
  public MetricUpdate create(String stepName, MetricName name, long value) {
    return MetricUpdate.create(MetricKey.create(stepName, name), value);
  }

  @Test
  public void testUsingMetrics() throws Exception {
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    MetricsContainerImpl metricsContainer = metricsContainerRegistry.getUnboundContainer();
    Closeable closeable = MetricsEnvironment.scopedMetricsContainer(metricsContainer);
    FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
    IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
    IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
    ByteString encodedWindowA =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
    ByteString encodedWindowB =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused")).apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                .withSideInputs(iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();

    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindows("X", windowA));
    mainInput.accept(valueInWindows("Y", windowB));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    MetricsContainer mc = MetricsEnvironment.getCurrentContainer();

    List<MonitoringInfo> expected = new ArrayList<MonitoringInfo>();
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "Window.Into()/Window.Assign.out");
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(
        MonitoringInfoConstants.Labels.PCOLLECTION,
        "pTransformId/ParMultiDo(TestSideInputIsAccessibleForDownstreamCallers).output");
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    builder = new SimpleMonitoringInfoBuilder();
    builder
        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
        .setLabel(
            MonitoringInfoConstants.Labels.NAMESPACE,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.class.getName())
        .setLabel(
            MonitoringInfoConstants.Labels.NAME,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.USER_COUNTER_NAME);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    closeable.close();
    List<MonitoringInfo> result = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : metricsContainerRegistry.getMonitoringInfos()) {
      result.add(mi);
    }
    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testTimers() throws Exception {
    dateTimeProvider.setDateTimeFixed(10000L);

    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> valuePCollection =
        p.apply(Create.of(KV.of("unused", "unused")));
    PCollection<String> outputPCollection =
        valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestTimerfulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environment.getDefaultInstance());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0))
            .toBuilder()
            .build();

    FakeBeamFnStateClient fakeStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("bag", "A"), encode("A0"),
                bagUserStateKey("bag", "C"), encode("C0")));
    FakeBeamFnTimerClient fakeTimerClient = new FakeBeamFnTimerClient();

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeStateClient,
            fakeTimerClient,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    LogicalEndpoint eventTimer = LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-event");
    LogicalEndpoint processingTimer =
        LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-processing");
    LogicalEndpoint eventFamilyTimer =
        LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "tfs-event-family");
    LogicalEndpoint processingFamilyTimer =
        LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "tfs-processing-family");
    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X1"), new Instant(1000L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y1"), new Instant(1100L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X2"), new Instant(1200L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y2"), new Instant(1300L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("A", new Instant(1400L), new Instant(2400L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("B", new Instant(1500L), new Instant(2500L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("A", new Instant(1600L), new Instant(2600L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("X", new Instant(1700L), new Instant(2700L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("C", new Instant(1800L), new Instant(2800L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("B", new Instant(1900L), new Instant(2900L)));
    fakeTimerClient.sendTimer(
        eventFamilyTimer,
        dynamicTimerInGlobalWindow("B", "event-timer2", new Instant(2000L), new Instant(3000L)));
    fakeTimerClient.sendTimer(
        processingFamilyTimer,
        dynamicTimerInGlobalWindow(
            "Y", "processing-timer2", new Instant(2100L), new Instant(3100L)));
    assertThat(
        mainOutputValues,
        contains(
            timestampedValueInGlobalWindow("mainX[X0]", new Instant(1000L)),
            timestampedValueInGlobalWindow("mainY[]", new Instant(1100L)),
            timestampedValueInGlobalWindow("mainX[X0, X1]", new Instant(1200L)),
            timestampedValueInGlobalWindow("mainY[Y1]", new Instant(1300L)),
            timestampedValueInGlobalWindow("event[A0]", new Instant(1400L)),
            timestampedValueInGlobalWindow("event[]", new Instant(1500L)),
            timestampedValueInGlobalWindow("event[A0, event]", new Instant(1600L)),
            timestampedValueInGlobalWindow("processing[X0, X1, X2]", new Instant(1700L)),
            timestampedValueInGlobalWindow("processing[C0]", new Instant(1800L)),
            timestampedValueInGlobalWindow("processing[event]", new Instant(1900L)),
            timestampedValueInGlobalWindow("event-family[event, processing]", new Instant(2000L)),
            timestampedValueInGlobalWindow("processing-family[Y1, Y2]", new Instant(2100L))));
    assertThat(
        fakeTimerClient.getTimers(eventTimer),
        contains(
            timerInGlobalWindow("X", new Instant(1000L), new Instant(1001L)),
            timerInGlobalWindow("Y", new Instant(1100L), new Instant(1101L)),
            timerInGlobalWindow("X", new Instant(1200L), new Instant(1201L)),
            timerInGlobalWindow("Y", new Instant(1300L), new Instant(1301L)),
            timerInGlobalWindow("A", new Instant(1400L), new Instant(2411L)),
            timerInGlobalWindow("B", new Instant(1500L), new Instant(2511L)),
            timerInGlobalWindow("A", new Instant(1600L), new Instant(2611L)),
            timerInGlobalWindow("X", new Instant(1700L), new Instant(1721L)),
            timerInGlobalWindow("C", new Instant(1800L), new Instant(1821L)),
            timerInGlobalWindow("B", new Instant(1900L), new Instant(1921L)),
            timerInGlobalWindow("B", new Instant(2000L), new Instant(2031L)),
            timerInGlobalWindow("Y", new Instant(2100L), new Instant(2141L))));
    assertThat(
        fakeTimerClient.getTimers(processingTimer),
        contains(
            timerInGlobalWindow("X", new Instant(1000L), new Instant(10002L)),
            timerInGlobalWindow("Y", new Instant(1100L), new Instant(10002L)),
            timerInGlobalWindow("X", new Instant(1200L), new Instant(10002L)),
            timerInGlobalWindow("Y", new Instant(1300L), new Instant(10002L)),
            timerInGlobalWindow("A", new Instant(1400L), new Instant(10012L)),
            timerInGlobalWindow("B", new Instant(1500L), new Instant(10012L)),
            timerInGlobalWindow("A", new Instant(1600L), new Instant(10012L)),
            timerInGlobalWindow("X", new Instant(1700L), new Instant(10022L)),
            timerInGlobalWindow("C", new Instant(1800L), new Instant(10022L)),
            timerInGlobalWindow("B", new Instant(1900L), new Instant(10022L)),
            timerInGlobalWindow("B", new Instant(2000L), new Instant(10032L)),
            timerInGlobalWindow("Y", new Instant(2100L), new Instant(10042L))));
    assertThat(
        fakeTimerClient.getTimers(eventFamilyTimer),
        contains(
            dynamicTimerInGlobalWindow("X", "event-timer1", new Instant(1000L), new Instant(1003L)),
            dynamicTimerInGlobalWindow("Y", "event-timer1", new Instant(1100L), new Instant(1103L)),
            dynamicTimerInGlobalWindow("X", "event-timer1", new Instant(1200L), new Instant(1203L)),
            dynamicTimerInGlobalWindow("Y", "event-timer1", new Instant(1300L), new Instant(1303L)),
            dynamicTimerInGlobalWindow("A", "event-timer1", new Instant(1400L), new Instant(2413L)),
            dynamicTimerInGlobalWindow("B", "event-timer1", new Instant(1500L), new Instant(2513L)),
            dynamicTimerInGlobalWindow("A", "event-timer1", new Instant(1600L), new Instant(2613L)),
            dynamicTimerInGlobalWindow("X", "event-timer1", new Instant(1700L), new Instant(1723L)),
            dynamicTimerInGlobalWindow("C", "event-timer1", new Instant(1800L), new Instant(1823L)),
            dynamicTimerInGlobalWindow("B", "event-timer1", new Instant(1900L), new Instant(1923L)),
            dynamicTimerInGlobalWindow("B", "event-timer1", new Instant(2000L), new Instant(2033L)),
            dynamicTimerInGlobalWindow(
                "Y", "event-timer1", new Instant(2100L), new Instant(2143L))));
    assertThat(
        fakeTimerClient.getTimers(processingFamilyTimer),
        contains(
            dynamicTimerInGlobalWindow(
                "X", "processing-timer1", new Instant(1000L), new Instant(10004L)),
            dynamicTimerInGlobalWindow(
                "Y", "processing-timer1", new Instant(1100L), new Instant(10004L)),
            dynamicTimerInGlobalWindow(
                "X", "processing-timer1", new Instant(1200L), new Instant(10004L)),
            dynamicTimerInGlobalWindow(
                "Y", "processing-timer1", new Instant(1300L), new Instant(10004L)),
            dynamicTimerInGlobalWindow(
                "A", "processing-timer1", new Instant(1400L), new Instant(10014L)),
            dynamicTimerInGlobalWindow(
                "B", "processing-timer1", new Instant(1500L), new Instant(10014L)),
            dynamicTimerInGlobalWindow(
                "A", "processing-timer1", new Instant(1600L), new Instant(10014L)),
            dynamicTimerInGlobalWindow(
                "X", "processing-timer1", new Instant(1700L), new Instant(10024L)),
            dynamicTimerInGlobalWindow(
                "C", "processing-timer1", new Instant(1800L), new Instant(10024L)),
            dynamicTimerInGlobalWindow(
                "B", "processing-timer1", new Instant(1900L), new Instant(10024L)),
            dynamicTimerInGlobalWindow(
                "B", "processing-timer1", new Instant(2000L), new Instant(10034L)),
            dynamicTimerInGlobalWindow(
                "Y", "processing-timer1", new Instant(2100L), new Instant(10044L))));
    mainOutputValues.clear();

    assertFalse(fakeTimerClient.isOutboundClosed(eventTimer));
    assertFalse(fakeTimerClient.isOutboundClosed(processingTimer));
    assertFalse(fakeTimerClient.isOutboundClosed(eventFamilyTimer));
    assertFalse(fakeTimerClient.isOutboundClosed(processingFamilyTimer));
    fakeTimerClient.closeInbound(eventTimer);
    fakeTimerClient.closeInbound(processingTimer);
    fakeTimerClient.closeInbound(eventFamilyTimer);
    fakeTimerClient.closeInbound(processingFamilyTimer);

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    assertTrue(fakeTimerClient.isOutboundClosed(eventTimer));
    assertTrue(fakeTimerClient.isOutboundClosed(processingTimer));
    assertTrue(fakeTimerClient.isOutboundClosed(eventFamilyTimer));
    assertTrue(fakeTimerClient.isOutboundClosed(processingFamilyTimer));

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2", "processing"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2", "processing-family"))
            .put(bagUserStateKey("bag", "A"), encode("A0", "event", "event"))
            .put(bagUserStateKey("bag", "B"), encode("event", "processing", "event-family"))
            .put(bagUserStateKey("bag", "C"), encode("C0", "processing"))
            .build(),
        fakeStateClient.getData());
  }

  private <K> org.apache.beam.runners.core.construction.Timer<K> timerInGlobalWindow(
      K userKey, Instant holdTimestamp, Instant fireTimestamp) {
    return dynamicTimerInGlobalWindow(userKey, "", holdTimestamp, fireTimestamp);
  }

  private <K> org.apache.beam.runners.core.construction.Timer<K> dynamicTimerInGlobalWindow(
      K userKey, String dynamicTimerTag, Instant holdTimestamp, Instant fireTimestamp) {
    return org.apache.beam.runners.core.construction.Timer.of(
        userKey,
        dynamicTimerTag,
        Collections.singletonList(GlobalWindow.INSTANCE),
        fireTimestamp,
        holdTimestamp,
        PaneInfo.NO_FIRING);
  }

  private <T> WindowedValue<T> valueInWindows(
      T value, BoundedWindow window, BoundedWindow... windows) {
    return WindowedValue.of(
        value,
        window.maxTimestamp(),
        ImmutableList.<BoundedWindow>builder().add(window).add(windows).build(),
        PaneInfo.NO_FIRING);
  }

  private static class TestTimerfulDoFn extends DoFn<KV<String, String>, String> {
    @StateId("bag")
    private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

    @TimerId("event")
    private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("processing")
    private final TimerSpec processingTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerFamily("event-family")
    private final TimerSpec eventTimerFamilySpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    @TimerFamily("processing-family")
    private final TimerSpec processingTimerFamilySpec =
        TimerSpecs.timerMap(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer,
        @TimerFamily("event-family") TimerMap eventTimerFamily,
        @TimerFamily("processing-family") TimerMap processingTimerFamily) {
      context.output("main" + context.element().getKey() + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(1L));
      processingTimeTimer.offset(Duration.millis(2L));
      processingTimeTimer.setRelative();
      eventTimerFamily
          .get("event-timer1")
          .withOutputTimestamp(context.timestamp())
          .set(context.timestamp().plus(3L));
      processingTimerFamily.get("processing-timer1").offset(Duration.millis(4L)).setRelative();
    }

    @OnTimer("event")
    public void eventTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer,
        @TimerFamily("event-family") TimerMap eventTimerFamily,
        @TimerFamily("processing-family") TimerMap processingTimerFamily) {
      context.output("event" + Iterables.toString(bagState.read()));
      bagState.add("event");
      eventTimeTimer
          .withOutputTimestamp(context.timestamp())
          .set(context.fireTimestamp().plus(11L));
      processingTimeTimer.offset(Duration.millis(12L));
      processingTimeTimer.setRelative();
      eventTimerFamily
          .get("event-timer1")
          .withOutputTimestamp(context.timestamp())
          .set(context.fireTimestamp().plus(13L));
      processingTimerFamily.get("processing-timer1").offset(Duration.millis(14L)).setRelative();
    }

    @OnTimer("processing")
    public void processingTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer,
        @TimerFamily("event-family") TimerMap eventTimerFamily,
        @TimerFamily("processing-family") TimerMap processingTimerFamily) {
      context.output("processing" + Iterables.toString(bagState.read()));
      bagState.add("processing");
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(21L));
      processingTimeTimer.offset(Duration.millis(22L));
      processingTimeTimer.setRelative();
      eventTimerFamily
          .get("event-timer1")
          .withOutputTimestamp(context.timestamp())
          .set(context.timestamp().plus(23L));
      processingTimerFamily.get("processing-timer1").offset(Duration.millis(24L)).setRelative();
    }

    @OnTimerFamily("event-family")
    public void eventFamilyOnTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer,
        @TimerFamily("event-family") TimerMap eventTimerFamily,
        @TimerFamily("processing-family") TimerMap processingTimerFamily) {
      context.output("event-family" + Iterables.toString(bagState.read()));
      bagState.add("event-family");
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(31L));
      processingTimeTimer.offset(Duration.millis(32L));
      processingTimeTimer.setRelative();
      eventTimerFamily
          .get("event-timer1")
          .withOutputTimestamp(context.timestamp())
          .set(context.timestamp().plus(33L));
      processingTimerFamily.get("processing-timer1").offset(Duration.millis(34L)).setRelative();
    }

    @OnTimerFamily("processing-family")
    public void processingFamilyOnTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer,
        @TimerFamily("event-family") TimerMap eventTimerFamily,
        @TimerFamily("processing-family") TimerMap processingTimerFamily) {
      context.output("processing-family" + Iterables.toString(bagState.read()));
      bagState.add("processing-family");
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(41L));
      processingTimeTimer.offset(Duration.millis(42L));
      processingTimeTimer.setRelative();
      eventTimerFamily
          .get("event-timer1")
          .withOutputTimestamp(context.timestamp())
          .set(context.timestamp().plus(43L));
      processingTimerFamily.get("processing-timer1").offset(Duration.millis(44L)).setRelative();
    }
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
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  @Test
  public void testRegistration() {
    for (PTransformRunnerFactory.Registrar registrar :
        ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
      if (registrar instanceof FnApiDoFnRunner.Registrar) {
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(PTransformTranslation.PAR_DO_TRANSFORM_URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }

  /**
   * The trySplit testing of this splittable DoFn is done when processing the {@link
   * NonWindowObservingTestSplittableDoFn#SPLIT_ELEMENT}. Always checkpoints at element {@link
   * NonWindowObservingTestSplittableDoFn#CHECKPOINT_UPPER_BOUND}.
   *
   * <p>The expected thread flow is:
   *
   * <ul>
   *   <li>splitting thread: {@link
   *       NonWindowObservingTestSplittableDoFn#waitForSplitElementToBeProcessed()}
   *   <li>process element thread: {@link
   *       NonWindowObservingTestSplittableDoFn#enableAndWaitForTrySplitToHappen()}
   *   <li>splitting thread: perform try split
   *   <li>splitting thread: {@link
   *       NonWindowObservingTestSplittableDoFn#releaseWaitingProcessElementThread()}
   * </ul>
   */
  static class NonWindowObservingTestSplittableDoFn extends DoFn<String, String> {
    private static final ConcurrentMap<String, KV<CountDownLatch, CountDownLatch>>
        DOFN_INSTANCE_TO_LOCK = new ConcurrentHashMap<>();
    private static final long SPLIT_ELEMENT = 3;
    private static final long CHECKPOINT_UPPER_BOUND = 8;

    private KV<CountDownLatch, CountDownLatch> getLatches() {
      return DOFN_INSTANCE_TO_LOCK.computeIfAbsent(
          this.uuid, (uuid) -> KV.of(new CountDownLatch(1), new CountDownLatch(1)));
    }

    public void enableAndWaitForTrySplitToHappen() throws Exception {
      KV<CountDownLatch, CountDownLatch> latches = getLatches();
      latches.getKey().countDown();
      if (!latches.getValue().await(30, TimeUnit.SECONDS)) {
        fail("Failed to wait for trySplit to occur.");
      }
    }

    public void waitForSplitElementToBeProcessed() throws Exception {
      KV<CountDownLatch, CountDownLatch> latches = getLatches();
      if (!latches.getKey().await(30, TimeUnit.SECONDS)) {
        fail("Failed to wait for split element to be processed.");
      }
    }

    public void releaseWaitingProcessElementThread() {
      KV<CountDownLatch, CountDownLatch> latches = getLatches();
      latches.getValue().countDown();
    }

    private final String uuid;

    private NonWindowObservingTestSplittableDoFn() {
      this.uuid = UUID.randomUUID().toString();
    }

    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context,
        RestrictionTracker<OffsetRange, Long> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator)
        throws Exception {
      long checkpointUpperBound = CHECKPOINT_UPPER_BOUND;
      long position = tracker.currentRestriction().getFrom();
      boolean claimStatus;
      while (true) {
        claimStatus = (tracker.tryClaim(position));
        if (!claimStatus) {
          break;
        } else if (position == SPLIT_ELEMENT) {
          enableAndWaitForTrySplitToHappen();
        }
        context.outputWithTimestamp(
            context.element() + ":" + position, GlobalWindow.TIMESTAMP_MIN_VALUE.plus(position));
        watermarkEstimator.setWatermark(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(position));
        position += 1L;
        if (position == checkpointUpperBound) {
          break;
        }
      }
      if (!claimStatus) {
        return ProcessContinuation.stop();
      } else {
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(54321L));
      }
    }

    @GetInitialRestriction
    public OffsetRange restriction(@Element String element) {
      return new OffsetRange(0, Integer.parseInt(element));
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction);
    }

    @SplitRestriction
    public void splitRange(@Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }

    @TruncateRestriction
    public TruncateResult<OffsetRange> truncateRestriction(@Restriction OffsetRange range) {
      return TruncateResult.of(new OffsetRange(range.getFrom(), range.getTo() / 2));
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return GlobalWindow.TIMESTAMP_MIN_VALUE;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermark) {
      return new WatermarkEstimators.Manual(watermark);
    }
  }

  /**
   * A window observing variant of {@link NonWindowObservingTestSplittableDoFn} which uses the side
   * inputs to choose the checkpoint upper bound.
   */
  static class WindowObservingTestSplittableDoFn extends NonWindowObservingTestSplittableDoFn {

    private final PCollectionView<String> singletonSideInput;

    private WindowObservingTestSplittableDoFn(PCollectionView<String> singletonSideInput) {
      this.singletonSideInput = singletonSideInput;
    }

    @Override
    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context,
        RestrictionTracker<OffsetRange, Long> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator)
        throws Exception {
      long checkpointUpperBound = Long.parseLong(context.sideInput(singletonSideInput));
      long position = tracker.currentRestriction().getFrom();
      boolean claimStatus;
      while (true) {
        claimStatus = (tracker.tryClaim(position));
        if (!claimStatus) {
          break;
        } else if (position == NonWindowObservingTestSplittableDoFn.SPLIT_ELEMENT) {
          enableAndWaitForTrySplitToHappen();
        }
        context.outputWithTimestamp(
            context.element() + ":" + position, GlobalWindow.TIMESTAMP_MIN_VALUE.plus(position));
        watermarkEstimator.setWatermark(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(position));
        position += 1L;
        if (position == checkpointUpperBound) {
          break;
        }
      }
      if (!claimStatus) {
        return ProcessContinuation.stop();
      } else {
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(54321L));
      }
    }
  }

  @Test
  public void testProcessElementForSizedElementAndRestriction() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    WindowObservingTestSplittableDoFn doFn =
        new WindowObservingTestSplittableDoFn(singletonSideInputView);
    valuePCollection.apply(
        TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

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
                                PTransformTranslation
                                    .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
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
        WindowedValue.getFullCoder(
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
    String outputPCollectionId = pTransform.getOutputsOrThrow("output");

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            iterableSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("8"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();
    List<ProgressRequestCallback> progressRequestCallbacks = new ArrayList<>();
    BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            progressRequestCallbacks::add,
            splitListener,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    {
      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  5.0)));
      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      // Since the side input upperBound is 8 we will process 5, 6, and 7 then checkpoint.
      // We expect that the watermark advances to MIN + 7 and that the primary represents [5, 8)
      // with
      // the original watermark while the residual represents [8, 10) with the new MIN + 7
      // watermark.
      BundleApplication primaryRoot = Iterables.getOnlyElement(splitListener.getPrimaryRoots());
      DelayedBundleApplication residualRoot =
          Iterables.getOnlyElement(splitListener.getResidualRoots());
      assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
      assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(
          ParDoTranslation.getMainInputName(pTransform),
          residualRoot.getApplication().getInputId());
      assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      assertEquals(
          valueInGlobalWindow(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(5, 8), GlobalWindow.TIMESTAMP_MIN_VALUE)), 3.0)),
          inputCoder.decode(primaryRoot.getElement().newInput()));
      assertEquals(
          valueInGlobalWindow(
              KV.of(
                  KV.of(
                      "5", KV.of(new OffsetRange(8, 10), GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7))),
                  2.0)),
          inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
      Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7);
      assertEquals(
          ImmutableMap.of(
              "output",
              org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                  .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                  .build()),
          residualRoot.getApplication().getOutputWatermarksMap());
      assertEquals(
          org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Duration.newBuilder()
              .setSeconds(54)
              .setNanos(321000000)
              .build(),
          residualRoot.getRequestedTimeDelay());
      splitListener.clear();

      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  2.0)));
      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      assertThat(
          mainOutputValues,
          contains(
              timestampedValueInGlobalWindow("5:5", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(5)),
              timestampedValueInGlobalWindow("5:6", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(6)),
              timestampedValueInGlobalWindow("5:7", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7)),
              timestampedValueInGlobalWindow("2:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0)),
              timestampedValueInGlobalWindow("2:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1))));
      assertTrue(splitListener.getPrimaryRoots().isEmpty());
      assertTrue(splitListener.getResidualRoots().isEmpty());
      mainOutputValues.clear();
    }

    {
      // Setup and launch the trySplit thread.
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<HandlesSplits.SplitResult> trySplitFuture =
          executorService.submit(
              () -> {
                try {
                  doFn.waitForSplitElementToBeProcessed();
                  // Currently processing "3" out of range [0, 5) elements.
                  assertEquals(0.6, ((HandlesSplits) mainInput).getProgress(), 0.01);

                  // Check that during progressing of an element we report progress
                  List<MonitoringInfo> mis =
                      Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos();
                  MonitoringInfo.Builder expectedCompleted = MonitoringInfo.newBuilder();
                  expectedCompleted.setUrn(MonitoringInfoConstants.Urns.WORK_COMPLETED);
                  expectedCompleted.setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE);
                  expectedCompleted.putLabels(
                      MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
                  expectedCompleted.setPayload(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(
                              IterableCoder.of(DoubleCoder.of()), Collections.singletonList(3.0))));
                  MonitoringInfo.Builder expectedRemaining = MonitoringInfo.newBuilder();
                  expectedRemaining.setUrn(MonitoringInfoConstants.Urns.WORK_REMAINING);
                  expectedRemaining.setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE);
                  expectedRemaining.putLabels(
                      MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
                  expectedRemaining.setPayload(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(
                              IterableCoder.of(DoubleCoder.of()), Collections.singletonList(2.0))));
                  assertThat(
                      mis,
                      containsInAnyOrder(expectedCompleted.build(), expectedRemaining.build()));

                  return ((HandlesSplits) mainInput).trySplit(0);
                } finally {
                  doFn.releaseWaitingProcessElementThread();
                }
              });

      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  2.0)));
      HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      // Since the SPLIT_ELEMENT is 3 we will process 0, 1, 2, 3 then be split.
      // We expect that the watermark advances to MIN + 2 since the manual watermark estimator
      // has yet to be invoked for the split element and that the primary represents [0, 4) with
      // the original watermark while the residual represents [4, 5) with the new MIN + 2 watermark.
      assertThat(
          mainOutputValues,
          contains(
              timestampedValueInGlobalWindow("7:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0)),
              timestampedValueInGlobalWindow("7:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1)),
              timestampedValueInGlobalWindow("7:2", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2)),
              timestampedValueInGlobalWindow("7:3", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(3))));

      BundleApplication primaryRoot = Iterables.getOnlyElement(trySplitResult.getPrimaryRoots());
      DelayedBundleApplication residualRoot =
          Iterables.getOnlyElement(trySplitResult.getResidualRoots());
      assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
      assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(
          ParDoTranslation.getMainInputName(pTransform),
          residualRoot.getApplication().getInputId());
      assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      assertEquals(
          valueInGlobalWindow(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 4), GlobalWindow.TIMESTAMP_MIN_VALUE)), 4.0)),
          inputCoder.decode(primaryRoot.getElement().newInput()));
      assertEquals(
          valueInGlobalWindow(
              KV.of(
                  KV.of(
                      "7", KV.of(new OffsetRange(4, 5), GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2))),
                  1.0)),
          inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
      Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2);
      assertEquals(
          ImmutableMap.of(
              "output",
              org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                  .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                  .build()),
          residualRoot.getApplication().getOutputWatermarksMap());
      // We expect 0 resume delay.
      assertEquals(
          residualRoot.getRequestedTimeDelay().getDefaultInstanceForType(),
          residualRoot.getRequestedTimeDelay());
      // We don't expect the outputs to goto the SDK initiated checkpointing listener.
      assertTrue(splitListener.getPrimaryRoots().isEmpty());
      assertTrue(splitListener.getResidualRoots().isEmpty());
      mainOutputValues.clear();
      executorService.shutdown();
    }

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  @Test
  public void testProcessElementForWindowedSizedElementAndRestriction() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    WindowObservingTestSplittableDoFn doFn =
        new WindowObservingTestSplittableDoFn(singletonSideInputView);

    valuePCollection
        .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
        .apply(TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

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
                                PTransformTranslation
                                    .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
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
        WindowedValue.getFullCoder(
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
    String outputPCollectionId = pTransform.getOutputsOrThrow("output");

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            iterableSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("8"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();
    List<ProgressRequestCallback> progressRequestCallbacks = new ArrayList<>();
    BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            progressRequestCallbacks::add,
            splitListener,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    BoundedWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    BoundedWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    {
      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              window1,
              window2);
      mainInput.accept(firstValue);
      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      // Since the side input upperBound is 8 we will process 5, 6, and 7 then checkpoint.
      // We expect that the watermark advances to MIN + 7 and that the primary represents [5, 8)
      // with the original watermark while the residual represents [8, 10) with the new MIN + 7
      // watermark.
      //
      // Since we were on the first window, we expect only a single primary root and two residual
      // roots (the split + the unprocessed window).
      BundleApplication primaryRoot = Iterables.getOnlyElement(splitListener.getPrimaryRoots());
      assertEquals(2, splitListener.getResidualRoots().size());
      DelayedBundleApplication residualRoot = splitListener.getResidualRoots().get(1);
      DelayedBundleApplication residualRootForUnprocessedWindows =
          splitListener.getResidualRoots().get(0);
      assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
      assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(
          ParDoTranslation.getMainInputName(pTransform),
          residualRoot.getApplication().getInputId());
      assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7);
      assertEquals(
          ImmutableMap.of(
              "output",
              org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                  .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                  .build()),
          residualRoot.getApplication().getOutputWatermarksMap());
      assertEquals(
          org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Duration.newBuilder()
              .setSeconds(54)
              .setNanos(321000000)
              .build(),
          residualRoot.getRequestedTimeDelay());
      assertEquals(
          ParDoTranslation.getMainInputName(pTransform),
          residualRootForUnprocessedWindows.getApplication().getInputId());
      assertEquals(
          TEST_TRANSFORM_ID, residualRootForUnprocessedWindows.getApplication().getTransformId());
      assertEquals(
          residualRootForUnprocessedWindows.getRequestedTimeDelay().getDefaultInstanceForType(),
          residualRootForUnprocessedWindows.getRequestedTimeDelay());
      assertTrue(
          residualRootForUnprocessedWindows.getApplication().getOutputWatermarksMap().isEmpty());

      assertEquals(
          decode(inputCoder, primaryRoot.getElement()),
          WindowedValue.of(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(5, 8), GlobalWindow.TIMESTAMP_MIN_VALUE)), 3.0),
              firstValue.getTimestamp(),
              window1,
              firstValue.getPane()));
      assertEquals(
          decode(inputCoder, residualRoot.getApplication().getElement()),
          WindowedValue.of(
              KV.of(
                  KV.of(
                      "5", KV.of(new OffsetRange(8, 10), GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7))),
                  2.0),
              firstValue.getTimestamp(),
              window1,
              firstValue.getPane()));
      assertEquals(
          decode(inputCoder, residualRootForUnprocessedWindows.getApplication().getElement()),
          WindowedValue.of(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              firstValue.getTimestamp(),
              window2,
              firstValue.getPane()));
      splitListener.clear();

      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(secondValue);
      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  "5:5", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(5), window1, firstValue.getPane()),
              WindowedValue.of(
                  "5:6", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(6), window1, firstValue.getPane()),
              WindowedValue.of(
                  "5:7", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(7), window1, firstValue.getPane()),
              WindowedValue.of(
                  "2:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0), window1, firstValue.getPane()),
              WindowedValue.of(
                  "2:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1), window1, firstValue.getPane()),
              WindowedValue.of(
                  "2:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0), window2, firstValue.getPane()),
              WindowedValue.of(
                  "2:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1), window2, firstValue.getPane())));
      assertTrue(splitListener.getPrimaryRoots().isEmpty());
      assertTrue(splitListener.getResidualRoots().isEmpty());
      mainOutputValues.clear();
    }

    {
      // Setup and launch the trySplit thread.
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<HandlesSplits.SplitResult> trySplitFuture =
          executorService.submit(
              () -> {
                try {
                  doFn.waitForSplitElementToBeProcessed();
                  // Currently processing "3" out of range [0, 5) elements.
                  assertEquals(0.6, ((HandlesSplits) mainInput).getProgress(), 0.01);

                  // Check that during progressing of an element we report progress
                  List<MonitoringInfo> mis =
                      Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos();
                  MonitoringInfo.Builder expectedCompleted = MonitoringInfo.newBuilder();
                  expectedCompleted.setUrn(MonitoringInfoConstants.Urns.WORK_COMPLETED);
                  expectedCompleted.setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE);
                  expectedCompleted.putLabels(
                      MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
                  expectedCompleted.setPayload(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(
                              IterableCoder.of(DoubleCoder.of()), Collections.singletonList(3.0))));
                  MonitoringInfo.Builder expectedRemaining = MonitoringInfo.newBuilder();
                  expectedRemaining.setUrn(MonitoringInfoConstants.Urns.WORK_REMAINING);
                  expectedRemaining.setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE);
                  expectedRemaining.putLabels(
                      MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
                  expectedRemaining.setPayload(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(
                              IterableCoder.of(DoubleCoder.of()), Collections.singletonList(2.0))));
                  assertThat(
                      mis,
                      containsInAnyOrder(expectedCompleted.build(), expectedRemaining.build()));

                  return ((HandlesSplits) mainInput).trySplit(0);
                } finally {
                  doFn.releaseWaitingProcessElementThread();
                }
              });

      // Check that before processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());
      WindowedValue<?> splitValue =
          valueInWindows(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(splitValue);
      HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

      // Check that after processing an element we don't report progress
      assertThat(Iterables.getOnlyElement(progressRequestCallbacks).getMonitoringInfos(), empty());

      // Since the SPLIT_ELEMENT is 3 we will process 0, 1, 2, 3 then be split on the first window.
      // We expect that the watermark advances to MIN + 2 since the manual watermark estimator
      // has yet to be invoked for the split element and that the primary represents [0, 4) with
      // the original watermark while the residual represents [4, 5) with the new MIN + 2 watermark.
      //
      // We expect to see none of the output for the second window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  "7:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0), window1, splitValue.getPane()),
              WindowedValue.of(
                  "7:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1), window1, splitValue.getPane()),
              WindowedValue.of(
                  "7:2", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2), window1, splitValue.getPane()),
              WindowedValue.of(
                  "7:3", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(3), window1, splitValue.getPane())));

      BundleApplication primaryRoot = Iterables.getOnlyElement(trySplitResult.getPrimaryRoots());
      assertEquals(2, trySplitResult.getResidualRoots().size());
      DelayedBundleApplication residualRoot = trySplitResult.getResidualRoots().get(1);
      DelayedBundleApplication residualRootInUnprocessedWindows =
          trySplitResult.getResidualRoots().get(0);
      assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
      assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(
          ParDoTranslation.getMainInputName(pTransform),
          residualRoot.getApplication().getInputId());
      assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      assertEquals(
          TEST_TRANSFORM_ID, residualRootInUnprocessedWindows.getApplication().getTransformId());
      assertEquals(
          residualRootInUnprocessedWindows.getRequestedTimeDelay().getDefaultInstanceForType(),
          residualRootInUnprocessedWindows.getRequestedTimeDelay());
      assertTrue(
          residualRootInUnprocessedWindows.getApplication().getOutputWatermarksMap().isEmpty());
      assertEquals(
          valueInWindows(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 4), GlobalWindow.TIMESTAMP_MIN_VALUE)), 4.0),
              window1),
          inputCoder.decode(primaryRoot.getElement().newInput()));
      assertEquals(
          valueInWindows(
              KV.of(
                  KV.of(
                      "7", KV.of(new OffsetRange(4, 5), GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2))),
                  1.0),
              window1),
          inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
      Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2);
      assertEquals(
          ImmutableMap.of(
              "output",
              org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                  .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                  .build()),
          residualRoot.getApplication().getOutputWatermarksMap());
      assertEquals(
          WindowedValue.of(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              splitValue.getTimestamp(),
              window2,
              splitValue.getPane()),
          inputCoder.decode(
              residualRootInUnprocessedWindows.getApplication().getElement().newInput()));

      // We expect 0 resume delay.
      assertEquals(
          residualRoot.getRequestedTimeDelay().getDefaultInstanceForType(),
          residualRoot.getRequestedTimeDelay());
      // We don't expect the outputs to goto the SDK initiated checkpointing listener.
      assertTrue(splitListener.getPrimaryRoots().isEmpty());
      assertTrue(splitListener.getResidualRoots().isEmpty());
      mainOutputValues.clear();
      executorService.shutdown();
    }

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
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

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow("5"));
    mainInput.accept(valueInGlobalWindow("2"));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE))),
            valueInGlobalWindow(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)))));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
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

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    WindowedValue<?> firstValue = valueInWindows("5", window1, window2);
    WindowedValue<?> secondValue = valueInWindows("2", window1, window2);
    mainInput.accept(firstValue);
    mainInput.accept(secondValue);
    assertThat(
        mainOutputValues,
        contains(
            WindowedValue.of(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                secondValue.getTimestamp(),
                window1,
                secondValue.getPane()),
            WindowedValue.of(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                secondValue.getTimestamp(),
                window2,
                secondValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  @Test
  public void testProcessElementForWindowedPairWithRestrictionWithNonWindowObservingOptimization()
      throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
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

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
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
            WindowedValue.of(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane()),
            WindowedValue.of(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                secondValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                secondValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
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

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
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

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
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

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
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
    assertThat(
        mainOutputValues,
        contains(
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  @Test
  public void
      testProcessElementForWindowedSplitAndSizeRestrictionWithNonWindowObservingOptimization()
          throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
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

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
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
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  private static SplitResult createSplitResult(double fractionOfRemainder) {
    ByteString.Output primaryBytes = ByteString.newOutput();
    ByteString.Output residualBytes = ByteString.newOutput();
    try {
      DoubleCoder.of().encode(fractionOfRemainder, primaryBytes);
      DoubleCoder.of().encode(1 - fractionOfRemainder, residualBytes);
    } catch (Exception e) {
      // No-op.
    }
    return SplitResult.of(
        ImmutableList.of(
            BundleApplication.newBuilder().setElement(primaryBytes.toByteString()).build()),
        ImmutableList.of(
            DelayedBundleApplication.newBuilder()
                .setApplication(
                    BundleApplication.newBuilder().setElement(residualBytes.toByteString()).build())
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
  public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWhenObservingWindow()
      throws Exception {
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
            SplittableParDoExpander.createTruncateReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    assertEquals(0, ((HandlesSplits) mainInput).getProgress(), 0.0);
    assertNull(((HandlesSplits) mainInput).trySplit(0.4));
  }

  @Test
  public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWhenoutObservingWindow()
      throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

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
                            .equals(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    assertEquals(0.7, ((HandlesSplits) mainInput).getProgress(), 0.0);
    assertEquals(createSplitResult(0.4), ((HandlesSplits) mainInput).trySplit(0.4));
  }

  @Test
  public void testProcessElementForTruncateAndSizeRestriction() throws Exception {
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
            SplittableParDoExpander.createTruncateReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    mainInput.accept(
        valueInGlobalWindow(
            KV.of(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0)));
    mainInput.accept(
        valueInGlobalWindow(
            KV.of(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0)));
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

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
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
                            .equals(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    WindowedValue<?> firstValue =
        valueInWindows(
            KV.of(KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
            window1,
            window2);
    WindowedValue<?> secondValue =
        valueInWindows(
            KV.of(KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
            window1,
            window2);
    mainInput.accept(firstValue);
    mainInput.accept(secondValue);
    assertThat(
        mainOutputValues,
        contains(
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  @Test
  public void
      testProcessElementForWindowedTruncateAndSizeRestrictionWithNonWindowObservingOptimization()
          throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
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
                            .equals(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    assertTrue(startFunctionRegistry.getFunctions().isEmpty());
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    assertThat(mainInput, instanceOf(HandlesSplits.class));

    IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
    WindowedValue<?> firstValue =
        valueInWindows(
            KV.of(KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
            window1,
            window2);
    WindowedValue<?> secondValue =
        valueInWindows(
            KV.of(KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
            window1,
            window2);
    mainInput.accept(firstValue);
    mainInput.accept(secondValue);
    // Ensure that each output element is in all the windows and not one per window.
    assertThat(
        mainOutputValues,
        contains(
            WindowedValue.of(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane()),
            WindowedValue.of(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0),
                firstValue.getTimestamp(),
                ImmutableList.of(window1, window2),
                firstValue.getPane())));
    mainOutputValues.clear();

    assertTrue(finishFunctionRegistry.getFunctions().isEmpty());
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }
}
