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

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMatchers;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
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

  public static final String TEST_PTRANSFORM_ID = "pTransformId";

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
        valuePCollection.apply(TEST_PTRANSFORM_ID, ParDo.of(new TestStatefulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto
                    .getComponents()
                    .getTransformsOrThrow(TEST_PTRANSFORM_ID)
                    .getSubtransforms(0));

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("value", "X"), encode("X0"),
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("combine", "X"), encode("X0")));

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forPipelineProto(pProto).withPipeline(Pipeline.create());
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), rehydratedComponents);
    consumers.register(
        outputPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            TEST_PTRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            rehydratedComponents,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* splitListener */);

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
    mainOutputValues.clear();
  }

  /** Produces a bag user {@link StateKey} for the test PTransform id in the global window. */
  private StateKey bagUserStateKey(String userStateId, String key) throws IOException {
    return StateKey.newBuilder()
        .setBagUserState(
            StateKey.BagUserState.newBuilder()
                .setPtransformId(TEST_PTRANSFORM_ID)
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
  public void testBasicWithSideInputsAndOutputs() throws Exception {
    Pipeline p = Pipeline.create();
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
            TEST_PTRANSFORM_ID,
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
        pProto.getComponents().getTransformsOrThrow(TEST_PTRANSFORM_ID);

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("singletonValue"),
            multimapSideInputKey(iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("iterableValue1", "iterableValue2", "iterableValue3"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forPipelineProto(pProto).withPipeline(Pipeline.create());

    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), rehydratedComponents);
    consumers.register(
        outputPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.register(
        additionalPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            TEST_PTRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            rehydratedComponents,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* splitListener */);

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

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
    mainOutputValues.clear();
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
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused")).apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(
            TEST_PTRANSFORM_ID,
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
                pProto
                    .getComponents()
                    .getTransformsOrThrow(TEST_PTRANSFORM_ID)
                    .getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forPipelineProto(pProto).withPipeline(Pipeline.create());
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), rehydratedComponents);
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            TEST_PTRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            rehydratedComponents,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* splitListener */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindow("X", windowA));
    mainInput.accept(valueInWindow("Y", windowB));
    assertThat(mainOutputValues, hasSize(2));
    assertThat(
        mainOutputValues.get(0).getValue(),
        contains("iterableValue1A", "iterableValue2A", "iterableValue3A"));
    assertThat(
        mainOutputValues.get(1).getValue(),
        contains("iterableValue1B", "iterableValue2B", "iterableValue3B"));

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
            TEST_PTRANSFORM_ID,
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
                pProto
                    .getComponents()
                    .getTransformsOrThrow(TEST_PTRANSFORM_ID)
                    .getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forPipelineProto(pProto).withPipeline(Pipeline.create());
    ;
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), rehydratedComponents);
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            TEST_PTRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            rehydratedComponents,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* splitListener */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindow("X", windowA));
    mainInput.accept(valueInWindow("Y", windowB));

    MetricsContainer mc = MetricsEnvironment.getCurrentContainer();

    List<Matcher<MonitoringInfo>> matchers = new ArrayList<Matcher<MonitoringInfo>>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "Window.Into()/Window.Assign.out");
    builder.setInt64Value(2);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(
        MonitoringInfoConstants.Labels.PCOLLECTION,
        "pTransformId/ParMultiDo(TestSideInputIsAccessibleForDownstreamCallers).output");
    builder.setInt64Value(2);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    builder = new SimpleMonitoringInfoBuilder();
    builder
        .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
        .setLabel(
            MonitoringInfoConstants.Labels.NAMESPACE,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.class.getName())
        .setLabel(
            MonitoringInfoConstants.Labels.NAME,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.USER_COUNTER_NAME);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_PTRANSFORM_ID);
    builder.setInt64Value(2);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    closeable.close();
    List<MonitoringInfo> result = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : metricsContainerRegistry.getMonitoringInfos()) {
      result.add(SimpleMonitoringInfoBuilder.copyAndClearTimestamp(mi));
    }

    for (Matcher<MonitoringInfo> matcher : matchers) {
      assertThat(metricsContainerRegistry.getMonitoringInfos(), Matchers.hasItem(matcher));
    }
  }

  private static class TestTimerfulDoFn extends DoFn<KV<String, String>, String> {
    @StateId("bag")
    private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

    @TimerId("event")
    private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("processing")
    private final TimerSpec processingTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("main" + context.element().getKey() + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());
      eventTimeTimer.set(context.timestamp().plus(1L));
      processingTimeTimer.offset(Duration.millis(2L));
      processingTimeTimer.setRelative();
    }

    @OnTimer("event")
    public void eventTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("event" + Iterables.toString(bagState.read()));
      bagState.add("event");
      eventTimeTimer.set(context.timestamp().plus(11L));
      processingTimeTimer.offset(Duration.millis(12L));
      processingTimeTimer.setRelative();
    }

    @OnTimer("processing")
    public void processingTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("processing" + Iterables.toString(bagState.read()));
      bagState.add("processing");
      eventTimeTimer.set(context.timestamp().plus(21L));
      processingTimeTimer.offset(Duration.millis(22L));
      processingTimeTimer.setRelative();
    }
  }

  @Test
  public void testTimers() throws Exception {
    dateTimeProvider.setDateTimeFixed(10000L);

    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> valuePCollection =
        p.apply(Create.of(KV.of("unused", "unused")));
    PCollection<String> outputPCollection =
        valuePCollection.apply(TEST_PTRANSFORM_ID, ParDo.of(new TestTimerfulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environment.getDefaultInstance());
    // Note that the pipeline translation for timers creates a loop between the ParDo with
    // the timer and the PCollection for that timer. This loop is unrolled by runners
    // during execution which we redo here manually.
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
    String eventTimerInputPCollectionId = "pTransformId/ParMultiDo(TestTimerful).event";
    String eventTimerOutputPCollectionId = "pTransformId/ParMultiDo(TestTimerful).event.output";
    String processingTimerInputPCollectionId = "pTransformId/ParMultiDo(TestTimerful).processing";
    String processingTimerOutputPCollectionId =
        "pTransformId/ParMultiDo(TestTimerful).processing.output";

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_PTRANSFORM_ID).getSubtransforms(0))
            .toBuilder()
            // We need to re-write the "output" PCollections that a runner would have inserted
            // on the way to a output sink.
            .putOutputs("event", eventTimerOutputPCollectionId)
            .putOutputs("processing", processingTimerOutputPCollectionId)
            .build();

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("bag", "A"), encode("A0"),
                bagUserStateKey("bag", "C"), encode("C0")));

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<KV<String, Timer>>> eventTimerOutputValues = new ArrayList<>();
    List<WindowedValue<KV<String, Timer>>> processingTimerOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();

    Map<String, RunnerApi.PCollection> pCollections =
        ImmutableMap.<String, RunnerApi.PCollection>builder()
            .putAll(pProto.getComponents().getPcollectionsMap())
            // We need to insert the "output" PCollections that a runner would have inserted
            // on the way to a output sink.
            .put(
                eventTimerOutputPCollectionId,
                pProto.getComponents().getPcollectionsOrThrow(eventTimerInputPCollectionId))
            .put(
                processingTimerOutputPCollectionId,
                pProto.getComponents().getPcollectionsOrThrow(processingTimerInputPCollectionId))
            .build();
    Map<String, RunnerApi.Coder> coders = pProto.getComponents().getCodersMap();
    Map<String, RunnerApi.WindowingStrategy> windowingStrategies =
        pProto.getComponents().getWindowingStrategiesMap();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(
                RunnerApi.Components.newBuilder()
                    .putAllCoders(coders)
                    .putAllPcollections(pCollections)
                    .putAllWindowingStrategies(windowingStrategies)
                    .build())
            .withPipeline((p));

    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), rehydratedComponents);
    consumers.register(
        outputPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.register(
        eventTimerOutputPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Timer>>>) eventTimerOutputValues::add);
    consumers.register(
        processingTimerOutputPCollectionId,
        TEST_PTRANSFORM_ID,
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Timer>>>) processingTimerOutputValues::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            TEST_PTRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            rehydratedComponents,
            pCollections,
            coders,
            windowingStrategies,
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* splitListener */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(
        consumers.keySet(),
        containsInAnyOrder(
            inputPCollectionId,
            outputPCollectionId,
            eventTimerInputPCollectionId,
            eventTimerOutputPCollectionId,
            processingTimerInputPCollectionId,
            processingTimerOutputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    FnDataReceiver<WindowedValue<?>> eventTimerInput =
        consumers.getMultiplexingConsumer(eventTimerInputPCollectionId);
    FnDataReceiver<WindowedValue<?>> processingTimerInput =
        consumers.getMultiplexingConsumer(processingTimerInputPCollectionId);
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X1"), new Instant(1000L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y1"), new Instant(1100L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X2"), new Instant(1200L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y2"), new Instant(1300L)));
    eventTimerInput.accept(timerInGlobalWindow("A", new Instant(1400L), new Instant(2400L)));
    eventTimerInput.accept(timerInGlobalWindow("B", new Instant(1500L), new Instant(2500L)));
    eventTimerInput.accept(timerInGlobalWindow("A", new Instant(1600L), new Instant(2600L)));
    processingTimerInput.accept(timerInGlobalWindow("X", new Instant(1700L), new Instant(2700L)));
    processingTimerInput.accept(timerInGlobalWindow("C", new Instant(1800L), new Instant(2800L)));
    processingTimerInput.accept(timerInGlobalWindow("B", new Instant(1900L), new Instant(2900L)));
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
            timestampedValueInGlobalWindow("processing[event]", new Instant(1900L))));
    assertThat(
        eventTimerOutputValues,
        contains(
            timerInGlobalWindow("X", new Instant(1000L), new Instant(1001L)),
            timerInGlobalWindow("Y", new Instant(1100L), new Instant(1101L)),
            timerInGlobalWindow("X", new Instant(1200L), new Instant(1201L)),
            timerInGlobalWindow("Y", new Instant(1300L), new Instant(1301L)),
            timerInGlobalWindow("A", new Instant(1400L), new Instant(1411L)),
            timerInGlobalWindow("B", new Instant(1500L), new Instant(1511L)),
            timerInGlobalWindow("A", new Instant(1600L), new Instant(1611L)),
            timerInGlobalWindow("X", new Instant(1700L), new Instant(1721L)),
            timerInGlobalWindow("C", new Instant(1800L), new Instant(1821L)),
            timerInGlobalWindow("B", new Instant(1900L), new Instant(1921L))));
    assertThat(
        processingTimerOutputValues,
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
            timerInGlobalWindow("B", new Instant(1900L), new Instant(10022L))));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2", "processing"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2"))
            .put(bagUserStateKey("bag", "A"), encode("A0", "event", "event"))
            .put(bagUserStateKey("bag", "B"), encode("event", "processing"))
            .put(bagUserStateKey("bag", "C"), encode("C0", "processing"))
            .build(),
        fakeClient.getData());
    mainOutputValues.clear();
  }

  private <T> WindowedValue<T> valueInWindow(T value, BoundedWindow window) {
    return WindowedValue.of(value, window.maxTimestamp(), window, PaneInfo.ON_TIME_AND_ONLY_FIRING);
  }

  private <T>
      WindowedValue<KV<T, org.apache.beam.runners.core.construction.Timer>> timerInGlobalWindow(
          T value, Instant valueTimestamp, Instant scheduledTimestamp) {
    return timestampedValueInGlobalWindow(
        KV.of(value, org.apache.beam.runners.core.construction.Timer.of(scheduledTimestamp)),
        valueTimestamp);
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the global
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key) throws IOException {
    return multimapSideInputKey(
        sideInputId,
        key,
        ByteString.copyFrom(
            CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)));
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the supplied
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key, ByteString windowKey) {
    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setPtransformId(TEST_PTRANSFORM_ID)
                .setSideInputId(sideInputId)
                .setKey(key)
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
}
