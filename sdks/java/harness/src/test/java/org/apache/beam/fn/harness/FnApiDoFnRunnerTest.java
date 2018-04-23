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

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
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
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnApiDoFnRunner}. */
@RunWith(JUnit4.class)
public class FnApiDoFnRunnerTest {

  public static final String TEST_PTRANSFORM_ID = "pTransformId";

  private static class TestDoFn extends DoFn<String, String> {
    private static final TupleTag<String> mainOutput = new TupleTag<>("mainOutput");
    private static final TupleTag<String> additionalOutput = new TupleTag<>("output");

    private BoundedWindow window;

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      context.output("MainOutput" + context.element());
      context.output(additionalOutput, "AdditionalOutput" + context.element());
      this.window = window;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      if (window != null) {
        context.output("FinishBundle", window.maxTimestamp(), window);
        window = null;
      }
    }
  }

  /**
   * Create a DoFn that has 3 inputs (inputATarget1, inputATarget2, inputBTarget) and 2 outputs
   * (mainOutput, output). Validate that inputs are fed to the {@link DoFn} and that outputs
   * are directed to the correct consumers.
   */
  @Test
  public void testCreatingAndProcessingDoFn() throws Exception {
    String pTransformId = "pTransformId";

    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        new TestDoFn(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(),
        StringUtf8Coder.of(),
        TestDoFn.mainOutput);
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build();
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("inputA", "inputATarget")
        .putInputs("inputB", "inputBTarget")
        .putOutputs(TestDoFn.mainOutput.getId(), "mainOutputTarget")
        .putOutputs(TestDoFn.additionalOutput.getId(), "additionalOutputTarget")
        .build();

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("mainOutputTarget",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.put("additionalOutputTarget",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        null /* beamFnStateClient */,
        pTransformId,
        pTransform,
        Suppliers.ofInstance("57L")::get,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    Iterables.getOnlyElement(startFunctions).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(
        "inputATarget", "inputBTarget", "mainOutputTarget", "additionalOutputTarget"));

    Iterables.getOnlyElement(consumers.get("inputATarget")).accept(valueInGlobalWindow("A1"));
    Iterables.getOnlyElement(consumers.get("inputATarget")).accept(valueInGlobalWindow("A2"));
    Iterables.getOnlyElement(consumers.get("inputBTarget")).accept(valueInGlobalWindow("B"));
    assertThat(mainOutputValues, contains(
        valueInGlobalWindow("MainOutputA1"),
        valueInGlobalWindow("MainOutputA2"),
        valueInGlobalWindow("MainOutputB")));
    assertThat(additionalOutputValues, contains(
        valueInGlobalWindow("AdditionalOutputA1"),
        valueInGlobalWindow("AdditionalOutputA2"),
        valueInGlobalWindow("AdditionalOutputB")));
    mainOutputValues.clear();
    additionalOutputValues.clear();

    Iterables.getOnlyElement(finishFunctions).run();
    assertThat(
        mainOutputValues,
        contains(
            timestampedValueInGlobalWindow("FinishBundle", GlobalWindow.INSTANCE.maxTimestamp())));
    mainOutputValues.clear();
  }

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

  private static class ConcatCombineFnWithContext
      extends CombineFnWithContext<String, String, String> {
    @Override
    public String createAccumulator(Context c) {
      return "";
    }

    @Override
    public String addInput(String accumulator, String input, Context c) {
      return accumulator.concat(input);
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumulators, Context c) {
      StringBuilder builder = new StringBuilder();
      for (String value : accumulators) {
        builder.append(value);
      }
      return builder.toString();
    }

    @Override
    public String extractOutput(String accumulator, Context c) {
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
    private final StateSpec<BagState<String>> bagStateSpec =
        StateSpecs.bag(StringUtf8Coder.of());
    @StateId("combine")
    private final StateSpec<CombiningState<String, String, String>> combiningStateSpec =
        StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFn());
    @StateId("combineWithContext")
    private final StateSpec<CombiningState<String, String, String>> combiningWithContextStateSpec =
        StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFnWithContext());

    @ProcessElement
    public void processElement(ProcessContext context,
        @StateId("value") ValueState<String> valueState,
        @StateId("bag") BagState<String> bagState,
        @StateId("combine") CombiningState<String, String, String> combiningState,
        @StateId("combineWithContext")
            CombiningState<String, String, String> combiningWithContextState) {
      context.output("value:" + valueState.read());
      valueState.write(context.element().getValue());

      context.output("bag:" + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());

      context.output("combine:" + combiningState.read());
      combiningState.add(context.element().getValue());

      context.output("combineWithContext:" + combiningWithContextState.read());
      combiningWithContextState.add(context.element().getValue());
    }
  }

  @Test
  public void testUsingUserState() throws Exception {
    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        new TestStatefulDoFn(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(),
        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
        new TupleTag<>("mainOutput"));
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build();
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("input", "inputTarget")
        .putOutputs("mainOutput", "mainOutputTarget")
        .build();

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of(
        bagUserStateKey("value", "X"), encode("X0"),
        bagUserStateKey("bag", "X"), encode("X0"),
        bagUserStateKey("combine", "X"), encode("X0"),
        bagUserStateKey("combineWithContext", "X"), encode("X0")
    ));

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("mainOutputTarget",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        fakeClient,
        TEST_PTRANSFORM_ID,
        pTransform,
        Suppliers.ofInstance("57L")::get,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    Iterables.getOnlyElement(startFunctions).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder("inputTarget", "mainOutputTarget"));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        Iterables.getOnlyElement(consumers.get("inputTarget"));
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X1")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y1")));
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X2")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y2")));
    assertThat(mainOutputValues, contains(
        valueInGlobalWindow("value:X0"),
        valueInGlobalWindow("bag:[X0]"),
        valueInGlobalWindow("combine:X0"),
        valueInGlobalWindow("combineWithContext:X0"),
        valueInGlobalWindow("value:null"),
        valueInGlobalWindow("bag:[]"),
        valueInGlobalWindow("combine:"),
        valueInGlobalWindow("combineWithContext:"),
        valueInGlobalWindow("value:X1"),
        valueInGlobalWindow("bag:[X0, X1]"),
        valueInGlobalWindow("combine:X0X1"),
        valueInGlobalWindow("combineWithContext:X0X1"),
        valueInGlobalWindow("value:Y1"),
        valueInGlobalWindow("bag:[Y1]"),
        valueInGlobalWindow("combine:Y1"),
        valueInGlobalWindow("combineWithContext:Y1")));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctions).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("value", "X"), encode("X2"))
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2"))
            .put(bagUserStateKey("combine", "X"), encode("X0X1X2"))
            .put(bagUserStateKey("combineWithContext", "X"), encode("X0X1X2"))
            .put(bagUserStateKey("value", "Y"), encode("Y2"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2"))
            .put(bagUserStateKey("combine", "Y"), encode("Y1Y2"))
            .put(bagUserStateKey("combineWithContext", "Y"), encode("Y1Y2"))
            .build(),
        fakeClient.getData());
    mainOutputValues.clear();
  }

  /** Produces a bag user {@link StateKey} for the test PTransform id in the global window. */
  private StateKey bagUserStateKey(String userStateId, String key) throws IOException {
    return StateKey.newBuilder().setBagUserState(
        StateKey.BagUserState.newBuilder()
            .setPtransformId(TEST_PTRANSFORM_ID)
            .setUserStateId(userStateId)
            .setKey(encode(key))
            .setWindow(ByteString.copyFrom(
                CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE))))
        .build();
  }

  private static class TestSideInputDoFn extends DoFn<String, String> {
    private final PCollectionView<String> defaultSingletonSideInput;
    private final PCollectionView<String> singletonSideInput;
    private final PCollectionView<Iterable<String>> iterableSideInput;
    private TestSideInputDoFn(
        PCollectionView<String> defaultSingletonSideInput,
        PCollectionView<String> singletonSideInput,
        PCollectionView<Iterable<String>> iterableSideInput) {
      this.defaultSingletonSideInput = defaultSingletonSideInput;
      this.singletonSideInput = singletonSideInput;
      this.iterableSideInput = iterableSideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element() + ":" + context.sideInput(defaultSingletonSideInput));
      context.output(context.element() + ":" + context.sideInput(singletonSideInput));
      for (String sideInputValue : context.sideInput(iterableSideInput)) {
        context.output(context.element() + ":" + sideInputValue);
      }
    }
  }

  @Test
  public void testUsingSideInput() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> defaultSingletonSideInputView = valuePCollection.apply(
        View.<String>asSingleton().withDefaultValue("defaultSingletonValue"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<String> outputPCollection = valuePCollection.apply(TEST_PTRANSFORM_ID, ParDo.of(
        new TestSideInputDoFn(
            defaultSingletonSideInputView,
            singletonSideInputView,
            iterableSideInputView))
        .withSideInputs(
            defaultSingletonSideInputView, singletonSideInputView, iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform = pProto.getComponents().getTransformsOrThrow(
        pProto.getComponents().getTransformsOrThrow(TEST_PTRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData = ImmutableMap.of(
        multimapSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
        encode("singletonValue"),
        multimapSideInputKey(iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY),
        encode("iterableValue1", "iterableValue2", "iterableValue3"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put(Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new FnApiDoFnRunner.NewFactory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        fakeClient,
        TEST_PTRANSFORM_ID,
        pTransform,
        Suppliers.ofInstance("57L")::get,
        pProto.getComponents().getPcollectionsMap(),
        pProto.getComponents().getCodersMap(),
        pProto.getComponents().getWindowingStrategiesMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    Iterables.getOnlyElement(startFunctions).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        Iterables.getOnlyElement(consumers.get(inputPCollectionId));
    mainInput.accept(valueInGlobalWindow("X"));
    mainInput.accept(valueInGlobalWindow("Y"));
    assertThat(mainOutputValues, contains(
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
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
    mainOutputValues.clear();
  }

  private static class TestSideInputIsAccessibleForDownstreamCallersDoFn
      extends DoFn<String, Iterable<String>> {
    private final PCollectionView<Iterable<String>> iterableSideInput;
    private TestSideInputIsAccessibleForDownstreamCallersDoFn(
        PCollectionView<Iterable<String>> iterableSideInput) {
      this.iterableSideInput = iterableSideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
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
    PCollection<String> valuePCollection = p.apply(Create.of("unused"))
        .apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(TEST_PTRANSFORM_ID, ParDo.of(
            new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
            .withSideInputs(iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform = pProto.getComponents().getTransformsOrThrow(
        pProto.getComponents().getTransformsOrThrow(TEST_PTRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData = ImmutableMap.of(
        multimapSideInputKey(
            iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowA),
        encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
        multimapSideInputKey(
            iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowB),
        encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put(Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new FnApiDoFnRunner.NewFactory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        fakeClient,
        TEST_PTRANSFORM_ID,
        pTransform,
        Suppliers.ofInstance("57L")::get,
        pProto.getComponents().getPcollectionsMap(),
        pProto.getComponents().getCodersMap(),
        pProto.getComponents().getWindowingStrategiesMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    Iterables.getOnlyElement(startFunctions).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        Iterables.getOnlyElement(consumers.get(inputPCollectionId));
    mainInput.accept(valueInWindow("X", windowA));
    mainInput.accept(valueInWindow("Y", windowB));
    assertThat(mainOutputValues, hasSize(2));
    assertThat(mainOutputValues.get(0).getValue(), contains(
        "iterableValue1A", "iterableValue2A", "iterableValue3A"));
    assertThat(mainOutputValues.get(1).getValue(), contains(
        "iterableValue1B", "iterableValue2B", "iterableValue3B"));

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  private <T> WindowedValue<T> valueInWindow(T value, BoundedWindow window) {
    return WindowedValue.of(value, window.maxTimestamp(), window, PaneInfo.ON_TIME_AND_ONLY_FIRING);
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the global
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key) throws IOException {
    return multimapSideInputKey(sideInputId, key, ByteString.copyFrom(
        CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)));
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the supplied
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key, ByteString windowKey) {
    return StateKey.newBuilder().setMultimapSideInput(
        StateKey.MultimapSideInput.newBuilder()
            .setPtransformId(TEST_PTRANSFORM_ID)
            .setSideInputId(sideInputId)
            .setKey(key)
            .setWindow(windowKey))
        .build();
  }

  private ByteString encode(String ... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  @Test
  public void testRegistration() {
    for (Registrar registrar :
        ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof FnApiDoFnRunner.Registrar) {
        assertThat(registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
