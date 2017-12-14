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
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.hamcrest.collection.IsMapContaining;
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
    String mainOutputId = "101";
    String additionalOutputId = "102";

    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        new TestDoFn(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(),
        StringUtf8Coder.of(),
        Long.parseLong(mainOutputId),
        ImmutableMap.of(
            Long.parseLong(mainOutputId), TestDoFn.mainOutput,
            Long.parseLong(additionalOutputId), TestDoFn.additionalOutput));
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build();
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("inputA", "inputATarget")
        .putInputs("inputB", "inputBTarget")
        .putOutputs(mainOutputId, "mainOutputTarget")
        .putOutputs(additionalOutputId, "additionalOutputTarget")
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
    Iterables.getOnlyElement(consumers.get("inputATarget")).accept(valueInGlobalWindow("B"));
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
    String mainOutputId = "101";

    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        new TestStatefulDoFn(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(),
        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
        Long.parseLong(mainOutputId),
        ImmutableMap.of(Long.parseLong(mainOutputId), new TupleTag<String>("mainOutput")));
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build();
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("input", "inputTarget")
        .putOutputs(mainOutputId, "mainOutputTarget")
        .build();

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of(
        key("value", "X"), encode("X0"),
        key("bag", "X"), encode("X0"),
        key("combine", "X"), encode("X0"),
        key("combineWithContext", "X"), encode("X0")
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
            .put(key("value", "X"), encode("X2"))
            .put(key("bag", "X"), encode("X0", "X1", "X2"))
            .put(key("combine", "X"), encode("X0X1X2"))
            .put(key("combineWithContext", "X"), encode("X0X1X2"))
            .put(key("value", "Y"), encode("Y2"))
            .put(key("bag", "Y"), encode("Y1", "Y2"))
            .put(key("combine", "Y"), encode("Y1Y2"))
            .put(key("combineWithContext", "Y"), encode("Y1Y2"))
            .build(),
        fakeClient.getData());
    mainOutputValues.clear();
  }

  /** Produces a {@link StateKey} for the test PTransform id in the Global Window. */
  private StateKey key(String userStateId, String key) throws IOException {
    return StateKey.newBuilder().setBagUserState(
        StateKey.BagUserState.newBuilder()
            .setPtransformId(TEST_PTRANSFORM_ID)
            .setUserStateId(userStateId)
            .setKey(encode(key))
            .setWindow(ByteString.copyFrom(
                CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE))))
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
