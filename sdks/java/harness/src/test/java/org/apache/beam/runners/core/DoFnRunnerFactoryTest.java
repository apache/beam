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

package org.apache.beam.runners.core;

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.runners.core.PTransformRunnerFactory.Registrar;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnRunnerFactory}. */
@RunWith(JUnit4.class)
public class DoFnRunnerFactoryTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Coder<WindowedValue<String>> STRING_CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final String STRING_CODER_SPEC_ID = "999L";
  private static final RunnerApi.Coder STRING_CODER_SPEC;
  private static final String URN = "urn:org.apache.beam:dofn:java:0.1";

  static {
    try {
      STRING_CODER_SPEC = RunnerApi.Coder.newBuilder()
          .setSpec(RunnerApi.SdkFunctionSpec.newBuilder()
              .setSpec(RunnerApi.FunctionSpec.newBuilder()
                  .setParameter(Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
                      OBJECT_MAPPER.writeValueAsBytes(CloudObjects.asCloudObject(STRING_CODER))))
                      .build())))
              .build())
          .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

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
    Map<String, Message> fnApiRegistry = ImmutableMap.of(STRING_CODER_SPEC_ID, STRING_CODER_SPEC);
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
    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn("urn:org.apache.beam:dofn:java:0.1")
        .setParameter(Any.pack(BytesValue.newBuilder()
            .setValue(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build()))
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
    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("mainOutputTarget",
        (ThrowingConsumer) (ThrowingConsumer<WindowedValue<String>>) mainOutputValues::add);
    consumers.put("additionalOutputTarget",
        (ThrowingConsumer) (ThrowingConsumer<WindowedValue<String>>) additionalOutputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new DoFnRunnerFactory.Factory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        pTransformId,
        pTransform,
        Suppliers.ofInstance("57L")::get,
        ImmutableMap.of(),
        ImmutableMap.of(),
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

  @Test
  public void testRegistration() {
    for (Registrar registrar :
        ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof DoFnRunnerFactory.Registrar) {
        assertThat(registrar.getPTransformRunnerFactories(), IsMapContaining.hasKey(URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
