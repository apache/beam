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

package org.apache.beam.fn.harness.control;

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessBundleHandler}. */
@RunWith(JUnit4.class)
public class ProcessBundleHandlerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Coder<WindowedValue<String>> STRING_CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final String LONG_CODER_SPEC_ID = "998L";
  private static final String STRING_CODER_SPEC_ID = "999L";
  private static final BeamFnApi.RemoteGrpcPort REMOTE_PORT = BeamFnApi.RemoteGrpcPort.newBuilder()
      .setApiServiceDescriptor(BeamFnApi.ApiServiceDescriptor.newBuilder()
          .setId("58L")
          .setUrl("TestUrl"))
      .build();
  private static final RunnerApi.Coder LONG_CODER_SPEC;
  private static final RunnerApi.Coder STRING_CODER_SPEC;
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
      LONG_CODER_SPEC = RunnerApi.Coder.newBuilder()
          .setSpec(RunnerApi.SdkFunctionSpec.newBuilder()
              .setSpec(RunnerApi.FunctionSpec.newBuilder()
                  .setParameter(Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
                      OBJECT_MAPPER.writeValueAsBytes(
                          CloudObjects.asCloudObject(WindowedValue.getFullCoder(VarLongCoder.of(),
                              GlobalWindow.Coder.INSTANCE)))))
                      .build())))
              .build())
          .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";
  private static final String DATA_OUTPUT_URN = "urn:org.apache.beam:sink:runner:0.1";
  private static final String JAVA_DO_FN_URN = "urn:org.apache.beam:dofn:java:0.1";
  private static final String JAVA_SOURCE_URN = "urn:org.apache.beam:source:java:0.1";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BeamFnDataClient beamFnDataClient;
  @Captor private ArgumentCaptor<ThrowingConsumer<WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOrderOfStartAndFinishCalls() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .putOutputs("2L-output", "2L-output-pc")
                .build())
            .putTransforms("3L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_OUTPUT_URN).build())
                .putInputs("3L-input", "2L-output-pc")
                .build())
            .putPcollections("2L-output-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    List<RunnerApi.PTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected void createRunnerForPTransform(
          String pTransformId,
          RunnerApi.PTransform pTransform,
          Supplier<String> processBundleInstructionId,
          Map<String, RunnerApi.PCollection> pCollections,
          Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

        assertThat(processBundleInstructionId.get(), equalTo("999L"));

        transformsProcessed.add(pTransform);
        addStartFunction.accept(
            () -> orderOfOperations.add("Start" + pTransformId));
        addFinishFunction.accept(
            () -> orderOfOperations.add("Finish" + pTransformId));
      }
    };
    handler.processBundle(BeamFnApi.InstructionRequest.newBuilder()
        .setInstructionId("999L")
        .setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
        .build());

    // Processing of transforms is performed in reverse order.
    assertThat(transformsProcessed, contains(
        processBundleDescriptor.getTransformsMap().get("3L"),
        processBundleDescriptor.getTransformsMap().get("2L")));
    // Start should occur in reverse order while finish calls should occur in forward order
    assertThat(orderOfOperations, contains("Start3L", "Start2L", "Finish2L", "Finish3L"));
  }

  @Test
  public void testCreatingPTransformExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected void createRunnerForPTransform(
          String pTransformId,
          RunnerApi.PTransform pTransform,
          Supplier<String> processBundleInstructionId,
          Map<String, RunnerApi.PCollection> pCollections,
          Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("TestException");
        throw new IllegalStateException("TestException");
      }
    };
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformStartExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected void createRunnerForPTransform(
          String pTransformId,
          RunnerApi.PTransform pTransform,
          Supplier<String> processBundleInstructionId,
          Map<String, RunnerApi.PCollection> pCollections,
          Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("TestException");
        addStartFunction.accept(this::throwException);
      }

      private void throwException() {
        throw new IllegalStateException("TestException");
      }
    };
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformFinishExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected void createRunnerForPTransform(
          String pTransformId,
          RunnerApi.PTransform pTransform,
          Supplier<String> processBundleInstructionId,
          Map<String, RunnerApi.PCollection> pCollections,
          Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("TestException");
        addFinishFunction.accept(this::throwException);
      }

      private void throwException() {
        throw new IllegalStateException("TestException");
      }
    };
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
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
    String pTransformId = "100L";
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
        .setUrn(JAVA_DO_FN_URN)
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

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);
    handler.createRunnerForPTransform(
        pTransformId,
        pTransform,
        Suppliers.ofInstance("57L")::get,
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
  public void testCreatingAndProcessingSource() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(LONG_CODER_SPEC_ID, LONG_CODER_SPEC);
    List<WindowedValue<String>> outputValues = new ArrayList<>();

    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("outputPC",
        (ThrowingConsumer) (ThrowingConsumer<WindowedValue<String>>) outputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn(JAVA_SOURCE_URN)
        .setParameter(Any.pack(BytesValue.newBuilder()
            .setValue(ByteString.copyFrom(
                SerializableUtils.serializeToByteArray(CountingSource.upTo(3))))
            .build()))
        .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("input", "inputPC")
        .putOutputs("output", "outputPC")
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createRunnerForPTransform(
        "pTransformId",
        pTransform,
        Suppliers.ofInstance("57L")::get,
        ImmutableMap.of(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    // This is testing a deprecated way of running sources and should be removed
    // once all source definitions are instead propagated along the input edge.
    Iterables.getOnlyElement(startFunctions).run();
    assertThat(outputValues, contains(
        valueInGlobalWindow(0L),
        valueInGlobalWindow(1L),
        valueInGlobalWindow(2L)));
    outputValues.clear();

    // Check that when passing a source along as an input, the source is processed.
    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));
    Iterables.getOnlyElement(consumers.get("inputPC")).accept(
        valueInGlobalWindow(CountingSource.upTo(2)));
    assertThat(outputValues, contains(
        valueInGlobalWindow(0L),
        valueInGlobalWindow(1L)));

    assertThat(finishFunctions, empty());
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataReadRunner() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(STRING_CODER_SPEC_ID, STRING_CODER_SPEC);
    String bundleId = "57";
    String outputId = "101";

    List<WindowedValue<String>> outputValues = new ArrayList<>();

    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("outputPC",
        (ThrowingConsumer) (ThrowingConsumer<WindowedValue<String>>) outputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn(DATA_INPUT_URN)
        .setParameter(Any.pack(REMOTE_PORT))
        .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putOutputs(outputId, "outputPC")
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createRunnerForPTransform(
        "pTransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of("outputPC",
            RunnerApi.PCollection.newBuilder().setCoderId(STRING_CODER_SPEC_ID).build()),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(beamFnDataClient);

    CompletableFuture<Void> completionFuture = new CompletableFuture<>();
    when(beamFnDataClient.forInboundConsumer(any(), any(), any(), any()))
        .thenReturn(completionFuture);
    Iterables.getOnlyElement(startFunctions).run();
    verify(beamFnDataClient).forInboundConsumer(
        eq(REMOTE_PORT.getApiServiceDescriptor()),
        eq(KV.of(bundleId, BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("pTransformId")
            .setName(outputId)
            .build())),
        eq(STRING_CODER),
        consumerCaptor.capture());

    consumerCaptor.getValue().accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder("outputPC"));

    completionFuture.complete(null);
    Iterables.getOnlyElement(finishFunctions).run();

    verifyNoMoreInteractions(beamFnDataClient);
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataWriteRunner() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(STRING_CODER_SPEC_ID, STRING_CODER_SPEC);
    String bundleId = "57L";
    String inputId = "100L";

    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn(DATA_OUTPUT_URN)
        .setParameter(Any.pack(REMOTE_PORT))
        .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs(inputId, "inputPC")
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createRunnerForPTransform(
        "ptransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of("inputPC",
            RunnerApi.PCollection.newBuilder().setCoderId(STRING_CODER_SPEC_ID).build()),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(beamFnDataClient);

    List<WindowedValue<String>> outputValues = new ArrayList<>();
    AtomicBoolean wasCloseCalled = new AtomicBoolean();
    CloseableThrowingConsumer<WindowedValue<String>> outputConsumer =
        new CloseableThrowingConsumer<WindowedValue<String>>(){
          @Override
          public void close() throws Exception {
            wasCloseCalled.set(true);
          }

          @Override
          public void accept(WindowedValue<String> t) throws Exception {
            outputValues.add(t);
          }
        };

    when(beamFnDataClient.forOutboundConsumer(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(outputConsumer);
    Iterables.getOnlyElement(startFunctions).run();
    verify(beamFnDataClient).forOutboundConsumer(
        eq(REMOTE_PORT.getApiServiceDescriptor()),
        eq(KV.of(bundleId, BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("ptransformId")
            .setName(inputId)
            .build())),
        eq(STRING_CODER));

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC"));
    Iterables.getOnlyElement(consumers.get("inputPC")).accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertFalse(wasCloseCalled.get());
    Iterables.getOnlyElement(finishFunctions).run();
    assertTrue(wasCloseCalled.get());

    verifyNoMoreInteractions(beamFnDataClient);
  }
}
