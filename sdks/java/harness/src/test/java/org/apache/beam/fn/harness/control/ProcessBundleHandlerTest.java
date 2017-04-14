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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
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
  private static final BeamFnApi.Coder LONG_CODER_SPEC;
  private static final BeamFnApi.Coder STRING_CODER_SPEC;
  static {
    try {
      STRING_CODER_SPEC =
          BeamFnApi.Coder.newBuilder().setFunctionSpec(BeamFnApi.FunctionSpec.newBuilder()
          .setId(STRING_CODER_SPEC_ID)
          .setData(Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
              OBJECT_MAPPER.writeValueAsBytes(STRING_CODER.asCloudObject()))).build())))
          .build();
      LONG_CODER_SPEC =
          BeamFnApi.Coder.newBuilder().setFunctionSpec(BeamFnApi.FunctionSpec.newBuilder()
          .setId(STRING_CODER_SPEC_ID)
          .setData(Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
              OBJECT_MAPPER.writeValueAsBytes(WindowedValue.getFullCoder(
                  VarLongCoder.of(), GlobalWindow.Coder.INSTANCE).asCloudObject()))).build())))
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
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("2L"))
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("3L"))
        .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    List<BeamFnApi.PrimitiveTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected <InputT, OutputT> void createConsumersForPrimitiveTransform(
          BeamFnApi.PrimitiveTransform primitiveTransform,
          Supplier<String> processBundleInstructionId,
          Function<BeamFnApi.Target,
                   Collection<ThrowingConsumer<WindowedValue<OutputT>>>> consumers,
          BiConsumer<BeamFnApi.Target, ThrowingConsumer<WindowedValue<InputT>>> addConsumer,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction)
          throws IOException {

        assertThat(processBundleInstructionId.get(), equalTo("999L"));

        transformsProcessed.add(primitiveTransform);
        addStartFunction.accept(
            () -> orderOfOperations.add("Start" + primitiveTransform.getId()));
        addFinishFunction.accept(
            () -> orderOfOperations.add("Finish" + primitiveTransform.getId()));
      }
    };
    handler.processBundle(BeamFnApi.InstructionRequest.newBuilder()
        .setInstructionId("999L")
        .setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
        .build());

    // Processing of primitive transforms is performed in reverse order.
    assertThat(transformsProcessed, contains(
        processBundleDescriptor.getPrimitiveTransform(1),
        processBundleDescriptor.getPrimitiveTransform(0)));
    // Start should occur in reverse order while finish calls should occur in forward order
    assertThat(orderOfOperations, contains("Start3L", "Start2L", "Finish2L", "Finish3L"));
  }

  @Test
  public void testCreatingPrimitiveTransformExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("2L"))
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("3L"))
        .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected <InputT, OutputT> void createConsumersForPrimitiveTransform(
          BeamFnApi.PrimitiveTransform primitiveTransform,
          Supplier<String> processBundleInstructionId,
          Function<BeamFnApi.Target,
                   Collection<ThrowingConsumer<WindowedValue<OutputT>>>> consumers,
          BiConsumer<BeamFnApi.Target, ThrowingConsumer<WindowedValue<InputT>>> addConsumer,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction)
          throws IOException {
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
  public void testPrimitiveTransformStartExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("2L"))
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("3L"))
        .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected <InputT, OutputT> void createConsumersForPrimitiveTransform(
          BeamFnApi.PrimitiveTransform primitiveTransform,
          Supplier<String> processBundleInstructionId,
          Function<BeamFnApi.Target,
                   Collection<ThrowingConsumer<WindowedValue<OutputT>>>> consumers,
          BiConsumer<BeamFnApi.Target, ThrowingConsumer<WindowedValue<InputT>>> addConsumer,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction)
          throws IOException {
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
  public void testPrimitiveTransformFinishExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("2L"))
        .addPrimitiveTransform(BeamFnApi.PrimitiveTransform.newBuilder().setId("3L"))
        .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient) {
      @Override
      protected <InputT, OutputT> void createConsumersForPrimitiveTransform(
          BeamFnApi.PrimitiveTransform primitiveTransform,
          Supplier<String> processBundleInstructionId,
          Function<BeamFnApi.Target,
                   Collection<ThrowingConsumer<WindowedValue<OutputT>>>> consumers,
          BiConsumer<BeamFnApi.Target, ThrowingConsumer<WindowedValue<InputT>>> addConsumer,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction)
          throws IOException {
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

    @StartBundle
    public void startBundle(Context context) {
      context.output("StartBundle");
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output("MainOutput" + context.element());
      context.output(additionalOutput, "AdditionalOutput" + context.element());
    }

    @FinishBundle
    public void finishBundle(Context context) {
      context.output("FinishBundle");
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
    String primitiveTransformId = "100L";
    long mainOutputId = 101L;
    long additionalOutputId = 102L;

    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        new TestDoFn(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(),
        STRING_CODER,
        mainOutputId,
        ImmutableMap.of(
            mainOutputId, TestDoFn.mainOutput,
            additionalOutputId, TestDoFn.additionalOutput));
    BeamFnApi.FunctionSpec functionSpec = BeamFnApi.FunctionSpec.newBuilder()
        .setId("1L")
        .setUrn(JAVA_DO_FN_URN)
        .setData(Any.pack(BytesValue.newBuilder()
            .setValue(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build()))
        .build();
    BeamFnApi.Target inputATarget1 = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference("1000L")
        .setName("inputATarget1")
        .build();
    BeamFnApi.Target inputATarget2 = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference("1001L")
        .setName("inputATarget1")
        .build();
    BeamFnApi.Target inputBTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference("1002L")
        .setName("inputBTarget")
        .build();
    BeamFnApi.PrimitiveTransform primitiveTransform = BeamFnApi.PrimitiveTransform.newBuilder()
        .setId(primitiveTransformId)
        .setFunctionSpec(functionSpec)
        .putInputs("inputA", BeamFnApi.Target.List.newBuilder()
            .addTarget(inputATarget1)
            .addTarget(inputATarget2)
            .build())
        .putInputs("inputB", BeamFnApi.Target.List.newBuilder()
            .addTarget(inputBTarget)
            .build())
        .putOutputs(Long.toString(mainOutputId), BeamFnApi.PCollection.newBuilder()
            .setCoderReference(STRING_CODER_SPEC_ID)
            .build())
        .putOutputs(Long.toString(additionalOutputId), BeamFnApi.PCollection.newBuilder()
            .setCoderReference(STRING_CODER_SPEC_ID)
            .build())
        .build();

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    BeamFnApi.Target mainOutputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference(primitiveTransformId)
        .setName(Long.toString(mainOutputId))
        .build();
    BeamFnApi.Target additionalOutputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference(primitiveTransformId)
        .setName(Long.toString(additionalOutputId))
        .build();
    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> existingConsumers =
        ImmutableMultimap.of(
            mainOutputTarget, mainOutputValues::add,
            additionalOutputTarget, additionalOutputValues::add);
    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> newConsumers =
        HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);
    handler.createConsumersForPrimitiveTransform(
        primitiveTransform,
        Suppliers.ofInstance("57L")::get,
        existingConsumers::get,
        newConsumers::put,
        startFunctions::add,
        finishFunctions::add);

    Iterables.getOnlyElement(startFunctions).run();
    assertThat(mainOutputValues, contains(valueInGlobalWindow("StartBundle")));
    mainOutputValues.clear();

    assertEquals(newConsumers.keySet(),
        ImmutableSet.of(inputATarget1, inputATarget2, inputBTarget));

    Iterables.getOnlyElement(newConsumers.get(inputATarget1)).accept(valueInGlobalWindow("A1"));
    Iterables.getOnlyElement(newConsumers.get(inputATarget1)).accept(valueInGlobalWindow("A2"));
    Iterables.getOnlyElement(newConsumers.get(inputATarget1)).accept(valueInGlobalWindow("B"));
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
    assertThat(mainOutputValues, contains(valueInGlobalWindow("FinishBundle")));
    mainOutputValues.clear();
  }

  @Test
  public void testCreatingAndProcessingSource() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(LONG_CODER_SPEC_ID, LONG_CODER_SPEC);
    String primitiveTransformId = "100L";
    long outputId = 101L;

    BeamFnApi.Target inputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference("1000L")
        .setName("inputTarget")
        .build();

    List<WindowedValue<String>> outputValues = new ArrayList<>();
    BeamFnApi.Target outputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference(primitiveTransformId)
        .setName(Long.toString(outputId))
        .build();

    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> existingConsumers =
        ImmutableMultimap.of(outputTarget, outputValues::add);
    Multimap<BeamFnApi.Target,
             ThrowingConsumer<WindowedValue<BoundedSource<Long>>>> newConsumers =
             HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    BeamFnApi.FunctionSpec functionSpec = BeamFnApi.FunctionSpec.newBuilder()
        .setId("1L")
        .setUrn(JAVA_SOURCE_URN)
        .setData(Any.pack(BytesValue.newBuilder()
            .setValue(ByteString.copyFrom(
                SerializableUtils.serializeToByteArray(CountingSource.upTo(3))))
            .build()))
        .build();

    BeamFnApi.PrimitiveTransform primitiveTransform = BeamFnApi.PrimitiveTransform.newBuilder()
        .setId(primitiveTransformId)
        .setFunctionSpec(functionSpec)
        .putInputs("input",
            BeamFnApi.Target.List.newBuilder().addTarget(inputTarget).build())
        .putOutputs(Long.toString(outputId),
            BeamFnApi.PCollection.newBuilder().setCoderReference(LONG_CODER_SPEC_ID).build())
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createConsumersForPrimitiveTransform(
        primitiveTransform,
        Suppliers.ofInstance("57L")::get,
        existingConsumers::get,
        newConsumers::put,
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
    assertEquals(newConsumers.keySet(), ImmutableSet.of(inputTarget));
    Iterables.getOnlyElement(newConsumers.get(inputTarget)).accept(
        valueInGlobalWindow(CountingSource.upTo(2)));
    assertThat(outputValues, contains(
        valueInGlobalWindow(0L),
        valueInGlobalWindow(1L)));

    assertThat(finishFunctions, empty());
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataReadRunner() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(STRING_CODER_SPEC_ID, STRING_CODER_SPEC);
    String bundleId = "57L";
    String primitiveTransformId = "100L";
    long outputId = 101L;

    List<WindowedValue<String>> outputValues = new ArrayList<>();
    BeamFnApi.Target outputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference(primitiveTransformId)
        .setName(Long.toString(outputId))
        .build();

    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> existingConsumers =
        ImmutableMultimap.of(outputTarget, outputValues::add);
    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> newConsumers =
        HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    BeamFnApi.FunctionSpec functionSpec = BeamFnApi.FunctionSpec.newBuilder()
        .setId("1L")
        .setUrn(DATA_INPUT_URN)
        .setData(Any.pack(REMOTE_PORT))
        .build();

    BeamFnApi.PrimitiveTransform primitiveTransform = BeamFnApi.PrimitiveTransform.newBuilder()
        .setId(primitiveTransformId)
        .setFunctionSpec(functionSpec)
        .putInputs("input", BeamFnApi.Target.List.getDefaultInstance())
        .putOutputs(Long.toString(outputId),
            BeamFnApi.PCollection.newBuilder().setCoderReference(STRING_CODER_SPEC_ID).build())
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createConsumersForPrimitiveTransform(
        primitiveTransform,
        Suppliers.ofInstance(bundleId)::get,
        existingConsumers::get,
        newConsumers::put,
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
            .setPrimitiveTransformReference(primitiveTransformId)
            .setName("input")
            .build())),
        eq(STRING_CODER),
        consumerCaptor.capture());

    consumerCaptor.getValue().accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertThat(newConsumers.keySet(), empty());

    completionFuture.complete(null);
    Iterables.getOnlyElement(finishFunctions).run();

    verifyNoMoreInteractions(beamFnDataClient);
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataWriteRunner() throws Exception {
    Map<String, Message> fnApiRegistry = ImmutableMap.of(STRING_CODER_SPEC_ID, STRING_CODER_SPEC);
    String bundleId = "57L";
    String primitiveTransformId = "100L";
    long outputId = 101L;

    BeamFnApi.Target inputTarget = BeamFnApi.Target.newBuilder()
        .setPrimitiveTransformReference("1000L")
        .setName("inputTarget")
        .build();

    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> existingConsumers =
        ImmutableMultimap.of();
    Multimap<BeamFnApi.Target, ThrowingConsumer<WindowedValue<String>>> newConsumers =
        HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    BeamFnApi.FunctionSpec functionSpec = BeamFnApi.FunctionSpec.newBuilder()
        .setId("1L")
        .setUrn(DATA_OUTPUT_URN)
        .setData(Any.pack(REMOTE_PORT))
        .build();

    BeamFnApi.PrimitiveTransform primitiveTransform = BeamFnApi.PrimitiveTransform.newBuilder()
        .setId(primitiveTransformId)
        .setFunctionSpec(functionSpec)
        .putInputs("input", BeamFnApi.Target.List.newBuilder().addTarget(inputTarget).build())
        .putOutputs(Long.toString(outputId),
            BeamFnApi.PCollection.newBuilder().setCoderReference(STRING_CODER_SPEC_ID).build())
        .build();

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient);

    handler.createConsumersForPrimitiveTransform(
        primitiveTransform,
        Suppliers.ofInstance(bundleId)::get,
        existingConsumers::get,
        newConsumers::put,
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
            .setPrimitiveTransformReference(primitiveTransformId)
            .setName(Long.toString(outputId))
            .build())),
        eq(STRING_CODER));

    assertEquals(newConsumers.keySet(), ImmutableSet.of(inputTarget));
    Iterables.getOnlyElement(newConsumers.get(inputTarget)).accept(
        valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertFalse(wasCloseCalled.get());
    Iterables.getOnlyElement(finishFunctions).run();
    assertTrue(wasCloseCalled.get());

    verifyNoMoreInteractions(beamFnDataClient);
  }
}
