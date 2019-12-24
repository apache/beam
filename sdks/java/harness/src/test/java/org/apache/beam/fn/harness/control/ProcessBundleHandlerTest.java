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

import static org.apache.beam.fn.harness.control.ProcessBundleHandler.REGISTERED_RUNNER_FACTORIES;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.data.QueueingBeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Message;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessBundleHandler}. */
@RunWith(JUnit4.class)
public class ProcessBundleHandlerTest {
  private static final String DATA_INPUT_URN = "beam:source:runner:0.1";
  private static final String DATA_OUTPUT_URN = "beam:sink:runner:0.1";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BeamFnDataClient beamFnDataClient;
  @Captor private ArgumentCaptor<ThrowingConsumer<Exception, WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private static class TestDoFn extends DoFn<String, String> {
    private static final TupleTag<String> mainOutput = new TupleTag<>("mainOutput");

    static List<String> orderOfOperations = new ArrayList<>();

    private enum State {
      NOT_SET_UP,
      SET_UP,
      START_BUNDLE,
      FINISH_BUNDLE,
      TEAR_DOWN
    }

    private TestDoFn.State state = TestDoFn.State.NOT_SET_UP;

    @Setup
    public void setUp() {
      checkState(TestDoFn.State.NOT_SET_UP.equals(state), "Unexpected state: %s", state);
      state = TestDoFn.State.SET_UP;
      orderOfOperations.add("setUp");
    }

    @Teardown
    public void tearDown() {
      checkState(!TestDoFn.State.TEAR_DOWN.equals(state), "Unexpected state: %s", state);
      state = TestDoFn.State.TEAR_DOWN;
      orderOfOperations.add("tearDown");
    }

    @StartBundle
    public void startBundle() {
      state = TestDoFn.State.START_BUNDLE;
      orderOfOperations.add("startBundle");
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      checkState(TestDoFn.State.START_BUNDLE.equals(state), "Unexpected state: %s", state);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      checkState(TestDoFn.State.START_BUNDLE.equals(state), "Unexpected state: %s", state);
      state = TestDoFn.State.FINISH_BUNDLE;
      orderOfOperations.add("finishBundle");
    }
  }

  private static class TestBundleProcessor extends BundleProcessor {
    static int resetCnt = 0;

    private BundleProcessor wrappedBundleProcessor;

    TestBundleProcessor(BundleProcessor wrappedBundleProcessor) {
      this.wrappedBundleProcessor = wrappedBundleProcessor;
    }

    @Override
    PTransformFunctionRegistry getStartFunctionRegistry() {
      return wrappedBundleProcessor.getStartFunctionRegistry();
    }

    @Override
    PTransformFunctionRegistry getFinishFunctionRegistry() {
      return wrappedBundleProcessor.getFinishFunctionRegistry();
    }

    @Override
    List<ThrowingRunnable> getTearDownFunctions() {
      return wrappedBundleProcessor.getTearDownFunctions();
    }

    @Override
    Multimap<String, BeamFnApi.DelayedBundleApplication> getAllResiduals() {
      return wrappedBundleProcessor.getAllResiduals();
    }

    @Override
    PCollectionConsumerRegistry getpCollectionConsumerRegistry() {
      return wrappedBundleProcessor.getpCollectionConsumerRegistry();
    }

    @Override
    MetricsContainerStepMap getMetricsContainerRegistry() {
      return wrappedBundleProcessor.getMetricsContainerRegistry();
    }

    @Override
    ExecutionStateTracker getStateTracker() {
      return wrappedBundleProcessor.getStateTracker();
    }

    @Override
    ProcessBundleHandler.HandleStateCallsForBundle getBeamFnStateClient() {
      return wrappedBundleProcessor.getBeamFnStateClient();
    }

    @Override
    QueueingBeamFnDataClient getQueueingClient() {
      return wrappedBundleProcessor.getQueueingClient();
    }

    @Override
    void reset() {
      resetCnt++;
      wrappedBundleProcessor.reset();
    }
  }

  private static class TestBundleProcessorCache extends BundleProcessorCache {

    @Override
    BundleProcessor get(
        String bundleDescriptorId, Supplier<BundleProcessor> bundleProcessorSupplier) {
      return new TestBundleProcessor(super.get(bundleDescriptorId, bundleProcessorSupplier));
    }
  }

  @Test
  public void testOrderOfStartAndFinishCalls() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .putOutputs("2L-output", "2L-output-pc")
                    .build())
            .putTransforms(
                "3L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_OUTPUT_URN).build())
                    .putInputs("3L-input", "2L-output-pc")
                    .build())
            .putPcollections("2L-output-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    List<RunnerApi.PTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    PTransformRunnerFactory<Object> startFinishRecorder =
        (pipelineOptions,
            beamFnDataClient,
            beamFnStateClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionConsumerRegistry,
            startFunctionRegistry,
            finishFunctionRegistry,
            addTearDownFunction,
            splitListener) -> {
          transformsProcessed.add(pTransform);
          startFunctionRegistry.register(
              pTransformId,
              () -> {
                assertThat(processBundleInstructionId.get(), equalTo("999L"));
                orderOfOperations.add("Start" + pTransformId);
              });
          finishFunctionRegistry.register(
              pTransformId,
              () -> {
                assertThat(processBundleInstructionId.get(), equalTo("999L"));
                orderOfOperations.add("Finish" + pTransformId);
              });
          return null;
        };

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            ImmutableMap.of(
                DATA_INPUT_URN, startFinishRecorder,
                DATA_OUTPUT_URN, startFinishRecorder),
            new BundleProcessorCache());

    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("999L")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // Processing of transforms is performed in reverse order.
    assertThat(
        transformsProcessed,
        contains(
            processBundleDescriptor.getTransformsMap().get("3L"),
            processBundleDescriptor.getTransformsMap().get("2L")));
    // Start should occur in reverse order while finish calls should occur in forward order
    assertThat(orderOfOperations, contains("Start3L", "Start2L", "Finish2L", "Finish3L"));
  }

  @Test
  public void testOrderOfSetupTeardownCalls() throws Exception {
    DoFnWithExecutionInformation doFnWithExecutionInformation =
        DoFnWithExecutionInformation.of(
            new TestDoFn(),
            TestDoFn.mainOutput,
            Collections.emptyMap(),
            DoFnSchemaInformation.create());
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(
                ByteString.copyFrom(
                    SerializableUtils.serializeToByteArray(doFnWithExecutionInformation)))
            .build();
    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.newBuilder().setDoFn(functionSpec).build();
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .putOutputs("2L-output", "2L-output-pc")
                    .build())
            .putTransforms(
                "3L",
                PTransform.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(parDoPayload.toByteString()))
                    .putInputs("3L-input", "2L-output-pc")
                    .build())
            .putPcollections(
                "2L-output-pc",
                PCollection.newBuilder()
                    .setWindowingStrategyId("window-strategy")
                    .setCoderId("2L-output-coder")
                    .build())
            .putWindowingStrategies(
                "window-strategy",
                WindowingStrategy.newBuilder()
                    .setWindowCoderId("window-strategy-coder")
                    .setWindowFn(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:windowfn:global_windows:v0.1"))
                    .setOutputTime(RunnerApi.OutputTime.Enum.END_OF_WINDOW)
                    .setAccumulationMode(RunnerApi.AccumulationMode.Enum.ACCUMULATING)
                    .setTrigger(
                        RunnerApi.Trigger.newBuilder()
                            .setAlways(RunnerApi.Trigger.Always.getDefaultInstance()))
                    .setClosingBehavior(RunnerApi.ClosingBehavior.Enum.EMIT_ALWAYS)
                    .setOnTimeBehavior(RunnerApi.OnTimeBehavior.Enum.FIRE_ALWAYS)
                    .build())
            .putCoders("2L-output-coder", CoderTranslation.toProto(StringUtf8Coder.of()).getCoder())
            .putCoders(
                "window-strategy-coder",
                Coder.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                            .build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap =
        Maps.newHashMap(REGISTERED_RUNNER_FACTORIES);
    urnToPTransformRunnerFactoryMap.put(
        DATA_INPUT_URN,
        (pipelineOptions,
            beamFnDataClient,
            beamFnStateClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionConsumerRegistry,
            startFunctionRegistry,
            finishFunctionRegistry,
            addTearDownFunction,
            splitListener) -> null);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            urnToPTransformRunnerFactoryMap,
            new BundleProcessorCache());

    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("998L")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("999L")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    handler.shutdown();

    // setup and teardown should occur only once when processing multiple bundles for the same
    // descriptor
    assertThat(
        TestDoFn.orderOfOperations,
        contains(
            "setUp", "startBundle", "finishBundle", "startBundle", "finishBundle", "tearDown"));
  }

  @Test
  public void testBundleProcessorIsResetWhenAddedBackToCache() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (pipelineOptions,
                    beamFnDataClient,
                    beamFnStateClient,
                    pTransformId,
                    pTransform,
                    processBundleInstructionId,
                    pCollections,
                    coders,
                    windowingStrategies,
                    pCollectionConsumerRegistry,
                    startFunctionRegistry,
                    finishFunctionRegistry,
                    addTearDownFunction,
                    splitListener) -> null),
            new TestBundleProcessorCache());

    assertThat(TestBundleProcessor.resetCnt, equalTo(0));

    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("998L")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // Check that BundleProcessor is reset when added back to the cache
    assertThat(TestBundleProcessor.resetCnt, equalTo(1));

    // BundleProcessor is added back to the BundleProcessorCache
    assertThat(handler.bundleProcessorCache.getCachedBundleProcessors().size(), equalTo(1));
    assertThat(
        handler.bundleProcessorCache.getCachedBundleProcessors().get("1L").size(), equalTo(1));
  }

  @Test
  public void testBundleProcessorReset() {
    PTransformFunctionRegistry startFunctionRegistry = mock(PTransformFunctionRegistry.class);
    PTransformFunctionRegistry finishFunctionRegistry = mock(PTransformFunctionRegistry.class);
    Multimap<String, BeamFnApi.DelayedBundleApplication> allResiduals = mock(Multimap.class);
    PCollectionConsumerRegistry pCollectionConsumerRegistry =
        mock(PCollectionConsumerRegistry.class);
    MetricsContainerStepMap metricsContainerRegistry = mock(MetricsContainerStepMap.class);
    ExecutionStateTracker stateTracker = mock(ExecutionStateTracker.class);
    ProcessBundleHandler.HandleStateCallsForBundle beamFnStateClient =
        mock(ProcessBundleHandler.HandleStateCallsForBundle.class);
    QueueingBeamFnDataClient queueingClient = mock(QueueingBeamFnDataClient.class);
    BundleProcessor bundleProcessor =
        BundleProcessor.create(
            startFunctionRegistry,
            finishFunctionRegistry,
            new ArrayList<>(),
            allResiduals,
            pCollectionConsumerRegistry,
            metricsContainerRegistry,
            stateTracker,
            beamFnStateClient,
            queueingClient);

    bundleProcessor.reset();
    verify(startFunctionRegistry, times(1)).reset();
    verify(finishFunctionRegistry, times(1)).reset();
    verify(allResiduals, times(1)).clear();
    verify(pCollectionConsumerRegistry, times(1)).reset();
    verify(metricsContainerRegistry, times(1)).reset();
    verify(stateTracker, times(1)).reset();
  }

  @Test
  public void testCreatingPTransformExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (pipelineOptions,
                    beamFnDataClient,
                    beamFnStateClient,
                    pTransformId,
                    pTransform,
                    processBundleInstructionId,
                    pCollections,
                    coders,
                    windowingStrategies,
                    pCollectionConsumerRegistry,
                    startFunctionRegistry,
                    finishFunctionRegistry,
                    addTearDownFunction,
                    splitListener) -> {
                  thrown.expect(IllegalStateException.class);
                  thrown.expectMessage("TestException");
                  throw new IllegalStateException("TestException");
                }),
            new BundleProcessorCache());
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());
  }

  @Test
  public void testPTransformStartExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionConsumerRegistry,
                        startFunctionRegistry,
                        finishFunctionRegistry,
                        addTearDownFunction,
                        splitListener) -> {
                      thrown.expect(IllegalStateException.class);
                      thrown.expectMessage("TestException");
                      startFunctionRegistry.register(
                          pTransformId, ProcessBundleHandlerTest::throwException);
                      return null;
                    }),
            new BundleProcessorCache());
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // BundleProcessor is not re-added back to the BundleProcessorCache in case of an exception
    // during bundle processing
    assertThat(
        handler.bundleProcessorCache.getCachedBundleProcessors(), equalTo(Collections.EMPTY_MAP));
  }

  @Test
  public void testPTransformFinishExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionConsumerRegistry,
                        startFunctionRegistry,
                        finishFunctionRegistry,
                        addTearDownFunction,
                        splitListener) -> {
                      thrown.expect(IllegalStateException.class);
                      thrown.expectMessage("TestException");
                      finishFunctionRegistry.register(
                          pTransformId, ProcessBundleHandlerTest::throwException);
                      return null;
                    }),
            new BundleProcessorCache());
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // BundleProcessor is not re-added back to the BundleProcessorCache in case of an exception
    // during bundle processing
    assertThat(
        handler.bundleProcessorCache.getCachedBundleProcessors(), equalTo(Collections.EMPTY_MAP));
  }

  @Test
  public void testPendingStateCallsBlockTillCompletion() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .setStateApiServiceDescriptor(ApiServiceDescriptor.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    CompletableFuture<StateResponse> successfulResponse = new CompletableFuture<>();
    CompletableFuture<StateResponse> unsuccessfulResponse = new CompletableFuture<>();

    BeamFnStateGrpcClientCache mockBeamFnStateGrpcClient =
        Mockito.mock(BeamFnStateGrpcClientCache.class);
    BeamFnStateClient mockBeamFnStateClient = Mockito.mock(BeamFnStateClient.class);
    when(mockBeamFnStateGrpcClient.forApiServiceDescriptor(any()))
        .thenReturn(mockBeamFnStateClient);

    doAnswer(
            invocation -> {
              StateRequest.Builder stateRequestBuilder =
                  (StateRequest.Builder) invocation.getArguments()[0];
              CompletableFuture<StateResponse> completableFuture =
                  (CompletableFuture<StateResponse>) invocation.getArguments()[1];
              new Thread(
                      () -> {
                        // Simulate sleeping which introduces a race which most of the time requires
                        // the ProcessBundleHandler to block.
                        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                        switch (stateRequestBuilder.getInstructionId()) {
                          case "SUCCESS":
                            completableFuture.complete(StateResponse.getDefaultInstance());
                            break;
                          case "FAIL":
                            completableFuture.completeExceptionally(
                                new RuntimeException("TEST ERROR"));
                        }
                      })
                  .start();
              return null;
            })
        .when(mockBeamFnStateClient)
        .handle(any(), any());

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            mockBeamFnStateGrpcClient,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(
                      PipelineOptions pipelineOptions,
                      BeamFnDataClient beamFnDataClient,
                      BeamFnStateClient beamFnStateClient,
                      String pTransformId,
                      PTransform pTransform,
                      Supplier<String> processBundleInstructionId,
                      Map<String, PCollection> pCollections,
                      Map<String, Coder> coders,
                      Map<String, WindowingStrategy> windowingStrategies,
                      PCollectionConsumerRegistry pCollectionConsumerRegistry,
                      PTransformFunctionRegistry startFunctionRegistry,
                      PTransformFunctionRegistry finishFunctionRegistry,
                      Consumer<ThrowingRunnable> addTearDownFunction,
                      BundleSplitListener splitListener)
                      throws IOException {
                    startFunctionRegistry.register(
                        pTransformId, () -> doStateCalls(beamFnStateClient));
                    return null;
                  }

                  private void doStateCalls(BeamFnStateClient beamFnStateClient) {
                    beamFnStateClient.handle(
                        StateRequest.newBuilder().setInstructionId("SUCCESS"), successfulResponse);
                    beamFnStateClient.handle(
                        StateRequest.newBuilder().setInstructionId("FAIL"), unsuccessfulResponse);
                  }
                }),
            new BundleProcessorCache());
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    assertTrue(successfulResponse.isDone());
    assertTrue(unsuccessfulResponse.isDone());
  }

  @Test
  public void testStateCallsFailIfNoStateApiServiceDescriptorSpecified() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(
                      PipelineOptions pipelineOptions,
                      BeamFnDataClient beamFnDataClient,
                      BeamFnStateClient beamFnStateClient,
                      String pTransformId,
                      PTransform pTransform,
                      Supplier<String> processBundleInstructionId,
                      Map<String, PCollection> pCollections,
                      Map<String, Coder> coders,
                      Map<String, WindowingStrategy> windowingStrategies,
                      PCollectionConsumerRegistry pCollectionConsumerRegistry,
                      PTransformFunctionRegistry startFunctionRegistry,
                      PTransformFunctionRegistry finishFunctionRegistry,
                      Consumer<ThrowingRunnable> addTearDownFunction,
                      BundleSplitListener splitListener)
                      throws IOException {
                    startFunctionRegistry.register(
                        pTransformId, () -> doStateCalls(beamFnStateClient));
                    return null;
                  }

                  private void doStateCalls(BeamFnStateClient beamFnStateClient) {
                    thrown.expect(IllegalStateException.class);
                    thrown.expectMessage("State API calls are unsupported");
                    beamFnStateClient.handle(
                        StateRequest.newBuilder().setInstructionId("SUCCESS"),
                        new CompletableFuture<>());
                  }
                }),
            new BundleProcessorCache());
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());
  }

  private static void throwException() {
    throw new IllegalStateException("TestException");
  }
}
