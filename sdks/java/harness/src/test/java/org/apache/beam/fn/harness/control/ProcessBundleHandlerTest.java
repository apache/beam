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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.BeamFnDataReadRunner;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.PTransformRunnerFactory.ProgressRequestCallback;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
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
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Message;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Instant;
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
  private static final String DATA_INPUT_URN = "beam:runner:source:v1";
  private static final String DATA_OUTPUT_URN = "beam:runner:sink:v1";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BeamFnDataClient beamFnDataClient;
  @Captor private ArgumentCaptor<ThrowingConsumer<Exception, WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    TestBundleProcessor.resetCnt = 0;
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
    List<ThrowingRunnable> getResetFunctions() {
      return wrappedBundleProcessor.getResetFunctions();
    }

    @Override
    List<ThrowingRunnable> getTearDownFunctions() {
      return wrappedBundleProcessor.getTearDownFunctions();
    }

    @Override
    List<ProgressRequestCallback> getProgressRequestCallbacks() {
      return wrappedBundleProcessor.getProgressRequestCallbacks();
    }

    @Override
    BundleSplitListener.InMemory getSplitListener() {
      return wrappedBundleProcessor.getSplitListener();
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
    Collection<CallbackRegistration> getBundleFinalizationCallbackRegistrations() {
      return wrappedBundleProcessor.getBundleFinalizationCallbackRegistrations();
    }

    @Override
    Collection<BeamFnDataReadRunner> getChannelRoots() {
      return wrappedBundleProcessor.getChannelRoots();
    }

    @Override
    void reset() throws Exception {
      resetCnt++;
      wrappedBundleProcessor.reset();
    }
  }

  private static class TestBundleProcessorCache extends BundleProcessorCache {

    @Override
    BundleProcessor get(
        String bundleDescriptorId,
        String instructionId,
        Supplier<BundleProcessor> bundleProcessorSupplier) {
      return new TestBundleProcessor(
          super.get(bundleDescriptorId, instructionId, bundleProcessorSupplier));
    }
  }

  @Test
  public void testTrySplitBeforeBundleDoesNotFail() {
    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            null,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            ImmutableMap.of(),
            new BundleProcessorCache());

    BeamFnApi.InstructionResponse response =
        handler
            .trySplit(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setInstructionId("999L")
                    .setProcessBundleSplit(
                        BeamFnApi.ProcessBundleSplitRequest.newBuilder()
                            .setInstructionId("unknown-id"))
                    .build())
            .build();
    assertNotNull(response.getProcessBundleSplit());
    assertEquals(0, response.getProcessBundleSplit().getChannelSplitsCount());
  }

  @Test
  public void testProgressBeforeBundleDoesNotFail() throws Exception {
    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            null,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            ImmutableMap.of(),
            new BundleProcessorCache());

    handler.progress(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("999L")
            .setProcessBundleProgress(
                BeamFnApi.ProcessBundleProgressRequest.newBuilder().setInstructionId("unknown-id"))
            .build());
    BeamFnApi.InstructionResponse response =
        handler
            .trySplit(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setInstructionId("999L")
                    .setProcessBundleSplit(
                        BeamFnApi.ProcessBundleSplitRequest.newBuilder()
                            .setInstructionId("unknown-id"))
                    .build())
            .build();
    assertNotNull(response.getProcessBundleProgress());
    assertEquals(0, response.getProcessBundleProgress().getMonitoringInfosCount());
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
            beamFnTimerClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionConsumerRegistry,
            startFunctionRegistry,
            finishFunctionRegistry,
            addResetFunction,
            addTearDownFunction,
            addProgressRequestCallback,
            splitListener,
            bundleFinalizer) -> {
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
            null /* finalizeBundleHandler */,
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
                            .setUrn("beam:window_fn:global_windows:v1"))
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
            beamFnTimerClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionConsumerRegistry,
            startFunctionRegistry,
            finishFunctionRegistry,
            addResetFunction,
            addTearDownFunction,
            addProgressRequestCallback,
            splitListener,
            bundleFinalizer) -> null);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (pipelineOptions,
                    beamFnDataClient,
                    beamFnStateClient,
                    beamFnTimerClient,
                    pTransformId,
                    pTransform,
                    processBundleInstructionId,
                    pCollections,
                    coders,
                    windowingStrategies,
                    pCollectionConsumerRegistry,
                    startFunctionRegistry,
                    finishFunctionRegistry,
                    addResetFunction,
                    addTearDownFunction,
                    addProgressRequestCallback,
                    splitListener,
                    bundleFinalizer) -> null),
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

    // Add a reset handler that throws to test discarding the bundle processor on reset failure.
    Iterables.getOnlyElement(handler.bundleProcessorCache.getCachedBundleProcessors().get("1L"))
        .getResetFunctions()
        .add(
            () -> {
              throw new IllegalStateException("ResetFailed");
            });

    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("999L")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // BundleProcessor is discarded instead of being added back to the BundleProcessorCache
    assertThat(
        handler.bundleProcessorCache.getCachedBundleProcessors().get("1L").size(), equalTo(0));
  }

  @Test
  public void testBundleProcessorIsFoundWhenActive() {
    BundleProcessor bundleProcessor = mock(BundleProcessor.class);
    when(bundleProcessor.getInstructionId()).thenReturn("known");
    BundleProcessorCache cache = new BundleProcessorCache();

    // Check that an unknown bundle processor is not found
    assertNull(cache.find("unknown"));

    // Once it is active, ensure the bundle processor is found
    cache.get("descriptorId", "known", () -> bundleProcessor);
    assertSame(bundleProcessor, cache.find("known"));

    // After it is released, ensure the bundle processor is no longer found
    cache.release("descriptorId", bundleProcessor);
    assertNull(cache.find("known"));
  }

  @Test
  public void testBundleProcessorReset() throws Exception {
    PTransformFunctionRegistry startFunctionRegistry = mock(PTransformFunctionRegistry.class);
    PTransformFunctionRegistry finishFunctionRegistry = mock(PTransformFunctionRegistry.class);
    BundleSplitListener.InMemory splitListener = mock(BundleSplitListener.InMemory.class);
    Collection<CallbackRegistration> bundleFinalizationCallbacks = mock(Collection.class);
    PCollectionConsumerRegistry pCollectionConsumerRegistry =
        mock(PCollectionConsumerRegistry.class);
    MetricsContainerStepMap metricsContainerRegistry = mock(MetricsContainerStepMap.class);
    ExecutionStateTracker stateTracker = mock(ExecutionStateTracker.class);
    ProcessBundleHandler.HandleStateCallsForBundle beamFnStateClient =
        mock(ProcessBundleHandler.HandleStateCallsForBundle.class);
    QueueingBeamFnDataClient queueingClient = mock(QueueingBeamFnDataClient.class);
    ThrowingRunnable resetFunction = mock(ThrowingRunnable.class);
    BundleProcessor bundleProcessor =
        BundleProcessor.create(
            startFunctionRegistry,
            finishFunctionRegistry,
            Collections.singletonList(resetFunction),
            new ArrayList<>(),
            new ArrayList<>(),
            splitListener,
            pCollectionConsumerRegistry,
            metricsContainerRegistry,
            stateTracker,
            beamFnStateClient,
            queueingClient,
            bundleFinalizationCallbacks);

    bundleProcessor.reset();
    verify(startFunctionRegistry, times(1)).reset();
    verify(finishFunctionRegistry, times(1)).reset();
    verify(splitListener, times(1)).clear();
    verify(pCollectionConsumerRegistry, times(1)).reset();
    verify(metricsContainerRegistry, times(1)).reset();
    verify(stateTracker, times(1)).reset();
    verify(bundleFinalizationCallbacks, times(1)).clear();
    verify(resetFunction, times(1)).run();
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (pipelineOptions,
                    beamFnDataClient,
                    beamFnStateClient,
                    beamFnTimerClient,
                    pTransformId,
                    pTransform,
                    processBundleInstructionId,
                    pCollections,
                    coders,
                    windowingStrategies,
                    pCollectionConsumerRegistry,
                    startFunctionRegistry,
                    finishFunctionRegistry,
                    addResetFunction,
                    addTearDownFunction,
                    addProgressRequestCallback,
                    splitListener,
                    bundleFinalizer) -> {
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
  public void testBundleFinalizationIsPropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);
    FinalizeBundleHandler mockFinalizeBundleHandler = mock(FinalizeBundleHandler.class);
    BundleFinalizer.Callback mockCallback = mock(BundleFinalizer.Callback.class);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            mockFinalizeBundleHandler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        beamFnTimerClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionConsumerRegistry,
                        startFunctionRegistry,
                        finishFunctionRegistry,
                        addResetFunction,
                        addTearDownFunction,
                        addProgressRequestCallback,
                        splitListener,
                        bundleFinalizer) -> {
                      startFunctionRegistry.register(
                          pTransformId,
                          () ->
                              bundleFinalizer.afterBundleCommit(
                                  Instant.ofEpochMilli(42L), mockCallback));
                      return null;
                    }),
            new BundleProcessorCache());
    BeamFnApi.InstructionResponse.Builder response =
        handler.processBundle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId("2L")
                .setProcessBundle(
                    BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
                .build());

    assertTrue(response.getProcessBundle().getRequiresFinalization());
    verify(mockFinalizeBundleHandler)
        .registerCallbacks(
            eq("2L"),
            argThat(
                (Collection<CallbackRegistration> arg) -> {
                  CallbackRegistration registration = Iterables.getOnlyElement(arg);
                  assertEquals(Instant.ofEpochMilli(42L), registration.getExpiryTime());
                  assertSame(mockCallback, registration.getCallback());
                  return true;
                }));
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        beamFnTimerClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionConsumerRegistry,
                        startFunctionRegistry,
                        finishFunctionRegistry,
                        addResetFunction,
                        addTearDownFunction,
                        addProgressRequestCallback,
                        splitListener,
                        bundleFinalizer) -> {
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        beamFnTimerClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionConsumerRegistry,
                        startFunctionRegistry,
                        finishFunctionRegistry,
                        addResetFunction,
                        addTearDownFunction,
                        addProgressRequestCallback,
                        splitListener,
                        bundleFinalizer) -> {
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(
                      PipelineOptions pipelineOptions,
                      BeamFnDataClient beamFnDataClient,
                      BeamFnStateClient beamFnStateClient,
                      BeamFnTimerClient beamFnTimerClient,
                      String pTransformId,
                      PTransform pTransform,
                      Supplier<String> processBundleInstructionId,
                      Map<String, PCollection> pCollections,
                      Map<String, Coder> coders,
                      Map<String, WindowingStrategy> windowingStrategies,
                      PCollectionConsumerRegistry pCollectionConsumerRegistry,
                      PTransformFunctionRegistry startFunctionRegistry,
                      PTransformFunctionRegistry finishFunctionRegistry,
                      Consumer<ThrowingRunnable> addResetFunction,
                      Consumer<ThrowingRunnable> addTearDownFunction,
                      Consumer<ProgressRequestCallback> addProgressRequestCallback,
                      BundleSplitListener splitListener,
                      BundleFinalizer bundleFinalizer)
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(
                      PipelineOptions pipelineOptions,
                      BeamFnDataClient beamFnDataClient,
                      BeamFnStateClient beamFnStateClient,
                      BeamFnTimerClient beamFnTimerClient,
                      String pTransformId,
                      PTransform pTransform,
                      Supplier<String> processBundleInstructionId,
                      Map<String, PCollection> pCollections,
                      Map<String, Coder> coders,
                      Map<String, WindowingStrategy> windowingStrategies,
                      PCollectionConsumerRegistry pCollectionConsumerRegistry,
                      PTransformFunctionRegistry startFunctionRegistry,
                      PTransformFunctionRegistry finishFunctionRegistry,
                      Consumer<ThrowingRunnable> addResetFunction,
                      Consumer<ThrowingRunnable> addTearDownFunction,
                      Consumer<ProgressRequestCallback> addProgressRequestCallback,
                      BundleSplitListener splitListener,
                      BundleFinalizer bundleFinalizer)
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

  @Test
  public void testTimerRegistrationsFailIfNoTimerApiServiceDescriptorSpecified() throws Exception {
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
            null /* finalizeBundleHandler */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(
                      PipelineOptions pipelineOptions,
                      BeamFnDataClient beamFnDataClient,
                      BeamFnStateClient beamFnStateClient,
                      BeamFnTimerClient beamFnTimerClient,
                      String pTransformId,
                      PTransform pTransform,
                      Supplier<String> processBundleInstructionId,
                      Map<String, PCollection> pCollections,
                      Map<String, Coder> coders,
                      Map<String, WindowingStrategy> windowingStrategies,
                      PCollectionConsumerRegistry pCollectionConsumerRegistry,
                      PTransformFunctionRegistry startFunctionRegistry,
                      PTransformFunctionRegistry finishFunctionRegistry,
                      Consumer<ThrowingRunnable> addResetFunction,
                      Consumer<ThrowingRunnable> addTearDownFunction,
                      Consumer<ProgressRequestCallback> addProgressRequestCallback,
                      BundleSplitListener splitListener,
                      BundleFinalizer bundleFinalizer)
                      throws IOException {
                    startFunctionRegistry.register(
                        pTransformId, () -> doTimerRegistrations(beamFnTimerClient));
                    return null;
                  }

                  private void doTimerRegistrations(BeamFnTimerClient beamFnTimerClient) {
                    thrown.expect(IllegalStateException.class);
                    thrown.expectMessage("Timers are unsupported");
                    beamFnTimerClient.register(
                        LogicalEndpoint.timer("1L", "2L", "Timer"),
                        Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE),
                        (timer) -> {});
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
