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

import static java.util.Arrays.asList;
import static org.apache.beam.fn.harness.control.ProcessBundleHandler.REGISTERED_RUNNER_FACTORIES;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.BeamFnDataReadRunner;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.MetricsEnvironmentStateForBundle;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Timers;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.AccumulationMode;
import org.apache.beam.model.pipeline.v1.RunnerApi.ClosingBehavior;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.IsBounded;
import org.apache.beam.model.pipeline.v1.RunnerApi.OnTimeBehavior;
import org.apache.beam.model.pipeline.v1.RunnerApi.OutputTime;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRunnerProtocols;
import org.apache.beam.model.pipeline.v1.RunnerApi.TimerFamilySpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger;
import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger.Always;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.DataEndpoint;
import org.apache.beam.sdk.fn.data.TimerEndpoint;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerFamilyDeclaration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.ModelCoders;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessBundleHandler}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unused", // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
})
public class ProcessBundleHandlerTest {
  private static final String DATA_INPUT_URN = "beam:runner:source:v1";
  private static final String DATA_OUTPUT_URN = "beam:runner:sink:v1";

  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);
  @Mock private BeamFnDataClient beamFnDataClient;
  private ExecutionStateSampler executionStateSampler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    TestBundleProcessor.resetCnt = 0;
    executionStateSampler =
        new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis);
  }

  @After
  public void tearDown() {
    executionStateSampler.stop();
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
    Cache<?, ?> getProcessWideCache() {
      return wrappedBundleProcessor.getProcessWideCache();
    }

    @Override
    BundleProgressReporter.InMemory getBundleProgressReporterAndRegistrar() {
      return wrappedBundleProcessor.getBundleProgressReporterAndRegistrar();
    }

    @Override
    ProcessBundleDescriptor getProcessBundleDescriptor() {
      return wrappedBundleProcessor.getProcessBundleDescriptor();
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
    BundleSplitListener.InMemory getSplitListener() {
      return wrappedBundleProcessor.getSplitListener();
    }

    @Override
    PCollectionConsumerRegistry getpCollectionConsumerRegistry() {
      return wrappedBundleProcessor.getpCollectionConsumerRegistry();
    }

    @Override
    MetricsEnvironmentStateForBundle getMetricsEnvironmentStateForBundle() {
      return wrappedBundleProcessor.getMetricsEnvironmentStateForBundle();
    }

    @Override
    public ExecutionStateTracker getStateTracker() {
      return wrappedBundleProcessor.getStateTracker();
    }

    @Override
    ProcessBundleHandler.HandleStateCallsForBundle getBeamFnStateClient() {
      return wrappedBundleProcessor.getBeamFnStateClient();
    }

    @Override
    List<ApiServiceDescriptor> getInboundEndpointApiServiceDescriptors() {
      return wrappedBundleProcessor.getInboundEndpointApiServiceDescriptors();
    }

    @Override
    List<DataEndpoint<?>> getInboundDataEndpoints() {
      return wrappedBundleProcessor.getInboundDataEndpoints();
    }

    @Override
    List<TimerEndpoint<?>> getTimerEndpoints() {
      return wrappedBundleProcessor.getTimerEndpoints();
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
    Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> getOutboundAggregators() {
      return wrappedBundleProcessor.getOutboundAggregators();
    }

    @Override
    Set<String> getRunnerCapabilities() {
      return wrappedBundleProcessor.getRunnerCapabilities();
    }

    @Override
    Lock getProgressRequestLock() {
      return wrappedBundleProcessor.getProgressRequestLock();
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
        InstructionRequest processBundleRequest,
        Supplier<BundleProcessor> bundleProcessorSupplier) {
      return new TestBundleProcessor(super.get(processBundleRequest, bundleProcessorSupplier));
    }
  }

  @Test
  public void testTrySplitBeforeBundleDoesNotFail() {
    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            null,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);

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
            Collections.emptySet(),
            null,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);

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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    List<RunnerApi.PTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    PTransformRunnerFactory<Object> startFinishRecorder =
        (context) -> {
          String pTransformId = context.getPTransformId();
          transformsProcessed.add(context.getPTransform());
          Supplier<String> processBundleInstructionId =
              context.getProcessBundleInstructionIdSupplier();
          context.addStartBundleFunction(
              () -> {
                assertThat(processBundleInstructionId.get(), equalTo("999L"));
                orderOfOperations.add("Start" + pTransformId);
              });
          context.addFinishBundleFunction(
              () -> {
                assertThat(processBundleInstructionId.get(), equalTo("999L"));
                orderOfOperations.add("Finish" + pTransformId);
              });
          return null;
        };

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN, startFinishRecorder,
                DATA_OUTPUT_URN, startFinishRecorder),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);

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
                    .setIsBounded(IsBounded.Enum.BOUNDED)
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap =
        Maps.newHashMap(REGISTERED_RUNNER_FACTORIES);
    urnToPTransformRunnerFactoryMap.put(DATA_INPUT_URN, (context) -> null);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            urnToPTransformRunnerFactoryMap,
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);

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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(DATA_INPUT_URN, (context) -> null),
            Caches.noop(),
            new TestBundleProcessorCache(),
            null /* dataSampler */);

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

  private static InstructionRequest processBundleRequestFor(
      String instructionId, String bundleDescriptorId, CacheToken... cacheTokens) {
    return InstructionRequest.newBuilder()
        .setInstructionId(instructionId)
        .setProcessBundle(
            ProcessBundleRequest.newBuilder()
                .setProcessBundleDescriptorId(bundleDescriptorId)
                .addAllCacheTokens(asList(cacheTokens)))
        .build();
  }

  @Test
  public void testBundleProcessorIsFoundWhenActive() {
    BundleProcessor bundleProcessor = mock(BundleProcessor.class);
    when(bundleProcessor.getInstructionId()).thenReturn("known");
    BundleProcessorCache cache = new BundleProcessorCache();

    // Check that an unknown bundle processor is not found
    assertNull(cache.find("unknown"));

    // Once it is active, ensure the bundle processor is found
    cache.get(processBundleRequestFor("known", "descriptorId"), () -> bundleProcessor);
    assertSame(bundleProcessor, cache.find("known"));

    // After it is released, ensure the bundle processor is no longer found
    cache.release("descriptorId", bundleProcessor);
    assertNull(cache.find("known"));

    // Once it is active, ensure the bundle processor is found
    cache.get(processBundleRequestFor("known", "descriptorId"), () -> bundleProcessor);
    assertSame(bundleProcessor, cache.find("known"));

    // After it is discarded, ensure the bundle processor is no longer found
    cache.discard(bundleProcessor);
    verify(bundleProcessor).discard();
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
    ExecutionStateTracker stateTracker = mock(ExecutionStateTracker.class);
    ProcessBundleHandler.HandleStateCallsForBundle beamFnStateClient =
        mock(ProcessBundleHandler.HandleStateCallsForBundle.class);
    ThrowingRunnable resetFunction = mock(ThrowingRunnable.class);
    Cache<Object, Object> processWideCache = Caches.eternal();
    BundleProcessor bundleProcessor =
        BundleProcessor.create(
            processWideCache,
            new BundleProgressReporter.InMemory(),
            ProcessBundleDescriptor.getDefaultInstance(),
            startFunctionRegistry,
            finishFunctionRegistry,
            Collections.singletonList(resetFunction),
            new ArrayList<>(),
            splitListener,
            pCollectionConsumerRegistry,
            new MetricsEnvironmentStateForBundle(),
            stateTracker,
            beamFnStateClient,
            bundleFinalizationCallbacks,
            new HashSet<>());
    bundleProcessor.finish();
    CacheToken cacheToken =
        CacheToken.newBuilder()
            .setSideInput(CacheToken.SideInput.newBuilder().setTransformId("transformId"))
            .build();
    bundleProcessor.setupForProcessBundleRequest(
        processBundleRequestFor("instructionId", "descriptorId", cacheToken));
    assertEquals("instructionId", bundleProcessor.getInstructionId());
    assertThat(bundleProcessor.getCacheTokens(), containsInAnyOrder(cacheToken));
    Cache<Object, Object> bundleCache = bundleProcessor.getBundleCache();
    bundleCache.put("A", "B");
    assertEquals("B", bundleCache.peek("A"));
    assertTrue(bundleProcessor.getProgressRequestLock().tryLock());
    bundleProcessor.reset();
    assertNull(bundleProcessor.getInstructionId());
    assertNull(bundleProcessor.getCacheTokens());
    assertNull(bundleCache.peek("A"));
    verify(splitListener, times(1)).clear();
    verify(stateTracker, times(1)).reset();
    verify(bundleFinalizationCallbacks, times(1)).clear();
    verify(resetFunction, times(1)).run();
    assertNull(MetricsEnvironment.getCurrentContainer());

    // Ensure that the next setup produces the expected state.
    bundleProcessor.setupForProcessBundleRequest(
        processBundleRequestFor("instructionId2", "descriptorId2"));
    assertNotSame(bundleCache, bundleProcessor.getBundleCache());
    assertEquals("instructionId2", bundleProcessor.getInstructionId());
    assertThat(bundleProcessor.getCacheTokens(), is(emptyIterable()));
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (context) -> {
                  throw new IllegalStateException("TestException");
                }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "TestException",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);
    FinalizeBundleHandler mockFinalizeBundleHandler = mock(FinalizeBundleHandler.class);
    BundleFinalizer.Callback mockCallback = mock(BundleFinalizer.Callback.class);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            mockFinalizeBundleHandler,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (context) -> {
                      BundleFinalizer bundleFinalizer = context.getBundleFinalizer();
                      context.addStartBundleFunction(
                          () ->
                              bundleFinalizer.afterBundleCommit(
                                  Instant.ofEpochMilli(42L), mockCallback));
                      return null;
                    }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
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
  public void testPTransformStartExceptionsArePropagated() {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (context) -> {
                      context.addStartBundleFunction(ProcessBundleHandlerTest::throwException);
                      return null;
                    }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "TestException",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));
    // BundleProcessor is not re-added back to the BundleProcessorCache in case of an exception
    // during bundle processing
    assertThat(handler.bundleProcessorCache.getCachedBundleProcessors().get("1L"), empty());
  }

  private static final class SimpleDoFn extends DoFn<KV<String, String>, String> {
    private static final TupleTag<String> MAIN_OUTPUT_TAG = new TupleTag<>("mainOutput");
    private static final String TIMER_FAMILY_ID = "timer_family";

    @TimerFamily(TIMER_FAMILY_ID)
    private final TimerSpec timer = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {}

    @OnTimerFamily(TIMER_FAMILY_ID)
    public void onTimer(@TimerFamily(TIMER_FAMILY_ID) TimerMap timerFamily) {
      timerFamily
          .get("output_timer")
          .withOutputTimestamp(Instant.ofEpochMilli(100L))
          .set(Instant.ofEpochMilli(100L));
    }
  }

  private ProcessBundleHandler setupProcessBundleHandlerForSimpleRecordingDoFn(
      List<String> dataOutput, List<Timers> timerOutput, boolean enableOutputEmbedding)
      throws Exception {
    DoFnWithExecutionInformation doFnWithExecutionInformation =
        DoFnWithExecutionInformation.of(
            new SimpleDoFn(),
            SimpleDoFn.MAIN_OUTPUT_TAG,
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
        ParDoPayload.newBuilder()
            .setDoFn(functionSpec)
            .putTimerFamilySpecs(
                "tfs-" + SimpleDoFn.TIMER_FAMILY_ID,
                TimerFamilySpec.newBuilder()
                    .setTimeDomain(RunnerApi.TimeDomain.Enum.EVENT_TIME)
                    .setTimerFamilyCoderId("timer-coder")
                    .build())
            .build();
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                PTransform.newBuilder()
                    .setSpec(FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .putOutputs("2L-output", "2L-output-pc")
                    .build())
            .putTransforms(
                "3L",
                PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(parDoPayload.toByteString()))
                    .putInputs("3L-input", "2L-output-pc")
                    .build())
            .putPcollections(
                "2L-output-pc",
                PCollection.newBuilder()
                    .setWindowingStrategyId("window-strategy")
                    .setCoderId("2L-output-coder")
                    .setIsBounded(IsBounded.Enum.BOUNDED)
                    .build())
            .putWindowingStrategies(
                "window-strategy",
                WindowingStrategy.newBuilder()
                    .setWindowCoderId("window-strategy-coder")
                    .setWindowFn(
                        FunctionSpec.newBuilder().setUrn("beam:window_fn:global_windows:v1"))
                    .setOutputTime(OutputTime.Enum.END_OF_WINDOW)
                    .setAccumulationMode(AccumulationMode.Enum.ACCUMULATING)
                    .setTrigger(Trigger.newBuilder().setAlways(Always.getDefaultInstance()))
                    .setClosingBehavior(ClosingBehavior.Enum.EMIT_ALWAYS)
                    .setOnTimeBehavior(OnTimeBehavior.Enum.FIRE_ALWAYS)
                    .build())
            .setTimerApiServiceDescriptor(ApiServiceDescriptor.newBuilder().setUrl("url").build())
            .putCoders("string_coder", CoderTranslation.toProto(StringUtf8Coder.of()).getCoder())
            .putCoders(
                "2L-output-coder",
                Coder.newBuilder()
                    .setSpec(FunctionSpec.newBuilder().setUrn(ModelCoders.KV_CODER_URN).build())
                    .addComponentCoderIds("string_coder")
                    .addComponentCoderIds("string_coder")
                    .build())
            .putCoders(
                "window-strategy-coder",
                Coder.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                            .build())
                    .build())
            .putCoders(
                "timer-coder",
                Coder.newBuilder()
                    .setSpec(FunctionSpec.newBuilder().setUrn(ModelCoders.TIMER_CODER_URN))
                    .addComponentCoderIds("string_coder")
                    .addComponentCoderIds("window-strategy-coder")
                    .build())
            .build();
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap =
        Maps.newHashMap(REGISTERED_RUNNER_FACTORIES);
    urnToPTransformRunnerFactoryMap.put(
        DATA_INPUT_URN,
        (PTransformRunnerFactory<Object>)
            (context) -> {
              context.addIncomingDataEndpoint(
                  ApiServiceDescriptor.getDefaultInstance(),
                  KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
                  (input) -> {
                    dataOutput.add(input.getValue());
                  });
              return null;
            });

    Mockito.doAnswer(
            (invocation) ->
                new BeamFnDataOutboundAggregator(
                    PipelineOptionsFactory.create(),
                    invocation.getArgument(1),
                    new StreamObserver<Elements>() {
                      @Override
                      public void onNext(Elements elements) {
                        for (Timers timer : elements.getTimersList()) {
                          timerOutput.addAll(elements.getTimersList());
                        }
                      }

                      @Override
                      public void onError(Throwable throwable) {}

                      @Override
                      public void onCompleted() {}
                    },
                    invocation.getArgument(2)))
        .when(beamFnDataClient)
        .createOutboundAggregator(any(), any(), anyBoolean());

    return new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        enableOutputEmbedding
            ? Collections.singleton(
                BeamUrns.getUrn(StandardRunnerProtocols.Enum.CONTROL_RESPONSE_ELEMENTS_EMBEDDING))
            : Collections.emptySet(),
        fnApiRegistry::get,
        beamFnDataClient,
        null /* beamFnStateClient */,
        null /* finalizeBundleHandler */,
        new ShortIdMap(),
        executionStateSampler,
        urnToPTransformRunnerFactoryMap,
        Caches.noop(),
        new BundleProcessorCache(),
        null /* dataSampler */);
  }

  @Test
  public void testInstructionEmbeddedElementsAreProcessed() throws Exception {
    List<String> dataOutput = new ArrayList<>();
    List<Timers> timerOutput = new ArrayList<>();
    ProcessBundleHandler handler =
        setupProcessBundleHandlerForSimpleRecordingDoFn(dataOutput, timerOutput, false);

    ByteStringOutputStream encodedData = new ByteStringOutputStream();
    KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()).encode(KV.of("", "data"), encodedData);
    ByteStringOutputStream encodedTimer = new ByteStringOutputStream();
    Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE)
        .encode(
            Timer.of(
                "",
                "timer_id",
                Collections.singletonList(GlobalWindow.INSTANCE),
                Instant.ofEpochMilli(1L),
                Instant.ofEpochMilli(1L),
                PaneInfo.ON_TIME_AND_ONLY_FIRING),
            encodedTimer);
    Elements elements =
        Elements.newBuilder()
            .addData(
                Data.newBuilder()
                    .setInstructionId("998L")
                    .setTransformId("2L")
                    .setData(encodedData.toByteString())
                    .build())
            .addData(
                Data.newBuilder()
                    .setInstructionId("998L")
                    .setTransformId("2L")
                    .setIsLast(true)
                    .build())
            .addTimers(
                Timers.newBuilder()
                    .setInstructionId("998L")
                    .setTransformId("3L")
                    .setTimerFamilyId(TimerFamilyDeclaration.PREFIX + SimpleDoFn.TIMER_FAMILY_ID)
                    .setTimers(encodedTimer.toByteString())
                    .build())
            .addTimers(
                Timers.newBuilder()
                    .setInstructionId("998L")
                    .setTransformId("3L")
                    .setTimerFamilyId(TimerFamilyDeclaration.PREFIX + SimpleDoFn.TIMER_FAMILY_ID)
                    .setIsLast(true)
                    .build())
            .build();
    handler.processBundle(
        InstructionRequest.newBuilder()
            .setInstructionId("998L")
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("1L")
                    .setElements(elements))
            .build());
    handler.shutdown();
    assertThat(dataOutput, contains("data"));
    Timer<String> timer =
        Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE)
            .decode(timerOutput.get(0).getTimers().newInput());
    assertEquals("output_timer", timer.getDynamicTimerTag());
  }

  @Test
  public void testInstructionEmbeddedElementsWithMalformedData() throws Exception {
    List<String> dataOutput = new ArrayList<>();
    List<Timers> timerOutput = new ArrayList<>();
    ProcessBundleHandler handler =
        setupProcessBundleHandlerForSimpleRecordingDoFn(dataOutput, timerOutput, false);

    ByteStringOutputStream encodedData = new ByteStringOutputStream();
    KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()).encode(KV.of("", "data"), encodedData);

    assertThrows(
        "Expect java.lang.IllegalStateException: Unable to find inbound data receiver for"
            + " instruction 998L and transform 3L.",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                InstructionRequest.newBuilder()
                    .setInstructionId("998L")
                    .setProcessBundle(
                        ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L")
                            .setElements(
                                Elements.newBuilder()
                                    .addData(
                                        Data.newBuilder()
                                            .setInstructionId("998L")
                                            .setTransformId("3L")
                                            .setData(encodedData.toByteString())
                                            .build())
                                    .build()))
                    .build()));
    assertThrows(
        "Elements embedded in ProcessBundleRequest do not contain stream terminators for "
            + "all data and timer inputs. Unterminated endpoints: [2L:data,"
            + " 3L:timers:tfs-timer_family]",
        RuntimeException.class,
        () ->
            handler.processBundle(
                InstructionRequest.newBuilder()
                    .setInstructionId("998L")
                    .setProcessBundle(
                        ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L")
                            .setElements(
                                Elements.newBuilder()
                                    .addData(
                                        Data.newBuilder()
                                            .setInstructionId("998L")
                                            .setTransformId("2L")
                                            .setData(encodedData.toByteString())
                                            .build())
                                    .build()))
                    .build()));
    handler.shutdown();
  }

  @Test
  public void testInstructionEmbeddedElementsWithMalformedTimers() throws Exception {
    List<String> dataOutput = new ArrayList<>();
    List<Timers> timerOutput = new ArrayList<>();
    ProcessBundleHandler handler =
        setupProcessBundleHandlerForSimpleRecordingDoFn(dataOutput, timerOutput, false);

    ByteStringOutputStream encodedTimer = new ByteStringOutputStream();
    Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE)
        .encode(
            Timer.of(
                "",
                "timer_id",
                Collections.singletonList(GlobalWindow.INSTANCE),
                Instant.ofEpochMilli(1L),
                Instant.ofEpochMilli(1L),
                PaneInfo.ON_TIME_AND_ONLY_FIRING),
            encodedTimer);

    assertThrows(
        "Expect java.lang.IllegalStateException: Unable to find inbound timer receiver "
            + "for instruction 998L, transform 4L, and timer family tfs-timer_family.",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                InstructionRequest.newBuilder()
                    .setInstructionId("998L")
                    .setProcessBundle(
                        ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L")
                            .setElements(
                                Elements.newBuilder()
                                    .addTimers(
                                        Timers.newBuilder()
                                            .setInstructionId("998L")
                                            .setTransformId("4L")
                                            .setTimerFamilyId(
                                                TimerFamilyDeclaration.PREFIX
                                                    + SimpleDoFn.TIMER_FAMILY_ID)
                                            .setTimers(encodedTimer.toByteString())
                                            .build())
                                    .build()))
                    .build()));
    assertThrows(
        "Expect java.lang.IllegalStateException: Unable to find inbound timer receiver "
            + "for instruction 998L, transform 3L, and timer family tfs-not_declared_id.",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                InstructionRequest.newBuilder()
                    .setInstructionId("998L")
                    .setProcessBundle(
                        ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L")
                            .setElements(
                                Elements.newBuilder()
                                    .addTimers(
                                        Timers.newBuilder()
                                            .setInstructionId("998L")
                                            .setTransformId("3L")
                                            .setTimerFamilyId(
                                                TimerFamilyDeclaration.PREFIX + "not_declared_id")
                                            .setTimers(encodedTimer.toByteString())
                                            .build())
                                    .build()))
                    .build()));
    assertThrows(
        "Elements embedded in ProcessBundleRequest do not contain stream terminators for "
            + "all data and timer inputs. Unterminated endpoints: [2L:data,"
            + " 3L:timers:tfs-timer_family]",
        RuntimeException.class,
        () ->
            handler.processBundle(
                InstructionRequest.newBuilder()
                    .setInstructionId("998L")
                    .setProcessBundle(
                        ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L")
                            .setElements(
                                Elements.newBuilder()
                                    .addTimers(
                                        Timers.newBuilder()
                                            .setInstructionId("998L")
                                            .setTransformId("3L")
                                            .setTimerFamilyId(
                                                TimerFamilyDeclaration.PREFIX
                                                    + SimpleDoFn.TIMER_FAMILY_ID)
                                            .setTimers(encodedTimer.toByteString())
                                            .build())
                                    .build()))
                    .build()));
    handler.shutdown();
  }

  @Test
  public void testOutputEmbeddedElementsAreProcessed() throws Exception {
    List<String> dataOutput = new ArrayList<>();
    List<Timers> timerOutput = new ArrayList<>();
    ProcessBundleHandler handler =
        setupProcessBundleHandlerForSimpleRecordingDoFn(dataOutput, timerOutput, true);

    ByteStringOutputStream encodedTimer = new ByteStringOutputStream();
    Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE)
        .encode(
            Timer.of(
                "",
                "timer_id",
                Collections.singletonList(GlobalWindow.INSTANCE),
                Instant.ofEpochMilli(1L),
                Instant.ofEpochMilli(1L),
                PaneInfo.ON_TIME_AND_ONLY_FIRING),
            encodedTimer);

    InstructionResponse.Builder builder =
        handler.processBundle(
            InstructionRequest.newBuilder()
                .setInstructionId("998L")
                .setProcessBundle(
                    ProcessBundleRequest.newBuilder()
                        .setProcessBundleDescriptorId("1L")
                        .setElements(
                            Elements.newBuilder()
                                .addData(
                                    Data.newBuilder()
                                        .setInstructionId("998L")
                                        .setTransformId("2L")
                                        .setIsLast(true)
                                        .build())
                                .addTimers(
                                    Timers.newBuilder()
                                        .setInstructionId("998L")
                                        .setTransformId("3L")
                                        .setTimerFamilyId(
                                            TimerFamilyDeclaration.PREFIX
                                                + SimpleDoFn.TIMER_FAMILY_ID)
                                        .setTimers(encodedTimer.toByteString())
                                        .build())
                                .addTimers(
                                    Timers.newBuilder()
                                        .setInstructionId("998L")
                                        .setTransformId("3L")
                                        .setTimerFamilyId(
                                            TimerFamilyDeclaration.PREFIX
                                                + SimpleDoFn.TIMER_FAMILY_ID)
                                        .setIsLast(true)
                                        .build())
                                .build()))
                .build());

    handler.shutdown();
    // Ensure no data is flushed through OutboundObserver.
    assertThat(timerOutput, empty());
    // Ensure ProcessBundleResponse gets the embedded data plus terminal element.
    assertEquals(2L, builder.build().getProcessBundle().getElements().getTimersCount());
  }

  @Test
  public void testInstructionIsUnregisteredFromBeamFnDataClientOnSuccess() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    Mockito.doAnswer(
            (invocation) -> {
              String instructionId = invocation.getArgument(0, String.class);
              CloseableFnDataReceiver<BeamFnApi.Elements> data =
                  invocation.getArgument(2, CloseableFnDataReceiver.class);
              data.accept(
                  BeamFnApi.Elements.newBuilder()
                      .addData(
                          BeamFnApi.Elements.Data.newBuilder()
                              .setInstructionId(instructionId)
                              .setTransformId("2L")
                              .setIsLast(true))
                      .build());
              return null;
            })
        .when(beamFnDataClient)
        .registerReceiver(any(), any(), any());

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (context) -> {
                      context.addIncomingDataEndpoint(
                          ApiServiceDescriptor.getDefaultInstance(),
                          StringUtf8Coder.of(),
                          (input) -> {});
                      return null;
                    }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("instructionId")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    // Ensure that we unregister during successful processing
    verify(beamFnDataClient).registerReceiver(eq("instructionId"), any(), any());
    verify(beamFnDataClient).unregisterReceiver(eq("instructionId"), any());
    verifyNoMoreInteractions(beamFnDataClient);
  }

  @Test
  public void testDataProcessingExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .build())
            .build();
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    Mockito.doAnswer(
            (invocation) -> {
              ByteStringOutputStream encodedData = new ByteStringOutputStream();
              StringUtf8Coder.of().encode("A", encodedData);
              String instructionId = invocation.getArgument(0, String.class);
              CloseableFnDataReceiver<BeamFnApi.Elements> data =
                  invocation.getArgument(2, CloseableFnDataReceiver.class);
              data.accept(
                  BeamFnApi.Elements.newBuilder()
                      .addData(
                          BeamFnApi.Elements.Data.newBuilder()
                              .setInstructionId(instructionId)
                              .setTransformId("2L")
                              .setData(encodedData.toByteString())
                              .setIsLast(true))
                      .build());

              return null;
            })
        .when(beamFnDataClient)
        .registerReceiver(any(), any(), any());

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (context) -> {
                      context.addIncomingDataEndpoint(
                          ApiServiceDescriptor.getDefaultInstance(),
                          StringUtf8Coder.of(),
                          (input) -> {
                            throw new IllegalStateException("TestException");
                          });
                      return null;
                    }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "TestException",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setInstructionId("instructionId")
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));

    // Ensure that we unregister during successful processing
    verify(beamFnDataClient).registerReceiver(eq("instructionId"), any(), any());
    verify(beamFnDataClient).poisonInstructionId(eq("instructionId"));
    verifyNoMoreInteractions(beamFnDataClient);
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (context) -> {
                      context.addFinishBundleFunction(ProcessBundleHandlerTest::throwException);
                      return null;
                    }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "TestException",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));

    // BundleProcessor is not re-added back to the BundleProcessorCache in case of an exception
    // during bundle processing
    assertThat(handler.bundleProcessorCache.getCachedBundleProcessors().get("1L"), empty());
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    CompletableFuture<StateResponse>[] successfulResponse = new CompletableFuture[1];
    CompletableFuture<StateResponse>[] unsuccessfulResponse = new CompletableFuture[1];

    BeamFnStateGrpcClientCache mockBeamFnStateGrpcClient =
        Mockito.mock(BeamFnStateGrpcClientCache.class);
    BeamFnStateClient mockBeamFnStateClient = Mockito.mock(BeamFnStateClient.class);
    when(mockBeamFnStateGrpcClient.forApiServiceDescriptor(any()))
        .thenReturn(mockBeamFnStateClient);

    doAnswer(
            invocation -> {
              StateRequest.Builder stateRequestBuilder =
                  (StateRequest.Builder) invocation.getArguments()[0];
              CompletableFuture<StateResponse> completableFuture = new CompletableFuture<>();
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
              return completableFuture;
            })
        .when(mockBeamFnStateClient)
        .handle(any());

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            mockBeamFnStateGrpcClient,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(Context context) throws IOException {
                    BeamFnStateClient beamFnStateClient = context.getBeamFnStateClient();
                    context.addStartBundleFunction(() -> doStateCalls(beamFnStateClient));
                    return null;
                  }

                  private void doStateCalls(BeamFnStateClient beamFnStateClient) {
                    successfulResponse[0] =
                        beamFnStateClient.handle(
                            StateRequest.newBuilder().setInstructionId("SUCCESS"));
                    unsuccessfulResponse[0] =
                        beamFnStateClient.handle(
                            StateRequest.newBuilder().setInstructionId("FAIL"));
                  }
                }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("1L"))
            .build());

    assertTrue(successfulResponse[0].isDone());
    assertTrue(unsuccessfulResponse[0].isDone());
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(Context context) throws IOException {
                    BeamFnStateClient beamFnStateClient = context.getBeamFnStateClient();
                    context.addStartBundleFunction(() -> doStateCalls(beamFnStateClient));
                    return null;
                  }

                  @SuppressWarnings("FutureReturnValueIgnored")
                  private void doStateCalls(BeamFnStateClient beamFnStateClient) {
                    beamFnStateClient.handle(StateRequest.newBuilder().setInstructionId("SUCCESS"));
                  }
                }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "State API calls are unsupported",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));
  }

  @Test
  public void testProgressReportingIsExecutedSerially() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms(
                "2L",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                    .putOutputs("2L-output", "2L-output-pc")
                    .build())
            .putPcollections("2L-output-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(1);

    AtomicReference<BundleProcessor> bundleProcessor = new AtomicReference<>();
    AtomicReference<Thread> mainBundleProcessingThread = new AtomicReference<>();
    AtomicInteger counter = new AtomicInteger();
    AtomicBoolean finalWasCalled = new AtomicBoolean();
    AtomicBoolean resetWasCalled = new AtomicBoolean();
    BundleProgressReporter testReporter =
        new BundleProgressReporter() {
          @Override
          public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
            assertTrue(
                ((ReentrantLock) bundleProcessor.get().getProgressRequestLock())
                    .isHeldByCurrentThread());
            assertNotEquals(Thread.currentThread(), mainBundleProcessingThread.get());
            assertFalse(finalWasCalled.get());
            assertFalse(resetWasCalled.get());
            monitoringData.put(
                "testId", ByteString.copyFromUtf8(Long.toString(counter.getAndIncrement())));
          }

          @Override
          public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
            assertTrue(
                ((ReentrantLock) bundleProcessor.get().getProgressRequestLock())
                    .isHeldByCurrentThread());
            assertEquals(Thread.currentThread(), mainBundleProcessingThread.get());
            assertFalse(finalWasCalled.getAndSet(true));
            assertFalse(resetWasCalled.get());
            monitoringData.put("testId", ByteString.copyFromUtf8(Long.toString(counter.get())));
          }

          @Override
          public void reset() {
            assertTrue(
                ((ReentrantLock) bundleProcessor.get().getProgressRequestLock())
                    .isHeldByCurrentThread());
            assertEquals(Thread.currentThread(), mainBundleProcessingThread.get());
            assertTrue(finalWasCalled.get());
            assertFalse(resetWasCalled.getAndSet(true));
          }
        };
    PTransformRunnerFactory<Object> startFinishGuard =
        (context) -> {
          String pTransformId = context.getPTransformId();
          Supplier<String> processBundleInstructionId =
              context.getProcessBundleInstructionIdSupplier();
          context.addBundleProgressReporter(testReporter);
          context.addStartBundleFunction(
              () -> {
                startLatch.countDown();
              });
          context.addFinishBundleFunction(
              () -> {
                finishLatch.await();
              });
          return null;
        };

    BundleProcessorCache bundleProcessorCache = new BundleProcessorCache();
    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.singleton(
                BeamUrns.getUrn(RunnerApi.StandardRunnerProtocols.Enum.MONITORING_INFO_SHORT_IDS)),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateClient */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(DATA_INPUT_URN, startFinishGuard),
            Caches.noop(),
            bundleProcessorCache,
            null /* dataSampler */);

    AtomicBoolean progressShouldExit = new AtomicBoolean();
    Future<InstructionResponse> bundleProcessorTask =
        executor.submit(
            () -> {
              mainBundleProcessingThread.set(Thread.currentThread());
              InstructionResponse response =
                  handler
                      .processBundle(
                          BeamFnApi.InstructionRequest.newBuilder()
                              .setInstructionId("999L")
                              .setProcessBundle(
                                  BeamFnApi.ProcessBundleRequest.newBuilder()
                                      .setProcessBundleDescriptorId("1L"))
                              .build())
                      .build();
              progressShouldExit.set(true);
              return response;
            });
    startLatch.await();
    bundleProcessor.set(bundleProcessorCache.find("999L"));

    final int minNumResults = 5;
    CountDownLatch progressLatch = new CountDownLatch(1);
    CountDownLatch someProgressIsDone = new CountDownLatch(minNumResults);
    List<Future<InstructionResponse>> progressReportingTasks = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      final int threadId = i;
      progressReportingTasks.add(
          executor.submit(
              () -> {
                // Wait till progress threads have all been started.
                progressLatch.await();
                int requestCount = 0;
                InstructionResponse.Builder response;
                try {
                  do {
                    response =
                        handler.progress(
                            BeamFnApi.InstructionRequest.newBuilder()
                                .setInstructionId("thread-" + threadId + "-" + (++requestCount))
                                .setProcessBundleProgress(
                                    ProcessBundleProgressRequest.newBuilder()
                                        .setInstructionId("999L")
                                        .build())
                                .build());
                  } while (!response
                          .getProcessBundleProgress()
                          .getMonitoringDataMap()
                          .containsKey("testId")
                      && !progressShouldExit.get());
                } finally {
                  someProgressIsDone.countDown();
                }
                return response.build();
              }));
    }
    // Allow progress reporting requests to start
    progressLatch.countDown();
    // Wait till some progress reports are done before allowing the main processing thread to
    // finish.
    someProgressIsDone.await();
    finishLatch.countDown();

    List<ByteString> progressReportingResults = new ArrayList<>();
    for (Future<InstructionResponse> progressReportingTask : progressReportingTasks) {
      ByteString result =
          progressReportingTask
              .get()
              .getProcessBundleProgress()
              .getMonitoringDataOrDefault("testId", null);
      if (result != null) {
        progressReportingResults.add(result);
      }
    }

    // We validate the lifecycle of intermediate -> final -> reset was invoked

    // We should see that there is at least minNumResults intermediate results representing the
    // set [0, 'counter.get()') with the final result having 'counter.get()'
    assertTrue(progressReportingResults.size() >= minNumResults);
    List<ByteString> expectedIntermediateResults = new ArrayList<>();
    for (int i = 0; i < counter.get(); ++i) {
      expectedIntermediateResults.add(ByteString.copyFromUtf8(Long.toString(i)));
    }
    assertThat(progressReportingResults, containsInAnyOrder(expectedIntermediateResults.toArray()));
    assertEquals(
        ByteString.copyFromUtf8(Long.toString(counter.get())),
        bundleProcessorTask.get().getProcessBundle().getMonitoringDataOrThrow("testId"));
    assertTrue(finalWasCalled.get());
    assertTrue(resetWasCalled.get());
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
    Map<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry =
        ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            Collections.emptySet(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            null /* finalizeBundleHandler */,
            new ShortIdMap(),
            executionStateSampler,
            ImmutableMap.of(
                DATA_INPUT_URN,
                new PTransformRunnerFactory<Object>() {
                  @Override
                  public Object createRunnerForPTransform(Context context) throws IOException {
                    context.addOutgoingTimersEndpoint(
                        "timer", Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));
                    return null;
                  }
                }),
            Caches.noop(),
            new BundleProcessorCache(),
            null /* dataSampler */);
    assertThrows(
        "Timers are unsupported",
        IllegalStateException.class,
        () ->
            handler.processBundle(
                BeamFnApi.InstructionRequest.newBuilder()
                    .setProcessBundle(
                        BeamFnApi.ProcessBundleRequest.newBuilder()
                            .setProcessBundleDescriptorId("1L"))
                    .build()));
  }

  private static void throwException() {
    throw new IllegalStateException("TestException");
  }
}
