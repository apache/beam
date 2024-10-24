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
package org.apache.beam.runners.fnexecution.control;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse.ChannelSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.fnexecution.EmbeddedSdkHarness;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor.ActiveBundle;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessClient}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class SdkHarnessClientTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Mock public FnApiControlClient fnApiControlClient;
  @Mock public FnDataService dataService;
  @Captor ArgumentCaptor<CloseableFnDataReceiver<BeamFnApi.Elements>> outputReceiverCaptor;
  @Rule public EmbeddedSdkHarness harness = EmbeddedSdkHarness.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private SdkHarnessClient sdkHarnessClient;
  private ProcessBundleDescriptor descriptor;
  private static final String SDK_GRPC_READ_TRANSFORM = "read";
  private static final String SDK_GRPC_WRITE_TRANSFORM = "write";

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    sdkHarnessClient = SdkHarnessClient.usingFnApiClient(fnApiControlClient, dataService);

    Pipeline userPipeline = Pipeline.create();
    TupleTag<String> outputTag = new TupleTag<>();
    userPipeline
        .apply("create", Create.of("foo"))
        .apply("proc", ParDo.of(new TestFn()).withOutputTags(outputTag, TupleTagList.empty()));
    RunnerApi.Pipeline userProto = PipelineTranslation.toProto(userPipeline);

    ProcessBundleDescriptor.Builder pbdBuilder =
        ProcessBundleDescriptor.newBuilder()
            .setId("my_id")
            .putAllEnvironments(userProto.getComponents().getEnvironmentsMap())
            .putAllWindowingStrategies(userProto.getComponents().getWindowingStrategiesMap())
            .putAllCoders(userProto.getComponents().getCodersMap());
    RunnerApi.Coder fullValueCoder =
        CoderTranslation.toProto(WindowedValue.getFullCoder(StringUtf8Coder.of(), Coder.INSTANCE))
            .getCoder();
    pbdBuilder.putCoders("wire_coder", fullValueCoder);

    PTransform targetProcessor = userProto.getComponents().getTransformsOrThrow("proc");
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(harness.dataEndpoint())
            .setCoderId("wire_coder")
            .build();
    RemoteGrpcPortRead readNode =
        RemoteGrpcPortRead.readFromPort(
            port, getOnlyElement(targetProcessor.getInputsMap().values()));
    RemoteGrpcPortWrite writeNode =
        RemoteGrpcPortWrite.writeToPort(
            getOnlyElement(targetProcessor.getOutputsMap().values()), port);
    // TODO: Ensure cross-env (Runner <-> SDK GRPC Read/Write Node) coders are length-prefixed
    for (String pc : targetProcessor.getInputsMap().values()) {
      pbdBuilder.putPcollections(pc, userProto.getComponents().getPcollectionsOrThrow(pc));
    }
    for (String pc : targetProcessor.getOutputsMap().values()) {
      pbdBuilder.putPcollections(pc, userProto.getComponents().getPcollectionsOrThrow(pc));
    }
    pbdBuilder
        .putTransforms("proc", targetProcessor)
        .putTransforms(SDK_GRPC_READ_TRANSFORM, readNode.toPTransform())
        .putTransforms(SDK_GRPC_WRITE_TRANSFORM, writeNode.toPTransform());
    descriptor = pbdBuilder.build();
  }

  @Test
  public void testRegister() throws Exception {
    ProcessBundleDescriptor descriptor1 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor1").build();

    List<RemoteInputDestination> remoteInputs =
        Collections.singletonList(
            RemoteInputDestination.of(
                (FullWindowedValueCoder)
                    FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
                SDK_GRPC_READ_TRANSFORM));

    sdkHarnessClient.getProcessor(descriptor1, remoteInputs);

    verify(fnApiControlClient).registerProcessBundleDescriptor(descriptor1);
  }

  @Test
  public void testRegisterCachesBundleProcessors() throws Exception {
    ProcessBundleDescriptor descriptor1 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor1").build();
    ProcessBundleDescriptor descriptor2 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor2").build();

    List<RemoteInputDestination> remoteInputs =
        Collections.singletonList(
            RemoteInputDestination.of(
                (FullWindowedValueCoder)
                    FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
                SDK_GRPC_READ_TRANSFORM));

    BundleProcessor processor1 = sdkHarnessClient.getProcessor(descriptor1, remoteInputs);
    BundleProcessor processor2 = sdkHarnessClient.getProcessor(descriptor2, remoteInputs);

    assertNotSame(processor1, processor2);

    // Ensure that caching works.
    assertSame(processor1, sdkHarnessClient.getProcessor(descriptor1, remoteInputs));
  }

  @Test
  public void testRegisterWithStateRequiresStateDelegator() throws Exception {
    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder()
            .setId("test")
            .setStateApiServiceDescriptor(ApiServiceDescriptor.newBuilder().setUrl("foo"))
            .build();

    List<RemoteInputDestination> remoteInputs =
        Collections.singletonList(
            RemoteInputDestination.of(
                (FullWindowedValueCoder)
                    FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
                SDK_GRPC_READ_TRANSFORM));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("containing a state");
    sdkHarnessClient.getProcessor(descriptor, remoteInputs);
  }

  @Test
  public void testNewBundleNoDataDoesNotCrash() throws Exception {
    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean()))
        .thenReturn(mock(BeamFnDataOutboundAggregator.class));

    try (RemoteBundle activeBundle =
        processor.newBundle(Collections.emptyMap(), BundleProgressHandler.ignored())) {
      // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
      // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
      // the response.
      //
      // Currently there are no fields so there's nothing to check. This test is formulated
      // to match the pattern it should have if/when the response is meaningful.
      BeamFnApi.ProcessBundleResponse response = ProcessBundleResponse.getDefaultInstance();
      processBundleResponseFuture.complete(
          BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    }
  }

  @Test
  public void testClosingActiveBundleMultipleTimesIsNoop() throws Exception {
    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean()))
        .thenReturn(mock(BeamFnDataOutboundAggregator.class));

    RemoteBundle activeBundle =
        processor.newBundle(Collections.emptyMap(), BundleProgressHandler.ignored());
    // Correlating the request and response is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    BeamFnApi.ProcessBundleResponse response = ProcessBundleResponse.getDefaultInstance();
    processBundleResponseFuture.complete(
        BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    activeBundle.close();

    activeBundle.close();
  }

  @Test
  public void testProgressAndSplitCallsAreIgnoredWhenBundleIsComplete() throws Exception {
    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean()))
        .thenReturn(mock(BeamFnDataOutboundAggregator.class));

    RemoteBundle activeBundle =
        processor.newBundle(Collections.emptyMap(), BundleProgressHandler.ignored());
    // Correlating the request and response is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    BeamFnApi.ProcessBundleResponse response = ProcessBundleResponse.getDefaultInstance();
    processBundleResponseFuture.complete(
        BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    activeBundle.close();

    verify(fnApiControlClient).registerProcessBundleDescriptor(any(ProcessBundleDescriptor.class));
    verify(fnApiControlClient).handle(any(BeamFnApi.InstructionRequest.class));

    activeBundle.requestProgress();
    activeBundle.split(0);
    verifyNoMoreInteractions(fnApiControlClient);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testProgressHandlerOnCompletedHappensAfterOnProgress() throws Exception {
    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    CompletableFuture<InstructionResponse> progressResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenAnswer(
            invocationOnMock -> {
              switch (invocationOnMock
                  .<BeamFnApi.InstructionRequest>getArgument(0)
                  .getRequestCase()) {
                case PROCESS_BUNDLE:
                  return processBundleResponseFuture;
                case PROCESS_BUNDLE_PROGRESS:
                  return progressResponseFuture;
                default:
                  throw new IllegalArgumentException(
                      "Unexpected request "
                          + invocationOnMock.<BeamFnApi.InstructionRequest>getArgument(0));
              }
            });

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean()))
        .thenReturn(mock(BeamFnDataOutboundAggregator.class));

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    RemoteBundle activeBundle = processor.newBundle(Collections.emptyMap(), mockProgressHandler);
    BeamFnApi.ProcessBundleResponse response =
        ProcessBundleResponse.newBuilder().putMonitoringData("test", ByteString.EMPTY).build();
    BeamFnApi.ProcessBundleProgressResponse progressResponse =
        ProcessBundleProgressResponse.newBuilder()
            .putMonitoringData("test2", ByteString.EMPTY)
            .build();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // Correlating the request and response is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Schedule the progress response to come in after the bundle response and after the close call.
    activeBundle.requestProgress();
    executor.schedule(
        () ->
            processBundleResponseFuture.complete(
                BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build()),
        1,
        TimeUnit.SECONDS);
    executor.schedule(
        () ->
            progressResponseFuture.complete(
                BeamFnApi.InstructionResponse.newBuilder()
                    .setProcessBundleProgress(progressResponse)
                    .build()),
        2,
        TimeUnit.SECONDS);
    activeBundle.close();

    InOrder inOrder = Mockito.inOrder(mockProgressHandler);
    inOrder.verify(mockProgressHandler).onProgress(eq(progressResponse));
    inOrder.verify(mockProgressHandler).onCompleted(eq(response));
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testCheckpointHappensAfterAnySplitCalls() throws Exception {
    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    CompletableFuture<InstructionResponse> splitResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenAnswer(
            invocationOnMock -> {
              switch (invocationOnMock
                  .<BeamFnApi.InstructionRequest>getArgument(0)
                  .getRequestCase()) {
                case PROCESS_BUNDLE:
                  return processBundleResponseFuture;
                case PROCESS_BUNDLE_SPLIT:
                  return splitResponseFuture;
                default:
                  throw new IllegalArgumentException(
                      "Unexpected request "
                          + invocationOnMock.<BeamFnApi.InstructionRequest>getArgument(0));
              }
            });

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean()))
        .thenReturn(mock(BeamFnDataOutboundAggregator.class));

    BundleCheckpointHandler mockCheckpointHandler = mock(BundleCheckpointHandler.class);
    BundleSplitHandler mockSplitHandler = mock(BundleSplitHandler.class);
    BundleFinalizationHandler mockFinalizationHandler = mock(BundleFinalizationHandler.class);

    RemoteBundle activeBundle =
        processor.newBundle(
            Collections.emptyMap(),
            Collections.emptyMap(),
            StateRequestHandler.unsupported(),
            BundleProgressHandler.ignored(),
            mockSplitHandler,
            mockCheckpointHandler,
            mockFinalizationHandler);
    BeamFnApi.ProcessBundleResponse response =
        ProcessBundleResponse.newBuilder()
            .addResidualRoots(
                DelayedBundleApplication.newBuilder()
                    .setApplication(BundleApplication.newBuilder().setTransformId("test").build())
                    .build())
            .build();
    BeamFnApi.ProcessBundleSplitResponse splitResponse =
        ProcessBundleSplitResponse.newBuilder()
            .addChannelSplits(ChannelSplit.newBuilder().setTransformId("test2"))
            .build();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // Correlating the request and response is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Schedule the split response to come in after the bundle response and after the close call.
    activeBundle.split(0.5);
    executor.schedule(
        () ->
            processBundleResponseFuture.complete(
                BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build()),
        1,
        TimeUnit.SECONDS);
    executor.schedule(
        () ->
            splitResponseFuture.complete(
                BeamFnApi.InstructionResponse.newBuilder()
                    .setProcessBundleSplit(splitResponse)
                    .build()),
        2,
        TimeUnit.SECONDS);
    activeBundle.close();

    InOrder inOrder = Mockito.inOrder(mockCheckpointHandler, mockSplitHandler);
    inOrder.verify(mockSplitHandler).split(eq(splitResponse));
    inOrder.verify(mockCheckpointHandler).onCheckpoint(eq(response));
  }

  @Test
  public void testNewBundleAndProcessElements() throws Exception {
    SdkHarnessClient client = harness.client();
    BundleProcessor processor =
        client.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder)
                        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE),
                    SDK_GRPC_READ_TRANSFORM)));

    Collection<WindowedValue<String>> outputs = new ArrayList<>();
    try (RemoteBundle activeBundle =
        processor.newBundle(
            Collections.singletonMap(
                SDK_GRPC_WRITE_TRANSFORM,
                RemoteOutputReceiver.of(
                    FullWindowedValueCoder.of(
                        LengthPrefixCoder.of(StringUtf8Coder.of()), Coder.INSTANCE),
                    outputs::add)),
            BundleProgressHandler.ignored())) {
      FnDataReceiver<WindowedValue<?>> bundleInputReceiver =
          Iterables.getOnlyElement(activeBundle.getInputReceivers().values());
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("foo"));
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("bar"));
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("baz"));
    }

    // The bundle can be a simple function of some sort, but needs to be complete.
    assertThat(
        outputs,
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("spam"),
            WindowedValue.valueInGlobalWindow("ham"),
            WindowedValue.valueInGlobalWindow("eggs")));
  }

  @Test
  public void handleCleanupWhenInputSenderFails() throws Exception {
    RuntimeException testException = new RuntimeException();

    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    doNothing().when(dataService).registerReceiver(any(), outputReceiverCaptor.capture());
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    doThrow(testException)
        .when(mockInputSender)
        .sendOrCollectBufferedDataAndFinishOutboundStreams();

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    try {
      try (RemoteBundle activeBundle =
          processor.newBundle(
              ImmutableMap.of(
                  SDK_GRPC_WRITE_TRANSFORM,
                  RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
              mockProgressHandler)) {
        // We shouldn't be required to complete the process bundle response future.
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);

      // We expect that we don't register the receiver and the next accept call will raise an error
      // making the data service aware of the error.
      verify(dataService, never()).unregisterReceiver(any());
      assertThrows(
          "Inbound observer closed.",
          Exception.class,
          () -> outputReceiverCaptor.getValue().accept(Elements.getDefaultInstance()));
    }
  }

  @Test
  public void handleCleanupWithStateWhenInputSenderFails() throws Exception {
    RuntimeException testException = new RuntimeException();

    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);
    when(mockStateHandler.getCacheTokens()).thenReturn(Collections.emptyList());
    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of((FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)),
            mockStateDelegator);
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    doThrow(testException)
        .when(mockInputSender)
        .sendOrCollectBufferedDataAndFinishOutboundStreams();

    try {
      try (RemoteBundle activeBundle =
          processor.newBundle(
              ImmutableMap.of(
                  SDK_GRPC_WRITE_TRANSFORM,
                  RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
              mockStateHandler,
              mockProgressHandler)) {
        // We shouldn't be required to complete the process bundle response future.
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);

      verify(mockStateRegistration).abort();
      // We expect that we don't register the receiver and the next accept call will raise an error
      // making the data service aware of the error.
      verify(dataService, never()).unregisterReceiver(any());
      assertThrows(
          "Inbound observer closed.",
          Exception.class,
          () -> outputReceiverCaptor.getValue().accept(Elements.getDefaultInstance()));
    }
  }

  @Test
  public void handleCleanupWhenProcessingBundleFails() throws Exception {
    RuntimeException testException = new RuntimeException();

    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    try {
      try (RemoteBundle activeBundle =
          processor.newBundle(
              ImmutableMap.of(
                  SDK_GRPC_WRITE_TRANSFORM,
                  RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
              mockProgressHandler)) {
        processBundleResponseFuture.completeExceptionally(testException);
      }
      fail("Exception expected");
    } catch (ExecutionException e) {
      assertEquals(testException, e.getCause());

      // We expect that we don't register the receiver and the next accept call will raise an error
      // making the data service aware of the error.
      verify(dataService, never()).unregisterReceiver(any());
      assertThrows(
          "Inbound observer closed.",
          Exception.class,
          () -> outputReceiverCaptor.getValue().accept(Elements.getDefaultInstance()));
    }
  }

  @Test
  public void handleCleanupWithStateWhenProcessingBundleFails() throws Exception {
    RuntimeException testException = new RuntimeException();

    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);
    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);
    when(mockStateHandler.getCacheTokens()).thenReturn(Collections.emptyList());
    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of((FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)),
            mockStateDelegator);
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    try {
      try (RemoteBundle activeBundle =
          processor.newBundle(
              ImmutableMap.of(
                  SDK_GRPC_WRITE_TRANSFORM,
                  RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
              mockStateHandler,
              mockProgressHandler)) {
        processBundleResponseFuture.completeExceptionally(testException);
      }
      fail("Exception expected");
    } catch (ExecutionException e) {
      assertEquals(testException, e.getCause());

      verify(mockStateRegistration).abort();
      // We expect that we don't register the receiver and the next accept call will raise an error
      // making the data service aware of the error.
      verify(dataService, never()).unregisterReceiver(any());
      assertThrows(
          "Inbound observer closed.",
          Exception.class,
          () -> outputReceiverCaptor.getValue().accept(Elements.getDefaultInstance()));
    }
  }

  @Test
  public void handleCleanupWhenAwaitingOnClosingOutputReceivers() throws Exception {
    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    doNothing().when(dataService).registerReceiver(any(), outputReceiverCaptor.capture());
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    RemoteBundle activeBundle =
        processor.newBundle(
            ImmutableMap.of(
                SDK_GRPC_WRITE_TRANSFORM,
                RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
            mockProgressHandler);
    // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse.getDefaultInstance();
    processBundleResponseFuture.complete(
        BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    // Inject an error before we close the bundle as if the data service closed the stream
    // explicitly
    outputReceiverCaptor.getValue().close();

    assertThrows("Inbound observer closed.", Exception.class, activeBundle::close);
  }

  @Test
  public void handleCleanupWithStateWhenAwaitingOnClosingOutputReceivers() throws Exception {
    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);
    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);
    when(mockStateHandler.getCacheTokens()).thenReturn(Collections.emptyList());
    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of((FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)),
            mockStateDelegator);
    doNothing().when(dataService).registerReceiver(any(), outputReceiverCaptor.capture());
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    RemoteBundle activeBundle =
        processor.newBundle(
            ImmutableMap.of(
                SDK_GRPC_WRITE_TRANSFORM,
                RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
            mockStateHandler,
            mockProgressHandler);
    // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse.getDefaultInstance();
    processBundleResponseFuture.complete(
        BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    // Inject an error before we close the bundle as if the data service closed the stream
    // explicitly.
    outputReceiverCaptor.getValue().close();
    assertThrows("Inbound observer closed.", Exception.class, activeBundle::close);
  }

  @Test
  public void verifyCacheTokensAreUsedInNewBundleRequest() throws InterruptedException {
    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(
            CompletableFuture.<InstructionResponse>completedFuture(
                InstructionResponse.newBuilder().build()));

    ProcessBundleDescriptor descriptor1 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor1").build();

    List<RemoteInputDestination> remoteInputs =
        Collections.singletonList(
            RemoteInputDestination.of(
                FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
                SDK_GRPC_READ_TRANSFORM));

    BundleProcessor processor1 = sdkHarnessClient.getProcessor(descriptor1, remoteInputs);
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    StateRequestHandler stateRequestHandler = Mockito.mock(StateRequestHandler.class);
    List<BeamFnApi.ProcessBundleRequest.CacheToken> cacheTokens =
        Collections.singletonList(
            BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder().getDefaultInstanceForType());
    when(stateRequestHandler.getCacheTokens()).thenReturn(cacheTokens);

    processor1.newBundle(
        ImmutableMap.of(
            SDK_GRPC_WRITE_TRANSFORM,
            RemoteOutputReceiver.of(ByteArrayCoder.of(), mock(FnDataReceiver.class))),
        stateRequestHandler,
        BundleProgressHandler.ignored());

    // Retrieve the requests made to the FnApiControlClient
    ArgumentCaptor<BeamFnApi.InstructionRequest> reqCaptor =
        ArgumentCaptor.forClass(BeamFnApi.InstructionRequest.class);
    Mockito.verify(fnApiControlClient, Mockito.times(1)).handle(reqCaptor.capture());
    List<BeamFnApi.InstructionRequest> requests = reqCaptor.getAllValues();

    // Verify that the cache tokens are included in the ProcessBundleRequest
    assertThat(
        requests.get(0).getRequestCase(),
        is(BeamFnApi.InstructionRequest.RequestCase.PROCESS_BUNDLE));
    assertThat(requests.get(0).getProcessBundle().getCacheTokensList(), is(cacheTokens));
  }

  @Test
  public void testBundleCheckpointCallback() throws Exception {
    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);
    BundleSplitHandler mockSplitHandler = mock(BundleSplitHandler.class);
    BundleCheckpointHandler mockCheckpointHandler = mock(BundleCheckpointHandler.class);
    BundleFinalizationHandler mockFinalizationHandler = mock(BundleFinalizationHandler.class);

    ProcessBundleResponse response =
        ProcessBundleResponse.newBuilder()
            .addResidualRoots(DelayedBundleApplication.getDefaultInstance())
            .build();
    try (ActiveBundle activeBundle =
        processor.newBundle(
            Collections.emptyMap(),
            Collections.emptyMap(),
            (request) -> {
              throw new UnsupportedOperationException();
            },
            mockProgressHandler,
            mockSplitHandler,
            mockCheckpointHandler,
            mockFinalizationHandler)) {
      processBundleResponseFuture.complete(
          InstructionResponse.newBuilder().setProcessBundle(response).build());
    }

    verify(mockProgressHandler).onCompleted(response);
    verify(mockCheckpointHandler).onCheckpoint(response);
    verifyNoMoreInteractions(mockFinalizationHandler, mockSplitHandler);
  }

  @Test
  public void testBundleFinalizationCallback() throws Exception {
    BeamFnDataOutboundAggregator mockInputSender = mock(BeamFnDataOutboundAggregator.class);

    CompletableFuture<InstructionResponse> processBundleResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            Collections.singletonList(
                RemoteInputDestination.of(
                    (FullWindowedValueCoder) coder, SDK_GRPC_READ_TRANSFORM)));
    when(dataService.createOutboundAggregator(any(), anyBoolean())).thenReturn(mockInputSender);

    BundleProgressHandler mockProgressHandler = mock(BundleProgressHandler.class);
    BundleSplitHandler mockSplitHandler = mock(BundleSplitHandler.class);
    BundleCheckpointHandler mockCheckpointHandler = mock(BundleCheckpointHandler.class);
    BundleFinalizationHandler mockFinalizationHandler = mock(BundleFinalizationHandler.class);

    ProcessBundleResponse response =
        ProcessBundleResponse.newBuilder().setRequiresFinalization(true).build();
    String bundleId;
    try (ActiveBundle activeBundle =
        processor.newBundle(
            Collections.emptyMap(),
            Collections.emptyMap(),
            (request) -> {
              throw new UnsupportedOperationException();
            },
            mockProgressHandler,
            mockSplitHandler,
            mockCheckpointHandler,
            mockFinalizationHandler)) {
      bundleId = activeBundle.getId();
      processBundleResponseFuture.complete(
          InstructionResponse.newBuilder().setProcessBundle(response).build());
    }

    verify(mockProgressHandler).onCompleted(response);
    verify(mockFinalizationHandler).requestsFinalization(bundleId);
    verifyNoMoreInteractions(mockCheckpointHandler, mockSplitHandler);
  }

  private static class TestFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      if ("foo".equals(context.element())) {
        context.output("spam");
      } else if ("bar".equals(context.element())) {
        context.output("ham");
      } else {
        context.output("eggs");
      }
    }
  }
}
